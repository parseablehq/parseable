import { decodeAlertTargetPolicy } from './networkPolicy';
import { clusterNodes, clusterMetrics } from './operationsContract';
import { createDemoClient } from './demo';
import { authEndpoint, cookieIdentity } from './auth';
import { demoEnabled } from './config';
import { apiKey, groupRoles, object, roles, roleSources, strings, userRoles } from './teamContract';
import { instantResult, rangeResult, successEnvelope } from './promqlContract';
import { alert, alertSummary, alertTarget, alertTargetStatus } from './alertsContract';
import { dashboard, dashboardSummary } from './dashboardsContract';
import { savedLogView, logContextResponse } from './logsContract';
import { datasetHotTier, datasetRetention, datasetStats } from './datasetsContract';
import { traceListResponse, decodeTraceDetail } from './tracesContract';
import type {
  AlertSeverity,
  Dataset,
  LogRecord,
  ParseableClient,
  PromqlMetadataRequest,
} from './types';

export { ApiError } from './apiError';
import { ApiError } from './apiError';

export function promqlErrorType(error: unknown): string | undefined {
  return error instanceof ApiError &&
    object(error.detail) &&
    typeof error.detail.errorType === 'string'
    ? error.detail.errorType
    : undefined;
}

async function checked(response: Response): Promise<Response> {
  if (!response.ok) {
    const body = await response.text();
    let detail: unknown = body;
    try {
      detail = JSON.parse(body);
    } catch {
      /* Backend errors can be plain text. */
    }
    const message =
      object(detail) && strings(detail.non_existent_roles)
        ? `Roles do not exist: ${detail.non_existent_roles.join(', ')}`
        : object(detail) && strings(detail.roles_not_assigned)
          ? `Roles are not assigned: ${detail.roles_not_assigned.join(', ')}`
          : object(detail) && typeof detail.message === 'string'
            ? detail.message
            : object(detail) && typeof detail.error === 'string'
              ? detail.error
              : body || response.statusText;
    throw new ApiError(message || `Request failed (${response.status})`, response.status, detail);
  }
  return response;
}

function malformed(endpoint: string): never {
  throw new ApiError(`Unexpected response from ${endpoint}`, 502);
}

async function readJson(
  endpoint: string,
  init: RequestInit,
  onUnauthorized?: () => void,
): Promise<unknown> {
  const response = await request(endpoint, init, onUnauthorized);
  try {
    return await response.json();
  } catch {
    return malformed(endpoint);
  }
}

async function request(endpoint: string, init: RequestInit, onUnauthorized?: () => void) {
  const response = await fetch(endpoint, { ...init, credentials: 'include' });
  if (response.status === 401) {
    try {
      return await checked(response);
    } catch (error) {
      if (error instanceof ApiError) {
        if (endpoint.startsWith('/prometheus/api/v1/') && promqlErrorType(error) === 'forbidden')
          throw new ApiError(error.message, 403, error.detail);
        if (/^\/api\/v1\/(alerts|targets)(?:[/?]|$)/.test(endpoint)) {
          // AlertError wraps actix errors on the wire. These are dataset authorization
          // failures from user_auth_for_alert_config/user_auth_for_datasets, not session expiry.
          const message = error.message.replace(/^ActixError: /, '');
          if (
            message.startsWith('User does not have access to stream- ') ||
            message === 'User does not have access to PromQL alert stream'
          )
            throw new ApiError(`Permission denied: ${message}`, 403, error.detail);
          if (message.startsWith('Stream not found: '))
            throw new ApiError(message, 404, error.detail);
        }
      }
      onUnauthorized?.();
      throw error;
    }
  }
  try {
    return await checked(response);
  } catch (error) {
    if (error instanceof ApiError) {
      // These handlers wrap a dataset authorization failure in QueryError.
      // Match the known wrapper and status without treating it as session expiry.
      const trace = /^\/api\/prism\/v1\/services\/traces\/(list|detail)$/.test(endpoint);
      const denied =
        trace && error.status === 500
          ? /^Query error: Error: (User does not have access to stream- .+)$/.exec(error.message)
          : endpoint === '/api/v1/query' && error.status === 400
            ? /^Error: (User does not have access to stream- .+)$/.exec(error.message)
            : null;
      if (denied) throw new ApiError(`Permission denied: ${denied[1]}`, 403, error.detail);
    }
    throw error;
  }
}

async function readText(endpoint: string, init: RequestInit, onUnauthorized?: () => void) {
  const response = await request(endpoint, init, onUnauthorized);
  const text = await response.text();
  if (!text.trim()) return malformed(endpoint);
  return text;
}

function jsonBody(method: string, body: unknown): RequestInit {
  return { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) };
}
function dashboardRevisionHeaders(revision?: string): Record<string, string> {
  return revision ? { 'If-Match': revision.startsWith('"') ? revision : `"${revision}"` } : {};
}

const seconds = (value: number) => String(Math.round(value * 1000) / 1000);

function metadataParams({ stream, start, end, match, limit }: PromqlMetadataRequest) {
  const params = new URLSearchParams({ stream });
  if (start !== undefined) params.set('start', seconds(start));
  if (end !== undefined) params.set('end', seconds(end));
  if (limit !== undefined) params.set('limit', String(limit));
  for (const selector of match ?? []) params.append('match[]', selector);
  return params;
}

export function createClient({
  mode,
  onUnauthorized,
}: {
  mode: 'demo' | 'live';
  onUnauthorized?: () => void;
}): ParseableClient {
  if (mode === 'demo' && demoEnabled) return createDemoClient();
  const base = '/api/v1';
  const prometheus = '/prometheus/api/v1';
  const keys = '/api/prism/v1/apikeys';
  const alertPath = (id: string) => `${base}/alerts/${encodeURIComponent(id)}`;
  const targetPath = (id: string) => `${base}/targets/${encodeURIComponent(id)}`;
  const dashboardPath = (id: string) => `${base}/dashboards/${encodeURIComponent(id)}`;
  const user = (id: string) => `${base}/user/${encodeURIComponent(id)}`;
  const datasetPath = (name: string) => `${base}/logstream/${encodeURIComponent(name)}`;
  const role = (name: string) => {
    if (name.toLowerCase() === 'default')
      throw new ApiError(
        'The name default is reserved for the default OIDC role endpoint. Choose another name.',
        400,
      );
    return `${base}/role/${encodeURIComponent(name)}`;
  };
  const mutate = async (endpoint: string, init: RequestInit) => {
    await request(endpoint, init, onUnauthorized);
  };
  const readPromql = async (endpoint: string, init: RequestInit) => {
    const data = await readJson(endpoint, init, onUnauthorized);
    if (object(data) && data.status === 'error') {
      if (typeof data.error !== 'string' || typeof data.errorType !== 'string')
        return malformed(endpoint);
      const statuses: Record<string, number> = {
        bad_data: 400,
        execution: 422,
        forbidden: 403,
        unavailable: 503,
        timeout: 503,
      };
      await checked(
        new Response(JSON.stringify(data), { status: statuses[data.errorType] ?? 422 }),
      );
    }
    if (!successEnvelope(data)) return malformed(endpoint);
    return data;
  };
  const metadata = async (endpoint: string, signal?: AbortSignal) => {
    const data = await readPromql(endpoint, { signal });
    if (!strings(data.data)) return malformed(endpoint);
    return {
      data: data.data,
      truncated: data.warnings?.some((warning) => warning.includes('truncated')) ?? false,
    };
  };
  const readAlert = async (endpoint: string, init: RequestInit) => {
    const data = await readJson(endpoint, init, onUnauthorized);
    if (!alert(data)) return malformed(endpoint);
    return data;
  };
  const readTarget = async (endpoint: string, init: RequestInit) => {
    const data = await readJson(endpoint, init, onUnauthorized);
    if (!alertTarget(data)) return malformed(endpoint);
    return data;
  };
  const readDashboard = async (endpoint: string, init: RequestInit) => {
    const data = await readJson(endpoint, init, onUnauthorized);
    if (!dashboard(data)) return malformed(endpoint);
    return data;
  };
  const readLogView = async (endpoint: string, init: RequestInit) => {
    const data = await readJson(endpoint, init, onUnauthorized);
    if (!savedLogView(data)) return malformed(endpoint);
    return data;
  };
  const logViewPath = (id: string) => `${base}/filters/${encodeURIComponent(id)}`;
  return {
    async getAlertTargetPolicy(signal) {
      return decodeAlertTargetPolicy(
        await readJson(`${base}/alert-target-policy`, { signal }, onUnauthorized),
      );
    },
    async putAlertTargetPolicy(policy) {
      const body = decodeAlertTargetPolicy(policy);
      return decodeAlertTargetPolicy(
        await readJson(`${base}/alert-target-policy`, jsonBody('PUT', body), onUnauthorized),
      );
    },
    async listLogViews(signal) {
      const endpoint = `${base}/filters`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!Array.isArray(data) || !data.every(savedLogView)) return malformed(endpoint);
      return data;
    },
    createLogView: (body) => readLogView(`${base}/filters`, jsonBody('POST', body)),
    updateLogView: (id, body) => readLogView(logViewPath(id), jsonBody('PUT', body)),
    convertLogViewToSql: (id) =>
      readLogView(`${logViewPath(id)}/convert-to-sql`, { method: 'POST' }),
    deleteLogView: (id) => mutate(logViewPath(id), { method: 'DELETE' }),
    async logContext(body, signal) {
      const endpoint = `${base}/query/context`;
      const data = await readJson(endpoint, { ...jsonBody('POST', body), signal }, onUnauthorized);
      if (!logContextResponse(data)) return malformed(endpoint);
      return data;
    },
    async listDashboards(signal) {
      const endpoint = `${base}/dashboards?limit=0`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!Array.isArray(data) || !data.every(dashboardSummary)) return malformed(endpoint);
      return data;
    },
    getDashboard: (id, signal) => readDashboard(dashboardPath(id), { signal }),
    createDashboard: (body) => readDashboard(`${base}/dashboards`, jsonBody('POST', body)),
    updateDashboard: (id, body) =>
      readDashboard(dashboardPath(id), {
        ...jsonBody('PUT', body),
        headers: {
          'Content-Type': 'application/json',
          ...dashboardRevisionHeaders(body.revision),
        },
      }),
    setDashboardFavorite: (id, isFavorite) =>
      readDashboard(`${dashboardPath(id)}?isFavorite=${isFavorite}`, { method: 'PUT' }),
    async deleteDashboard(id, revision) {
      try {
        await mutate(dashboardPath(id), {
          method: 'DELETE',
          headers: dashboardRevisionHeaders(revision),
        });
      } catch (error) {
        if (error instanceof ApiError && error.status === 412)
          throw new ApiError(
            'This dashboard changed. Reload and review it before deleting.',
            412,
            error.detail,
          );
        throw error;
      }
    },
    async listAlerts(signal) {
      // The server caps each page at 1000 and returns no total. Never silently truncate.
      // It sorts by state, severity and title before offsetting, so an alert changing state
      // between pages can appear twice or be missed. Keep the first copy of each id.
      const result = [],
        seen = new Set<string>();
      for (let offset = 0; ; offset += 1000) {
        const endpoint = `${base}/alerts?limit=1000&offset=${offset}`;
        const data = await readJson(endpoint, { signal }, onUnauthorized);
        if (!Array.isArray(data) || !data.every(alertSummary)) return malformed(endpoint);
        for (const item of data) {
          if (seen.has(item.id)) continue;
          seen.add(item.id);
          result.push({ ...item, severity: item.severity.toLowerCase() as AlertSeverity });
        }
        if (data.length < 1000) return result;
      }
    },
    getAlert: (id, signal) => readAlert(alertPath(id), { signal }),
    createAlert: (body) => readAlert(`${base}/alerts`, jsonBody('POST', body)),
    updateAlert: (id, body) => readAlert(alertPath(id), jsonBody('PUT', body)),
    deleteAlert: (id) => mutate(alertPath(id), { method: 'DELETE' }),
    enableAlert: (id) => readAlert(`${alertPath(id)}/enable`, { method: 'PATCH' }),
    disableAlert: (id) => readAlert(`${alertPath(id)}/disable`, { method: 'PATCH' }),
    muteAlert: (id, state) =>
      readAlert(`${alertPath(id)}/update_notification_state`, jsonBody('PATCH', { state })),
    evaluateAlert: (id) => readAlert(`${alertPath(id)}/evaluate_alert`, { method: 'PUT' }),
    async listAlertTags(signal) {
      const endpoint = `${base}/alerts/list_tags`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!strings(data)) return malformed(endpoint);
      return data;
    },
    async listAlertTargets(signal) {
      const endpoint = `${base}/targets`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!Array.isArray(data) || !data.every(alertTargetStatus)) return malformed(endpoint);
      return data;
    },
    async getAlertTarget(id, signal) {
      const endpoint = targetPath(id);
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!alertTargetStatus(data)) return malformed(endpoint);
      return data;
    },
    createAlertTarget: (body) => readTarget(`${base}/targets`, jsonBody('POST', body)),
    updateAlertTarget: (id, body) => readTarget(targetPath(id), jsonBody('PUT', body)),
    deleteAlertTarget: (id) => readTarget(targetPath(id), { method: 'DELETE' }),
    async clusterNodes(signal) {
      return clusterNodes(await readJson(`${base}/cluster/info`, { signal }, onUnauthorized));
    },
    async clusterMetrics(signal) {
      return clusterMetrics(await readJson(`${base}/cluster/metrics`, { signal }, onUnauthorized));
    },
    async about(signal) {
      const endpoint = `${base}/about`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (
        !object(data) ||
        (data.oidcActive !== undefined && typeof data.oidcActive !== 'boolean') ||
        (data.capabilities !== undefined && !object(data.capabilities))
      )
        return malformed(endpoint);
      const capabilities = object(data.capabilities) ? data.capabilities : {};
      if (
        [
          'oidcRoleMapping',
          'oidcRoleSync',
          'promql',
          'promqlAlerts',
          'promqlDashboard',
          'promqlMetadata',
        ].some(
          (field) => capabilities[field] !== undefined && typeof capabilities[field] !== 'boolean',
        )
      )
        return malformed(endpoint);
      return {
        oidcActive: data.oidcActive === true,
        capabilities: {
          oidcRoleMapping: capabilities.oidcRoleMapping === true,
          oidcRoleSync: capabilities.oidcRoleSync === true,
          promqlAlerts: capabilities.promqlAlerts === true,
          promqlDashboard: capabilities.promqlDashboard === true,
          promqlMetadata: capabilities.promqlMetadata === true,
          promql:
            typeof capabilities.promql === 'boolean'
              ? capabilities.promql
              : object(data.license) &&
                typeof data.license.plan === 'string' &&
                data.license.plan !== 'OSS',
        },
      };
    },
    async listUsers(signal) {
      const endpoint = `${base}/users`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!Array.isArray(data)) return malformed(endpoint);
      return data.map((item) => {
        if (!object(item)) return malformed(endpoint);
        const groups = item.groupRoles ?? item.group_roles;
        const memberships = item.userGroups ?? item.user_groups;
        if (
          typeof item.id !== 'string' ||
          typeof item.username !== 'string' ||
          (item.method !== 'native' && item.method !== 'oauth') ||
          !userRoles(item.roles) ||
          !groupRoles(groups) ||
          !strings(memberships) ||
          (item.email != null && typeof item.email !== 'string') ||
          (item.picture != null && typeof item.picture !== 'string')
        )
          return malformed(endpoint);
        return {
          id: item.id,
          username: item.username,
          method: item.method,
          email: typeof item.email === 'string' ? item.email : undefined,
          picture: typeof item.picture === 'string' ? item.picture : undefined,
          roles: item.roles,
          groupRoles: groups,
          userGroups: memberships,
        };
      });
    },
    createUser: (name, assigned) =>
      readText(user(name), jsonBody('POST', assigned), onUnauthorized),
    deleteUser: (id) => mutate(user(id), { method: 'DELETE' }),
    addUserRoles: (id, assigned) => mutate(`${user(id)}/role/add`, jsonBody('PATCH', assigned)),
    removeUserRoles: (id, assigned) =>
      mutate(`${user(id)}/role/remove`, jsonBody('PATCH', assigned)),
    resetPassword: (id) =>
      readText(`${user(id)}/generate-new-password`, { method: 'POST' }, onUnauthorized),
    async userRoleSources(id, signal) {
      const endpoint = `${user(id)}/role`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!object(data)) return malformed(endpoint);
      const normalized = { ...data, groupRoles: data.groupRoles ?? data.group_roles };
      if (!roleSources(normalized)) return malformed(endpoint);
      return normalized;
    },
    async listRoles(signal) {
      const endpoint = `${base}/roles`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!roles(data)) return malformed(endpoint);
      return data;
    },
    async putRole(name, actions) {
      await mutate(role(name), jsonBody('PUT', actions));
    },
    async deleteRole(name) {
      await mutate(role(name), { method: 'DELETE' });
    },
    async defaultRole(signal) {
      const endpoint = `${base}/role/default`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (data !== null && typeof data !== 'string') return malformed(endpoint);
      return data;
    },
    setDefaultRole: (name) => mutate(`${base}/role/default`, jsonBody('PUT', name)),
    clearDefaultRole: () => mutate(`${base}/role/default`, { method: 'DELETE' }),
    async listApiKeys(signal) {
      const data = await readJson(keys, { signal }, onUnauthorized);
      if (!Array.isArray(data) || !data.every(apiKey)) return malformed(keys);
      return data;
    },
    async createApiKey(keyName, assigned, ttlSeconds) {
      const data = await readJson(
        keys,
        jsonBody('POST', {
          keyName,
          roles: assigned,
          ...(ttlSeconds === undefined ? {} : { ttlSeconds }),
        }),
        onUnauthorized,
      );
      if (!apiKey(data)) return malformed(keys);
      return data;
    },
    deleteApiKey: (id) => mutate(`${keys}/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    createDataset: (name, options) => {
      const headers: Record<string, string> = { 'X-P-Telemetry-Type': options.telemetryType };
      if (options.customPartition) headers['X-P-Custom-Partition'] = options.customPartition;
      if (options.logSource) headers['X-P-Log-Source'] = options.logSource;
      return mutate(datasetPath(name), { method: 'PUT', headers });
    },
    deleteDataset: (name) => mutate(datasetPath(name), { method: 'DELETE' }),
    async datasetStats(name, signal) {
      const endpoint = `${datasetPath(name)}/stats`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!datasetStats(data)) return malformed(endpoint);
      return data;
    },
    async datasetRetention(name, signal) {
      const endpoint = `${datasetPath(name)}/retention`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (!datasetRetention(data)) return malformed(endpoint);
      return data;
    },
    setDatasetRetention: (name, retention) =>
      mutate(`${datasetPath(name)}/retention`, jsonBody('PUT', retention)),
    async datasetHotTier(name, signal) {
      const endpoint = `${datasetPath(name)}/hottier`;
      let data: unknown;
      try {
        data = await readJson(endpoint, { signal }, onUnauthorized);
      } catch (error) {
        // A configured server reports a missing per-dataset hot tier as a validation error.
        if (
          error instanceof ApiError &&
          [400, 404, 500].includes(error.status) &&
          /Hot tier not found for stream /.test(error.message)
        )
          return null;
        throw error;
      }
      if (!datasetHotTier(data)) return malformed(endpoint);
      return data;
    },
    setDatasetHotTier: (name, size) =>
      mutate(`${datasetPath(name)}/hottier`, jsonBody('PUT', { size })),
    clearDatasetHotTier: (name) => mutate(`${datasetPath(name)}/hottier`, { method: 'DELETE' }),
    async listDatasets(signal) {
      const endpoint = `${base}/logstream`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (
        !Array.isArray(data) ||
        !data.every((item) => object(item) && typeof item.name === 'string')
      )
        return malformed(endpoint);
      // This endpoint returns names only. Do not infer telemetry type from a name.
      return data.map((item) => ({ name: item.name, type: 'logs' }) satisfies Dataset);
    },
    async datasetInfo(name, signal) {
      const endpoint = `${base}/logstream/${encodeURIComponent(name)}/info`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (
        !object(data) ||
        (data.telemetryType !== undefined && typeof data.telemetryType !== 'string') ||
        (data.latestEventAt != null && typeof data.latestEventAt !== 'string') ||
        [
          'createdAt',
          'firstEventAt',
          'timePartition',
          'timePartitionLimit',
          'customPartition',
          'streamType',
        ].some((key) => data[key] != null && typeof data[key] !== 'string') ||
        (data.hotTierEnabled !== undefined && typeof data.hotTierEnabled !== 'boolean') ||
        (data.staticSchemaFlag !== undefined && data.staticSchemaFlag !== 'true') ||
        (data.logSource !== undefined &&
          (!Array.isArray(data.logSource) ||
            !data.logSource.every(
              (source) => object(source) && typeof source.log_source_format === 'string',
            )))
      )
        return malformed(endpoint);
      return {
        name,
        telemetryType: typeof data.telemetryType === 'string' ? data.telemetryType : undefined,
        logSourceFormats: Array.isArray(data.logSource)
          ? data.logSource.map((source) => source.log_source_format as string)
          : [],
        latestEventAt: typeof data.latestEventAt === 'string' ? data.latestEventAt : undefined,
        createdAt: typeof data.createdAt === 'string' ? data.createdAt : undefined,
        firstEventAt: typeof data.firstEventAt === 'string' ? data.firstEventAt : undefined,
        timePartition: typeof data.timePartition === 'string' ? data.timePartition : undefined,
        timePartitionLimit:
          typeof data.timePartitionLimit === 'string' ? data.timePartitionLimit : undefined,
        customPartition:
          typeof data.customPartition === 'string' ? data.customPartition : undefined,
        streamType: typeof data.streamType === 'string' ? data.streamType : undefined,
        staticSchemaFlag: data.staticSchemaFlag === 'true' ? true : undefined,
        hotTierEnabled: typeof data.hotTierEnabled === 'boolean' ? data.hotTierEnabled : undefined,
      };
    },
    async promqlQuery({ stream, query, time }, signal) {
      const endpoint = `${prometheus}/query`;
      const body = new URLSearchParams({ stream, query });
      if (time !== undefined) body.set('time', seconds(time));
      const data = await readPromql(endpoint, {
        method: 'POST',
        signal,
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body,
      });
      if (!instantResult(data.data)) return malformed(endpoint);
      return data.data;
    },
    async promqlQueryRange({ stream, query, start, end, step }, signal) {
      const endpoint = `${prometheus}/query_range`;
      const data = await readPromql(endpoint, {
        method: 'POST',
        signal,
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          stream,
          query,
          start: seconds(start),
          end: seconds(end),
          step,
        }),
      });
      if (!rangeResult(data.data)) return malformed(endpoint);
      return data.data;
    },
    promqlLabels: (params, signal) =>
      metadata(`${prometheus}/labels?${metadataParams(params)}`, signal),
    promqlLabelValues: (label, params, signal) =>
      metadata(
        `${prometheus}/label/${encodeURIComponent(label)}/values?${metadataParams(params)}`,
        signal,
      ),
    async listTraces(body, signal) {
      const endpoint = '/api/prism/v1/services/traces/list';
      const data = await readJson(endpoint, { ...jsonBody('POST', body), signal }, onUnauthorized);
      if (!traceListResponse(data)) return malformed(endpoint);
      return data;
    },
    async traceDetail(body, signal) {
      const endpoint = '/api/prism/v1/services/traces/detail';
      const data = await readJson(endpoint, { ...jsonBody('POST', body), signal }, onUnauthorized);
      const detail = decodeTraceDetail(data);
      if (!detail) return malformed(endpoint);
      return detail;
    },
    async schema(dataset, signal) {
      const endpoint = `${base}/logstream/${encodeURIComponent(dataset)}/schema`;
      const data = await readJson(endpoint, { signal }, onUnauthorized);
      if (
        !object(data) ||
        !Array.isArray(data.fields) ||
        !data.fields.every((field) => object(field) && typeof field.name === 'string')
      )
        return malformed(endpoint);
      return data.fields.map((field) => field.name as string);
    },
    async query({ sql, startTime, endTime }, signal) {
      const endpoint = `${base}/query`;
      const data = await readJson(
        endpoint,
        {
          method: 'POST',
          signal,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: sql, startTime, endTime, sendNull: true }),
        },
        onUnauthorized,
      );
      if (!Array.isArray(data) || !data.every(object)) return malformed(endpoint);
      return data as LogRecord[];
    },
    async identity(signal) {
      const fallback = cookieIdentity();
      if (!fallback) return undefined;
      const endpoint = `${base}/users/${encodeURIComponent(fallback.id)}`;
      try {
        const data = await readJson(endpoint, { signal }, onUnauthorized);
        if (!object(data) || typeof data.id !== 'string' || typeof data.username !== 'string')
          return malformed(endpoint);
        return {
          id: data.id,
          username: data.username,
          method: typeof data.method === 'string' ? data.method : undefined,
          email: typeof data.email === 'string' ? data.email : undefined,
          source: 'server',
        };
      } catch (error) {
        if (signal?.aborted || (error instanceof ApiError && error.status === 401)) throw error;
        // Identity hints are display-only; a missing/unavailable profile must not
        // make the workspace unusable or grant access to protected requests.
        return fallback;
      }
    },
    async login(username, password, returnPath = '/') {
      if (!username || username.includes(':'))
        throw new ApiError('Enter a username without a colon', 400);
      const bytes = new TextEncoder().encode(`${username}:${password}`);
      const credentials = btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(''));
      await checked(
        await fetch(authEndpoint('login', returnPath), {
          credentials: 'include',
          headers: { Authorization: `Basic ${credentials}` },
          redirect: 'follow',
        }),
      );
      // Verify the exchanged cookie independently; never persist the password or
      // resend Basic auth with data requests.
      try {
        await readJson(`${base}/logstream`, {});
      } catch (error) {
        // A restricted account may sign in successfully without stream-list access.
        if (!(error instanceof ApiError && error.status === 403)) throw error;
      }
    },
    async logout() {
      // OIDC logout can cross origins; a fetch cannot complete that browser flow.
      window.location.assign(authEndpoint('logout', '/login'));
    },
  };
}
