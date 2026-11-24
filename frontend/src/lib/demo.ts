import { decodeAlertTargetPolicy, emptyAlertTargetPolicy } from './networkPolicy';
import { demoClusterNodes, demoClusterMetrics } from './clusterDemo';
import type {
  ApiKey,
  Dataset,
  LogRecord,
  ParseableClient,
  QueryRequest,
  Roles,
  TeamUser,
  UserRoleSources,
} from './types';
import { ApiError } from './apiError';
import { parseEventTimestamp } from '../components/explorer/timestamp';
import { createDemoMetrics } from './demoMetrics';
import { createDemoAlerts } from './demoAlerts';
import { createDemoDashboards } from './demoDashboards';
import { createDemoDatasets } from './demoDatasets';
import { createDemoLogMethods } from './demoLogs';
import { createDemoTraceRecords, createDemoTraceMethods } from './demoTraces';
import { handleApmDemoQuery } from './apmDemo';
import { errorDemoQueryTarget, handleErrorDemoQuery } from './errorAnalyticsDemo';
import { alertDuration } from '../features/alerts/helpers';
import { validateName } from '../features/team/helpers';

export const demoDatasets: Dataset[] = [
  { name: 'application_logs', type: 'logs' },
  { name: 'api_logs', type: 'logs' },
  { name: 'infrastructure_logs', type: 'logs' },
  { name: 'demo_metrics', type: 'metrics' },
  { name: 'demo_traces', type: 'traces' },
];

/** Fixed sequences anchored to now keep the initial relative time range useful. */
export function createDemoRecords(now: number = Date.now()): LogRecord[] {
  const services = ['api-gateway', 'checkout', 'inventory', 'auth', 'worker'];
  const messages = [
    'Request completed successfully',
    'Order created and queued for fulfillment',
    'Inventory cache refreshed',
    'Session token validated',
    'Background job completed',
  ];
  return Array.from({ length: 100 }, (_, index) => {
    const level = index % 13 === 0 ? 'ERROR' : index % 7 === 0 ? 'WARN' : 'INFO';
    return {
      p_timestamp: new Date(now - index * 8_000).toISOString(),
      level,
      service: services[index % services.length],
      message:
        level === 'ERROR'
          ? 'Upstream request timed out after 3000ms'
          : level === 'WARN'
            ? 'Retrying request after a transient connection failure'
            : messages[index % messages.length],
      host: `prod-node-${String((index % 4) + 1).padStart(2, '0')}`,
      environment: 'production',
      region: index % 3 === 0 ? 'eu-west-1' : 'us-east-1',
      method: index % 3 === 0 ? 'POST' : 'GET',
      path: ['/api/orders', '/api/products', '/health', '/api/session'][index % 4],
      status_code: level === 'ERROR' ? 504 : level === 'WARN' ? 429 : 200,
      duration_ms: level === 'ERROR' ? 3000 : 12 + ((index * 37) % 240),
      trace_id: `8e74f620af734b9086bb${index.toString(16).padStart(12, '0')}`,
    };
  });
}

export const demoLogs = createDemoRecords();
type Token = { kind: 'identifier' | 'string' | 'word' | 'number' | 'symbol'; value: string };
const unsupported = () =>
  new Error(
    'Demo SQL supports SELECT * FROM a sample dataset, grouped AND/OR filters (= or !=), message ILIKE search, ORDER BY and LIMIT. Connect to a server for other SQL.',
  );

function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  const expression =
    /\s+|"(?:[^"]|"")*"|'(?:[^']|'')*'|>=|<=|!=|<>|[<>=*,();]|[A-Za-z_][A-Za-z_0-9]*|\d+/gy;
  let offset = 0;
  while (offset < sql.length) {
    expression.lastIndex = offset;
    const match = expression.exec(sql);
    if (!match) throw unsupported();
    const value = match[0];
    offset = expression.lastIndex;
    if (/^\s+$/.test(value)) continue;
    if (value[0] === '"')
      tokens.push({ kind: 'identifier', value: value.slice(1, -1).replaceAll('""', '"') });
    else if (value[0] === "'")
      tokens.push({ kind: 'string', value: value.slice(1, -1).replaceAll("''", "'") });
    else if (/^\d+$/.test(value)) tokens.push({ kind: 'number', value });
    else if (/^[A-Za-z_]/.test(value)) tokens.push({ kind: 'word', value });
    else tokens.push({ kind: 'symbol', value });
  }
  return tokens;
}

function likeRegex(pattern: string, escape: string): RegExp {
  let expression = '';
  for (let index = 0; index < pattern.length; index++) {
    let character = pattern[index];
    if (character === escape) {
      character = pattern[++index];
      if (character === undefined) throw unsupported();
    } else if (character === '%') {
      expression += '[\\s\\S]*';
      continue;
    } else if (character === '_') {
      expression += '[\\s\\S]';
      continue;
    }
    expression += character.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp(`^${expression}$`, 'i');
}

export function executeDemoQuery(
  request: QueryRequest,
  records: LogRecord[],
  datasets: Dataset[] = demoDatasets,
): LogRecord[] {
  return evaluateDemoQuery(request, records, datasets, 100);
}

/** Aggregate inputs must scan all matching demo rows, independent of display limits. */
function evaluateDemoQuery(
  request: QueryRequest,
  records: LogRecord[],
  datasets: Dataset[],
  defaultLimit: number,
): LogRecord[] {
  const histogram =
    /^SELECT DATE_BIN\(INTERVAL '(\d+) milliseconds', ("(?:[^"]|"")*"), TIMESTAMP '([^']+)'\) AS "bucket_start", COUNT\(\*\) AS "count" FROM ([\s\S]+) GROUP BY "bucket_start" ORDER BY "bucket_start" ASC$/i.exec(
      request.sql,
    );
  if (histogram) {
    const interval = Number(histogram[1]),
      origin = Date.parse(histogram[3]);
    if (!Number.isSafeInteger(interval) || interval < 1 || !Number.isFinite(origin))
      throw unsupported();
    const rows = evaluateDemoQuery(
      { ...request, sql: `SELECT * FROM ${histogram[4]}` },
      records,
      datasets,
      Infinity,
    );
    const buckets = new Map<number, number>();
    for (const row of rows) {
      const bucket =
        origin +
        Math.floor(
          (parseEventTimestamp(row[histogram[2].slice(1, -1).replaceAll('""', '"')]) - origin) /
            interval,
        ) *
          interval;
      buckets.set(bucket, (buckets.get(bucket) ?? 0) + 1);
    }
    return [...buckets]
      .sort(([a], [b]) => a - b)
      .map(([start, count]) => ({ bucket_start: new Date(start).toISOString(), count }));
  }
  const aggregate =
    /^\s*SELECT\s+(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(\*|[a-zA-Z_][\w]*|"[^"]+")\s*\)\s*(?:AS\s+([\w]+|"[^"]+"))?\s+FROM\s+([\s\S]+)$/i.exec(
      request.sql,
    );
  if (aggregate) {
    const [, fn, rawField, alias, tail] = aggregate;
    const field = rawField.replace(/^"|"$/g, '');
    if (/\b(?:GROUP\s+BY|ORDER\s+BY|LIMIT)\b/i.test(tail)) throw unsupported();
    if (field !== '*' && records.length && !Object.hasOwn(records[0], field))
      throw new Error(`Unknown demo field: ${field}`);
    if (field === '*' && fn.toUpperCase() !== 'COUNT') throw unsupported();
    const rows = evaluateDemoQuery(
      { ...request, sql: `SELECT * FROM ${tail}` },
      records,
      datasets,
      Infinity,
    );
    const values = rows
      .map((row) => row[field])
      .filter((value): value is number => typeof value === 'number');
    let value: number | null = null;
    if (fn.toUpperCase() === 'COUNT')
      value = field === '*' ? rows.length : rows.filter((row) => row[field] != null).length;
    else if (values.length)
      value =
        fn.toUpperCase() === 'SUM'
          ? values.reduce((a, b) => a + b, 0)
          : fn.toUpperCase() === 'AVG'
            ? values.reduce((a, b) => a + b, 0) / values.length
            : fn.toUpperCase() === 'MIN'
              ? Math.min(...values)
              : Math.max(...values);
    return [{ [alias?.replace(/^"|"$/g, '') ?? `${fn.toLowerCase()}(${field})`]: value }];
  }
  const tokens = tokenize(request.sql);
  let position = 0;
  const peek = (value: string) =>
    ['word', 'symbol'].includes(tokens[position]?.kind) &&
    tokens[position].value.toUpperCase() === value;
  const expect = (value: string) => {
    if (!peek(value)) throw unsupported();
    position++;
  };
  const identifier = () => {
    const token = tokens[position++];
    if (!token || !['word', 'identifier'].includes(token.kind)) throw unsupported();
    return token.kind === 'word' ? token.value.toLowerCase() : token.value;
  };
  const literal = () => {
    const token = tokens[position++];
    if (token?.kind !== 'string') throw unsupported();
    return token.value;
  };
  const field = () => {
    const name = identifier();
    if (records.length && !Object.hasOwn(records[0], name))
      throw new Error(`Unknown demo field: ${name}`);
    return name;
  };
  expect('SELECT');
  expect('*');
  expect('FROM');
  const dataset = identifier();
  if (!datasets.some((item) => item.name === dataset))
    throw new Error(`Unknown demo dataset: ${dataset}`);
  if (!demoDatasets.some((item) => item.name === dataset)) records = [];
  type Predicate = (row: LogRecord) => boolean;
  function atom(): Predicate {
    if (peek('AND') || peek('OR') || peek(')')) throw unsupported();
    if (peek('(')) {
      position++;
      const predicate = expression();
      expect(')');
      return predicate;
    }
    if (peek('CAST')) {
      position++;
      expect('(');
      const name = field();
      expect('AS');
      expect('VARCHAR');
      expect(')');
      expect('ILIKE');
      const pattern = literal();
      expect('ESCAPE');
      const escape = literal();
      if (escape.length !== 1) throw unsupported();
      const matcher = likeRegex(pattern, escape);
      return (row) => row[name] != null && matcher.test(String(row[name]));
    }
    const name = field();
    const operation = tokens[position++]?.value;
    if (['>=', '<'].includes(operation) && peek('TIMESTAMP')) {
      position++;
      const value = Date.parse(literal());
      if (!Number.isFinite(value)) throw unsupported();
      return (row) =>
        operation === '>='
          ? parseEventTimestamp(row[name]) >= value
          : parseEventTimestamp(row[name]) < value;
    }
    if (!['=', '!=', '<>'].includes(operation)) throw unsupported();
    const token = tokens[position++];
    if (!token || !['string', 'number'].includes(token.kind)) throw unsupported();
    return (row) =>
      row[name] != null &&
      (operation === '=' ? String(row[name]) === token.value : String(row[name]) !== token.value);
  }
  function conjunction(): Predicate {
    let predicate = atom();
    while (peek('AND')) {
      position++;
      const left = predicate,
        right = atom();
      predicate = (row) => left(row) && right(row);
    }
    return predicate;
  }
  function expression(): Predicate {
    let predicate = conjunction();
    while (peek('OR')) {
      position++;
      const left = predicate,
        right = conjunction();
      predicate = (row) => left(row) || right(row);
    }
    return predicate;
  }
  let predicate: Predicate = () => true;
  if (peek('WHERE')) {
    position++;
    predicate = expression();
  }
  let orderField: string | undefined;
  let descending = false;
  if (peek('ORDER')) {
    position++;
    expect('BY');
    orderField = field();
    if (peek('DESC')) {
      descending = true;
      position++;
    } else if (peek('ASC')) position++;
  }
  let limit = defaultLimit;
  if (peek('LIMIT')) {
    position++;
    const token = tokens[position++];
    if (token?.kind !== 'number') throw unsupported();
    limit = Number(token.value);
    if (!Number.isSafeInteger(limit) || limit > 10_000)
      throw new Error('Demo limit must be between 0 and 10000');
  }
  if (peek(';')) position++;
  if (position !== tokens.length) throw unsupported();
  const now = Date.now(),
    duration = alertDuration(request.startTime);
  const start =
      duration !== undefined
        ? Math.floor((now - duration * 1000) / 60_000) * 60_000
        : Date.parse(request.startTime),
    end =
      request.endTime === 'now' ? Math.floor(now / 60_000) * 60_000 : Date.parse(request.endTime);
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end)
    throw new Error('Invalid query time range');
  const result = records.filter((row) => {
    const timestamp = parseEventTimestamp(row.p_timestamp);
    return timestamp >= start && timestamp < end && predicate(row);
  });
  if (orderField) {
    const key = orderField;
    result.sort((a, b) => {
      const av = a[key],
        bv = b[key];
      const comparison =
        typeof av === 'number' && typeof bv === 'number'
          ? av - bv
          : String(av).localeCompare(String(bv));
      return descending ? -comparison : comparison;
    });
  }
  return result.slice(0, limit).map((row) => ({ ...row }));
}

export function createDemoClient(): ParseableClient {
  let networkPolicy = emptyAlertTargetPolicy();
  const fixtureTime = Date.now();
  const records = createDemoRecords(fixtureTime - 1_000);
  const traceRows = createDemoTraceRecords(fixtureTime);
  const datasets = createDemoDatasets(demoDatasets, records, fixtureTime, {
    demo_traces: traceRows,
  });
  const metrics = createDemoMetrics(fixtureTime);
  const alerts = createDemoAlerts(fixtureTime);
  const dashboards = createDemoDashboards(fixtureTime);
  const roles: Roles = {
    administrators: [{ privilege: 'admin' }],
    analysts: [{ privilege: 'reader', resource: { stream: 'application_logs' } }],
    auditors: [{ privilege: 'reader', resource: { stream: 'api_logs' } }],
    editors: [{ privilege: 'editor' }],
    ingestors: [{ privilege: 'ingestor', resource: { stream: '*' } }],
    observers: [{ privilege: 'reader', resource: { stream: 'infrastructure_logs' } }],
    'provider-readers': [{ privilege: 'reader', resource: { stream: '*' } }],
    writers: [{ privilege: 'writer', resource: { llmKey: '*' } }],
  };
  let defaultRole: string | null = 'observers';
  type DemoUser = Omit<TeamUser, 'roles' | 'groupRoles'> & {
    assigned: string[];
    inherited: Record<string, string[]>;
    oidc?: UserRoleSources['oidc'];
  };
  const users: DemoUser[] = [
    {
      id: 'admin',
      username: 'admin',
      method: 'native',
      assigned: ['super-admin'],
      inherited: {},
      userGroups: [],
    },
    {
      id: 'analyst',
      username: 'analyst',
      method: 'native',
      email: 'analyst@example.test',
      assigned: ['analysts'],
      inherited: { analytics: ['auditors'] },
      userGroups: ['analytics'],
    },
    {
      id: 'https://issuer.example/sso-user',
      username: 'sso.user',
      method: 'oauth',
      email: 'sso@example.test',
      assigned: [],
      inherited: {},
      userGroups: [],
      oidc: {
        issuer: 'https://issuer.example',
        legacy: false,
        groups: ['provider-readers'],
        providerRoles: ['provider-readers'],
        manualRoles: ['auditors'],
        defaultRole,
      },
    },
    {
      id: 'https://issuer.example/fallback-user',
      username: 'sso.fallback',
      method: 'oauth',
      assigned: [],
      inherited: {},
      userGroups: [],
      oidc: {
        issuer: 'https://issuer.example',
        legacy: false,
        groups: [],
        providerRoles: [],
        manualRoles: [],
        defaultRole,
      },
    },
  ];
  const now = new Date().toISOString();
  const keys: ApiKey[] = [
    {
      keyId: apiKeyId(),
      apiKey: 'demo-ingestion-key-1234',
      keyName: 'ingestion',
      roles: ['ingestors'],
      createdBy: 'admin',
      createdAt: now,
      modifiedAt: now,
      expiresAt: null,
    },
    {
      keyId: apiKeyId(),
      apiKey: 'demo-query-key-5678',
      keyName: 'queries',
      roles: ['analysts'],
      createdBy: 'admin',
      createdAt: now,
      modifiedAt: now,
      expiresAt: null,
    },
  ];
  const clone = <T>(value: T): T => structuredClone(value);
  function apiKeyId() {
    const alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    let timestamp = Date.now();
    let prefix = '';
    for (let i = 0; i < 10; i++) {
      prefix = alphabet[timestamp % 32] + prefix;
      timestamp = Math.floor(timestamp / 32);
    }
    const random = crypto.getRandomValues(new Uint8Array(16));
    return prefix + Array.from(random, (byte) => alphabet[byte % 32]).join('');
  }
  function fail(message: string, status = 400): never {
    throw new ApiError(message, status);
  }
  function knownRoles(names: string[]) {
    const missing = names.filter((name) => !Object.hasOwn(roles, name));
    if (missing.length) fail(`Roles do not exist: ${missing.join(', ')}`);
  }
  // Demo roles can only hold admin, editor, reader, writer or ingestor, so
  // the built-in `super-admin` is the only protected role.
  function rejectProtectedRoles(names: string[]) {
    if (names.includes('super-admin')) fail('Role is protected');
  }
  // Like the backend, role additions report protected roles before unknown
  // ones, and API keys and user creation the other way round. `super-admin`
  // exists on the backend but not in the demo's role map.
  function assignableRoles(names: string[], { protectedFirst = false } = {}) {
    if (protectedFirst) rejectProtectedRoles(names);
    knownRoles(names.filter((name) => name !== 'super-admin'));
    rejectProtectedRoles(names);
  }
  function named(name: string, kind: 'user' | 'role') {
    const error = validateName(name, kind);
    if (error) fail(error);
  }
  function findUser(id: string, mutation = false) {
    const found = users.find((user) => user.id === id);
    if (!found) fail('User does not exist', 404);
    if (mutation && id === 'admin') fail('Cannot call this API for root admin user');
    return found;
  }
  function effective(user: DemoUser): string[] {
    if (!user.oidc || user.oidc.legacy) return user.assigned;
    const assigned = [
      ...new Set([...(user.oidc.manualRoles ?? []), ...(user.oidc.providerRoles ?? [])]),
    ];
    return assigned.length ? assigned : defaultRole ? [defaultRole] : [];
  }
  function privileges(names: string[]): TeamUser['roles'] {
    return Object.fromEntries(
      names.map((name) => [
        name,
        name === 'super-admin' ? [{ privilege: 'superadmin' }] : clone(roles[name] ?? []),
      ]),
    );
  }
  function sources(user: DemoUser): UserRoleSources {
    const convert = (names: string[]) =>
      Object.fromEntries(
        Object.entries(privileges(names)).map(([name, actions]) => [
          name,
          { actions, roleType: name === 'super-admin' ? ('internal' as const) : ('user' as const) },
        ]),
      );
    return {
      roles: convert(effective(user)),
      groupRoles: Object.fromEntries(
        Object.entries(user.inherited).map(([group, names]) => [group, convert(names)]),
      ),
      oidc: user.oidc ? { ...clone(user.oidc), defaultRole } : undefined,
    };
  }
  function findKey(id: string) {
    const found = keys.find((key) => key.keyId === id);
    if (!found) fail(`API key not found: ${id}`, 404);
    return found;
  }
  async function ready(signal?: AbortSignal) {
    signal?.throwIfAborted();
    await Promise.resolve();
    signal?.throwIfAborted();
  }
  return {
    ...datasets,
    ...createDemoLogMethods(records, datasets.listDatasets),
    ...createDemoTraceMethods(traceRows, datasets.listDatasets, datasets.datasetInfo),
    ...metrics,
    ...alerts,
    ...dashboards,
    async getAlertTargetPolicy(signal) {
      await ready(signal);
      return structuredClone(networkPolicy);
    },
    async putAlertTargetPolicy(policy) {
      await ready();
      networkPolicy = decodeAlertTargetPolicy(policy);
      return structuredClone(networkPolicy);
    },
    async clusterNodes(signal) {
      await ready(signal);
      return structuredClone(demoClusterNodes);
    },
    async clusterMetrics(signal) {
      await ready(signal);
      return structuredClone(demoClusterMetrics);
    },
    async about(signal) {
      await ready(signal);
      return {
        oidcActive: true,
        capabilities: {
          oidcRoleMapping: true,
          oidcRoleSync: true,
          promql: true,
          promqlAlerts: true,
          promqlDashboard: true,
          promqlMetadata: true,
        },
      };
    },
    async listRoles(signal) {
      await ready(signal);
      return clone(roles);
    },
    async putRole(name, actions) {
      await ready();
      named(name, 'role');
      if (
        actions.some(
          (action) =>
            !['admin', 'editor', 'reader', 'writer', 'ingestor'].includes(action.privilege),
        )
      )
        fail('Cannot create a role with superadmin privilege.');
      roles[name] = clone(actions);
    },
    async deleteRole(name) {
      await ready();
      if (name.toLowerCase() === 'default') fail(validateName(name, 'role')!);
      if (defaultRole === name)
        fail('Clear or change the default OIDC role before deleting this role.');
      if (
        users.some(
          (user) =>
            effective(user).includes(name) ||
            Object.values(user.inherited).some((names) => names.includes(name)),
        ) ||
        keys.some((key) => key.roles.includes(name))
      )
        fail('Cannot perform this operation as role is assigned to an existing user.');
      delete roles[name];
    },
    async defaultRole(signal) {
      await ready(signal);
      return defaultRole;
    },
    async setDefaultRole(name) {
      await ready();
      named(name, 'role');
      if (!Object.hasOwn(roles, name)) fail('Default OIDC role must name an existing role.');
      defaultRole = name;
    },
    async clearDefaultRole() {
      await ready();
      defaultRole = null;
    },
    async listUsers(signal) {
      await ready(signal);
      return users.map(({ assigned: _assigned, inherited, oidc: _oidc, ...user }) => ({
        ...clone(user),
        roles: privileges(effective(findUser(user.id))),
        groupRoles: Object.fromEntries(
          Object.entries(inherited).map(([group, names]) => [group, privileges(names)]),
        ),
      }));
    },
    async createUser(username, assigned) {
      await ready();
      if (username === 'admin') fail('Cannot call this API for root admin user');
      named(username, 'user');
      assignableRoles(assigned);
      if (!assigned.length) fail('User cannot be created without a role');
      if (
        users.some(
          (user) =>
            user.id === username || (user.method === 'native' && user.username === username),
        )
      )
        fail(`User ${username} already exists`);
      users.push({
        id: username,
        username,
        method: 'native',
        assigned: [...new Set(assigned)],
        inherited: {},
        userGroups: [],
      });
      return `demo-password-${crypto.randomUUID()}`;
    },
    async deleteUser(id) {
      await ready();
      const found = findUser(id, true);
      if (found.userGroups.length) fail('Cannot delete user while assigned to a group.');
      users.splice(users.indexOf(found), 1);
    },
    async addUserRoles(id, assigned) {
      await ready();
      const user = findUser(id, true);
      assignableRoles(assigned, { protectedFirst: true });
      if (user.oidc && !user.oidc.legacy)
        user.oidc.manualRoles = [...new Set([...(user.oidc.manualRoles ?? []), ...assigned])];
      else user.assigned = [...new Set([...user.assigned, ...assigned])];
    },
    async removeUserRoles(id, assigned) {
      await ready();
      const user = findUser(id, true);
      knownRoles(assigned);
      const missing = assigned.filter((name) => !effective(user).includes(name));
      if (missing.length) fail(`Roles are not assigned: ${missing.join(', ')}`);
      if (user.oidc && !user.oidc.legacy) {
        if (assigned.some((name) => !user.oidc?.manualRoles?.includes(name)))
          fail(
            'Provider and default OIDC roles cannot be removed as manual grants; change the identity-provider group or default role instead.',
          );
        user.oidc.manualRoles = (user.oidc.manualRoles ?? []).filter(
          (name) => !assigned.includes(name),
        );
      } else user.assigned = user.assigned.filter((name) => !assigned.includes(name));
    },
    async resetPassword(id) {
      await ready();
      const user = findUser(id, true);
      if (user.method !== 'native') fail('User does not exist', 404);
      return `demo-password-${crypto.randomUUID()}`;
    },
    async userRoleSources(id, signal) {
      await ready(signal);
      return sources(findUser(id));
    },
    async listApiKeys(signal) {
      await ready(signal);
      return keys.map((key) => ({ ...clone(key), apiKey: `****${key.apiKey.slice(-4)}` }));
    },
    async createApiKey(keyName, assigned, ttlSeconds) {
      await ready();
      assignableRoles(assigned);
      if (
        ttlSeconds != null &&
        (!Number.isInteger(ttlSeconds) || ttlSeconds < 1800 || ttlSeconds > 7776000)
      ) {
        fail('ttlSeconds must be an integer between 1800 and 7776000.');
      }
      if (!keyName.trim()) fail('Enter a name for the API key.');
      if (keys.some((key) => key.keyName === keyName)) fail(`Duplicate key name: ${keyName}`, 409);
      const timestamp = new Date().toISOString();
      const key: ApiKey = {
        keyId: apiKeyId(),
        apiKey: crypto.randomUUID(),
        keyName,
        roles: [...new Set(assigned)],
        createdBy: 'admin',
        createdAt: timestamp,
        modifiedAt: timestamp,
        expiresAt:
          ttlSeconds == null
            ? null
            : new Date(Date.parse(timestamp) + ttlSeconds * 1000).toISOString(),
      };
      keys.push(key);
      return clone(key);
    },
    async deleteApiKey(id) {
      await ready();
      const key = findKey(id);
      keys.splice(keys.indexOf(key), 1);
    },
    async query(request, signal) {
      await ready(signal);
      const current = await datasets.listDatasets(signal);
      if (request.sql.startsWith('/* parseable-apm:')) {
        const table = /FROM\s+"((?:[^"]|"")*)"/i.exec(request.sql)?.[1].replaceAll('""', '"');
        const item = current.find((dataset) => dataset.name === table);
        if (!item) throw new ApiError(`Unknown demo dataset: ${table}`, 404);
        if (item.type !== 'traces') throw new ApiError('APM requires a traces dataset', 400);
        const result = handleApmDemoQuery(request, item.name === 'demo_traces' ? traceRows : []);
        if (result !== undefined) return result;
      }
      const errors = errorDemoQueryTarget(request);
      if (errors) {
        if (!current.some((dataset) => dataset.name === errors.dataset))
          throw new ApiError(`Unknown demo dataset: ${errors.dataset}`, 404);
        const fields = new Set(await datasets.schema(errors.dataset, signal));
        const missing = errors.fields.filter((field) => !fields.has(field));
        if (missing.length) throw new ApiError(`Unknown demo field: ${missing[0]}`, 400);
        // Seeded datasets share the fixture records; created datasets start empty.
        const rows =
          errors.dataset === 'demo_traces'
            ? traceRows
            : demoDatasets.some((dataset) => dataset.name === errors.dataset)
              ? records
              : [];
        return handleErrorDemoQuery(request, rows)!;
      }
      return executeDemoQuery(
        request,
        /FROM\s+(?:"demo_traces"|demo_traces\b)/i.test(request.sql) ? traceRows : records,
        current,
      );
    },
    async identity(signal) {
      await ready(signal);
      return { id: 'demo', username: 'Demo user', source: 'demo' };
    },
    async login() {
      await ready();
    },
    async logout() {
      await ready();
    },
  };
}
