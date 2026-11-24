import type { ClusterNode, ClusterMetrics } from './operationsContract';
export type Dataset = { name: string; type: 'logs' | 'metrics' | 'traces' };
export type DatasetCreateOptions = {
  telemetryType: Dataset['type'];
  customPartition?: string;
  logSource?: 'json' | 'otel-logs' | 'otel-metrics' | 'otel-traces';
};
export type DatasetStats = {
  stream: string;
  time: string;
  ingestion: {
    count: number;
    size: number;
    format: string;
    lifetime_count: number;
    lifetime_size: number;
    deleted_count: number;
    deleted_size: number;
  };
  storage: { size: number; format: string; lifetime_size: number; deleted_size: number };
};
export type DatasetRetention = Array<{ description: string; action: 'delete'; duration: string }>;
export type DatasetHotTier = {
  version: string | null;
  size: string;
  used_size: string;
  available_size: string;
  oldest_date_time_entry?: string;
};
export type PromqlLabels = Record<string, string>;
export type PromqlSample = [number, string]; // [unix seconds, value string]
export type PromqlInstantResult =
  | { resultType: 'vector'; result: Array<{ metric: PromqlLabels; value: PromqlSample }> }
  | { resultType: 'matrix'; result: Array<{ metric: PromqlLabels; values: PromqlSample[] }> }
  | { resultType: 'scalar' | 'string'; result: PromqlSample };
export type PromqlRangeResult = {
  resultType: 'matrix';
  result: Array<{ metric: PromqlLabels; values: PromqlSample[] }>;
};
export type PromqlQueryRequest = { stream: string; query: string; time?: number };
export type PromqlRangeRequest = {
  stream: string;
  query: string;
  start: number;
  end: number;
  step: string;
};
export type PromqlMetadataRequest = {
  stream: string;
  start?: number;
  end?: number;
  match?: string[];
  limit?: number;
};
export type PromqlMetadataResult = { data: string[]; truncated: boolean };
export type DatasetInfo = {
  name: string;
  telemetryType?: string;
  logSourceFormats: string[];
  latestEventAt?: string;
  createdAt?: string;
  firstEventAt?: string;
  timePartition?: string;
  timePartitionLimit?: string;
  customPartition?: string;
  streamType?: string;
  staticSchemaFlag?: boolean;
  hotTierEnabled?: boolean;
};
export type LogRecord = Record<string, unknown>;
export type TraceSortBy =
  'mostRecent' | 'leastRecent' | 'longestFirst' | 'shortestFirst' | 'mostSpans' | 'leastSpans';
export type TraceListOption = 'traces' | 'errorSpans' | 'spans';
export type TraceListRequest = {
  dataset: string;
  serviceName?: string;
  startTime: string;
  endTime: string;
  sortBy?: TraceSortBy;
  options?: TraceListOption;
  limit?: number;
  offset?: number;
  conditions?: { conditions?: LogContextConditions; groupBy?: string[]; topK?: number };
};
export type TraceSpanRecord = Record<string, unknown> & {
  'service.name': string;
  span_name: string;
  span_duration_ns: number;
  span_trace_id: string;
  span_span_id: string;
  /** Timestamp text in UTC; the field name is inherited from the OTLP schema. */
  span_start_time_unix_nano: string;
  /** Numeric nanoseconds can exceed JS integer precision; use the timestamp text for dates. */
  span_start_time_unix_nano_epoch: number;
  total_span_count: number;
};
export type TraceListRecord = TraceSpanRecord & { p_timestamp: string; error_count: number };
export type TraceListResponse = {
  count: number;
  offset: number;
  limit: number;
  records: TraceListRecord[];
};
export type TraceDetailRequest = {
  dataset: string;
  traceId: string;
  startTime: string;
  endTime: string;
};
export type TraceDetailRecord = TraceSpanRecord & {
  span_parent_span_id: string;
  has_error: boolean;
  level: number;
  event_count: number;
};
export type TraceDetailResponse = {
  startTime: string;
  endTime: string;
  records: TraceDetailRecord[];
};
export type SavedLogViewRule = { id: string; field: string; value: string; operator: string };
export type SavedLogViewBuilder = {
  id: string;
  combinator: string;
  rules: Array<{ id: string; combinator: string; rules: SavedLogViewRule[] }>;
};
/** Full /filters document: retain flattened metadata when updating a saved view. */
export type SavedLogView = {
  filter_id?: string | null;
  filter_name: string;
  stream_name: string;
  query: {
    filter_type: 'filter' | 'sql' | 'search' | 'promql';
    filter_query?: string | null;
    filter_builder?: SavedLogViewBuilder | null;
  };
  explorer_state?: string | null;
  time_filter?: { from: string; to: string } | null;
  [key: string]: unknown;
};
export type LogContextConditions = {
  operator?: 'and' | 'or' | null;
  conditionConfig?: Array<{
    column: string;
    operator:
      | '='
      | '!='
      | '<'
      | '>'
      | '<='
      | '>='
      | 'is null'
      | 'is not null'
      | 'ilike'
      | 'contains'
      | 'begins with'
      | 'ends with'
      | 'does not contain'
      | 'does not begin with'
      | 'does not end with';
    value?: string | null;
    type?: string | null;
  }>;
  groups?: LogContextConditions[] | null;
};
export type LogContextRequest = {
  dataset: string;
  pTimestamp: string;
  contextWindow?: string;
  contextStartTime?: string;
  contextEndTime?: string;
  log?: string;
  body?: string;
  message?: string;
  conditions?: LogContextConditions;
  pageSize?: number;
};
/** Wire payload for POST /query; reverseRecords is applied by the caller. */
export type LogContextQueryPayload = {
  query: string;
  startTime: string;
  endTime: string;
  sendNull: boolean;
  reverseRecords: boolean;
};
export type LogContextResponse = {
  scope: 'contextWindow';
  contextStartTime: string;
  contextEndTime: string;
  limit: number;
  anchorIndex: number;
  duplicateAnchorCount: number;
  anchoredDuplicate: 'first';
  records: LogRecord[];
  queries: { previous: LogContextQueryPayload | null; next: LogContextQueryPayload | null };
};

export type TimeRange =
  | '10m'
  | '15m'
  | '30m'
  | '1h'
  | '5h'
  | '6h'
  | '1d'
  | '24h'
  | '3d'
  | '7d'
  | { startTime: string; endTime: string };
export type QueryRequest = { sql: string; startTime: string; endTime: string };
export type LogFilter = { id: string; field: string; operator: '=' | '!='; value: string };
export type SessionIdentity = {
  id: string;
  username: string;
  method?: string;
  email?: string;
  source: 'server' | 'cookie' | 'demo';
};
export type PrivilegeName = 'admin' | 'editor' | 'writer' | 'reader' | 'ingestor';
export type PrivilegeResource = { stream: string } | { llmKey: string } | 'all' | null;
export type Privilege =
  | { privilege: 'admin' | 'editor' }
  | { privilege: 'writer' | 'reader' | 'ingestor'; resource?: PrivilegeResource };
// The root user can expose this internal privilege; it is never an editable choice.
export type UserPrivilege = Privilege | { privilege: 'superadmin' };
export type Roles = Record<string, Privilege[]>;
export type TeamUser = {
  id: string;
  username: string;
  method: 'native' | 'oauth';
  email?: string;
  picture?: string;
  roles: Record<string, UserPrivilege[]>;
  groupRoles: Record<string, Record<string, UserPrivilege[]>>;
  userGroups: string[];
};
export type RoleSource = { actions: UserPrivilege[]; roleType: 'user' | 'internal' };
export type OidcRoleSources = {
  issuer: string | null;
  legacy: boolean;
  groups: string[] | null;
  manualRoles: string[] | null;
  providerRoles: string[] | null;
  defaultRole: string | null;
};
export type UserRoleSources = {
  roles: Record<string, RoleSource>;
  groupRoles: Record<string, Record<string, RoleSource>>;
  oidc?: OidcRoleSources;
};
export type About = {
  oidcActive: boolean;
  capabilities: {
    oidcRoleMapping: boolean;
    oidcRoleSync: boolean;
    promql: boolean;
    promqlAlerts: boolean;
    promqlDashboard: boolean;
    promqlMetadata: boolean;
  };
};
export type AlertSeverity = 'critical' | 'high' | 'medium' | 'low';
export type AlertState = 'triggered' | 'not-triggered' | 'disabled';
export type AlertQueryType = 'promql' | 'code' | 'builder';
export type ThresholdOperator = '>' | '<' | '=' | '!=' | '>=' | '<=';
export type NotificationState = string | { mute: string };
export type PromqlAlertConfig = { holdDuration?: string; [key: string]: unknown };
export type PromqlAlertRuntime = {
  health: 'ok' | 'noData' | 'error';
  error: string | null;
  lastEvaluatedAt: string | null;
  instances: Record<
    string,
    {
      labels: PromqlLabels;
      state: 'pending' | 'firing' | 'resolved';
      pendingSince: string | null;
      lastSeen: string;
      value: number;
    }
  >;
  deliveries: Array<{
    labels: PromqlLabels;
    value: number;
    firing: boolean;
    target: string;
    attempts: number;
    error: string | null;
  }>;
};
export type AlertSummary = {
  id: string;
  title: string;
  severity: AlertSeverity;
  state: AlertState;
  alertType: 'threshold' | 'anomaly' | 'forecast' | (string & {});
  datasets: string[];
  notificationState: NotificationState;
  created: string;
  tags?: string[] | null;
  lastTriggeredAt?: string | null;
  queryType?: AlertQueryType;
  promqlConfig?: PromqlAlertConfig;
  promqlRuntime?: PromqlAlertRuntime;
};
export type AlertRequest = {
  title: string;
  severity: AlertSeverity;
  query: string;
  queryType: AlertQueryType;
  datasets: string[];
  alertType: 'threshold';
  thresholdConfig: { operator: ThresholdOperator; value: number };
  evalConfig: { rollingWindow: { evalStart: string; evalEnd: string; evalFrequency: number } };
  notificationConfig: { interval: number };
  targets: string[];
  tags?: string[] | null;
  promqlConfig?: PromqlAlertConfig;
  [key: string]: unknown;
};
export type Alert = AlertRequest & {
  version: string;
  id: string;
  state: AlertState;
  notificationState: NotificationState;
  created: string;
  lastTriggeredAt: string | null;
  promqlRuntime?: PromqlAlertRuntime;
  executionIdentity?: { userId: string; tenantId: string };
};
export type AlertTargetType = 'slack' | 'webhook' | 'alertManager';
export type AlertTargetRequest =
  | { name: string; type: 'slack'; endpoint: string }
  | {
      name: string;
      type: 'webhook';
      endpoint: string;
      headers?: Record<string, string>;
      skipTlsCheck: boolean;
    }
  | {
      name: string;
      type: 'alertManager';
      endpoint: string;
      username?: string;
      password?: string;
      skipTlsCheck: boolean;
    };
// Alertmanager is returned as webhook by the backend; auth keys identify it.
export type AlertTarget = {
  id: string;
  name: string;
  type: AlertTargetType;
  endpoint: string;
  headers?: Record<string, string>;
  skipTlsCheck?: boolean;
  username?: string | null;
  password?: string | null;
};
export type AlertTargetStatus = { target: AlertTarget; enabled: boolean; error?: string };
export type ApiKey = {
  keyId: string;
  apiKey: string;
  keyName: string;
  roles: string[];
  createdBy: string;
  createdAt: string;
  modifiedAt: string;
  expiresAt?: string | null;
};
// Dashboard and tile extras are part of the classic contract. Keep the fetched document
// intact and patch it; a narrower editor model must never become the PUT body.
export type DashboardTile = { tile_id: string; [key: string]: unknown };
export type DashboardSummary = {
  /** Opaque server revision. Older servers may omit it. */
  revision?: string;
  dataset?: string | null;
  dashboardId: string;
  title: string;
  author?: string | null;
  created?: string | null;
  modified?: string | null;
  tags?: string[] | null;
  isFavorite?: boolean | null;
  [key: string]: unknown;
};
export type DashboardRequest = {
  revision?: string;
  /** Saved auto refresh default, in milliseconds; absent means off. */
  refreshInterval?: number;
  dataset?: string | null;
  title: string;
  tiles?: DashboardTile[] | null;
  tags?: string[] | null;
  isFavorite?: boolean | null;
  [key: string]: unknown;
};
export type Dashboard = DashboardSummary & DashboardRequest;
export type DashboardVariable = {
  name: string;
  label: string;
  type: 'promql' | 'promql_query' | 'sql' | 'list' | 'text' | 'dataset';
  dataset?: string;
  labelName?: string;
  metric?: string;
  labelFilters?: Array<{ label: string; operator: '=' | '!=' | '=~' | '!~'; value: string }>;
  options?: string[];
  sqlQuery?: string;
  promqlQuery?: string;
  promqlQueryDataset?: string;
  promqlQueryLabel?: string;
  includeAll?: boolean;
  defaultValue?: string;
  [key: string]: unknown;
};
export interface AlertTargetPolicy {
  allowPrivate: boolean;
  allowedDomains: string[];
  allowedCidrs: string[];
  deniedDomains: string[];
  deniedCidrs: string[];
  allowInvalidTls: boolean;
}

export interface ParseableClient {
  getAlertTargetPolicy(signal?: AbortSignal): Promise<AlertTargetPolicy>;
  putAlertTargetPolicy(policy: AlertTargetPolicy): Promise<AlertTargetPolicy>;
  identity(signal?: AbortSignal): Promise<SessionIdentity | undefined>;
  listDatasets(signal?: AbortSignal): Promise<Dataset[]>;
  datasetInfo(name: string, signal?: AbortSignal): Promise<DatasetInfo>;
  createDataset(name: string, options: DatasetCreateOptions): Promise<void>;
  deleteDataset(name: string): Promise<void>;
  datasetStats(name: string, signal?: AbortSignal): Promise<DatasetStats>;
  datasetRetention(name: string, signal?: AbortSignal): Promise<DatasetRetention>;
  setDatasetRetention(name: string, retention: DatasetRetention): Promise<void>;
  datasetHotTier(name: string, signal?: AbortSignal): Promise<DatasetHotTier | null>;
  setDatasetHotTier(name: string, size: string): Promise<void>;
  clearDatasetHotTier(name: string): Promise<void>;
  promqlQuery(request: PromqlQueryRequest, signal?: AbortSignal): Promise<PromqlInstantResult>;
  promqlQueryRange(request: PromqlRangeRequest, signal?: AbortSignal): Promise<PromqlRangeResult>;
  promqlLabels(request: PromqlMetadataRequest, signal?: AbortSignal): Promise<PromqlMetadataResult>;
  promqlLabelValues(
    label: string,
    request: PromqlMetadataRequest,
    signal?: AbortSignal,
  ): Promise<PromqlMetadataResult>;
  schema(dataset: string, signal?: AbortSignal): Promise<string[]>;
  query(request: QueryRequest, signal?: AbortSignal): Promise<LogRecord[]>;
  listTraces(request: TraceListRequest, signal?: AbortSignal): Promise<TraceListResponse>;
  traceDetail(request: TraceDetailRequest, signal?: AbortSignal): Promise<TraceDetailResponse>;
  listLogViews(signal?: AbortSignal): Promise<SavedLogView[]>;
  createLogView(body: SavedLogView): Promise<SavedLogView>;
  updateLogView(id: string, body: SavedLogView): Promise<SavedLogView>;
  convertLogViewToSql(id: string): Promise<SavedLogView>;
  deleteLogView(id: string): Promise<void>;
  logContext(request: LogContextRequest, signal?: AbortSignal): Promise<LogContextResponse>;
  login(username: string, password: string, returnPath?: string): Promise<void>;
  logout(): Promise<void>;
  about(signal?: AbortSignal): Promise<About>;
  clusterNodes(signal?: AbortSignal): Promise<ClusterNode[]>;
  clusterMetrics(signal?: AbortSignal): Promise<ClusterMetrics[]>;
  listDashboards(signal?: AbortSignal): Promise<DashboardSummary[]>;
  getDashboard(id: string, signal?: AbortSignal): Promise<Dashboard>;
  createDashboard(body: DashboardRequest): Promise<Dashboard>;
  updateDashboard(id: string, body: DashboardRequest): Promise<Dashboard>;
  /** Bodyless partial PUT: the server skips tile validation and keeps the stored document. */
  setDashboardFavorite(id: string, isFavorite: boolean): Promise<Dashboard>;
  deleteDashboard(id: string, revision?: string): Promise<void>;
  listAlerts(signal?: AbortSignal): Promise<AlertSummary[]>;
  getAlert(id: string, signal?: AbortSignal): Promise<Alert>;
  createAlert(alert: AlertRequest): Promise<Alert>;
  updateAlert(id: string, alert: AlertRequest): Promise<Alert>;
  deleteAlert(id: string): Promise<void>;
  enableAlert(id: string): Promise<Alert>;
  disableAlert(id: string): Promise<Alert>;
  muteAlert(id: string, state: string): Promise<Alert>;
  evaluateAlert(id: string): Promise<Alert>;
  listAlertTags(signal?: AbortSignal): Promise<string[]>;
  listAlertTargets(signal?: AbortSignal): Promise<AlertTargetStatus[]>;
  getAlertTarget(id: string, signal?: AbortSignal): Promise<AlertTargetStatus>;
  createAlertTarget(target: AlertTargetRequest): Promise<AlertTarget>;
  updateAlertTarget(id: string, target: AlertTargetRequest): Promise<AlertTarget>;
  deleteAlertTarget(id: string): Promise<AlertTarget>;
  listUsers(signal?: AbortSignal): Promise<TeamUser[]>;
  createUser(username: string, roles: string[]): Promise<string>;
  deleteUser(id: string): Promise<void>;
  addUserRoles(id: string, roles: string[]): Promise<void>;
  removeUserRoles(id: string, roles: string[]): Promise<void>;
  resetPassword(id: string): Promise<string>;
  userRoleSources(id: string, signal?: AbortSignal): Promise<UserRoleSources>;
  listRoles(signal?: AbortSignal): Promise<Roles>;
  putRole(name: string, privileges: Privilege[]): Promise<void>;
  deleteRole(name: string): Promise<void>;
  defaultRole(signal?: AbortSignal): Promise<string | null>;
  setDefaultRole(name: string): Promise<void>;
  clearDefaultRole(): Promise<void>;
  listApiKeys(signal?: AbortSignal): Promise<ApiKey[]>;
  createApiKey(name: string, roles: string[], ttlSeconds?: number | null): Promise<ApiKey>;
  deleteApiKey(id: string): Promise<void>;
}
