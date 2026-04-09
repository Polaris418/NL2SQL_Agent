import axios from 'axios';
import type {
  Connection,
  ConnectionConfig,
  ConnectionTestResult,
  SchemaCacheEntry,
  ColumnInfo,
  TableSchema,
} from '../types/connection';
import type {
  AgentStep,
  ExportRequest,
  PaginationInfo,
  QueryContext,
  QueryHistoryDetail,
  QueryHistoryItem,
  QueryRequest,
  QueryResult,
  QueryTelemetry,
  SQLExecutionRequest,
} from '../types/query';
import type {
  AnalyticsReport,
  AnalyticsSummary,
  ErrorDistribution,
  RecentQuery,
  TopTable,
} from '../types/analytics';
import type {
  ConnectionRAGStatus,
  RAGIndexBuildRequest,
  RAGIndexHealthDetail,
  RAGIndexJob,
  RAGIndexState,
  RAGTelemetryDashboard,
} from '../types/rag';
import type { LLMProfileUpsert, LLMRoutingUpdate, LLMSettings, LLMTestResult } from '../types/llm';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: 30000,
});

const toIsoString = (value: unknown, fallback = new Date().toISOString()): string => {
  if (!value) {
    return fallback;
  }
  if (typeof value === 'string') {
    return value;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return String(value);
};

const extractMessage = (detail: unknown, fallback: string): string => {
  if (typeof detail === 'string') {
    return detail;
  }
  if (detail && typeof detail === 'object') {
    const candidate = detail as Record<string, unknown>;
    if (typeof candidate.message === 'string') {
      return candidate.message;
    }
    if (typeof candidate.detail === 'string') {
      return candidate.detail;
    }
  }
  return fallback;
};

export const normalizeColumnInfo = (column: any): ColumnInfo => ({
  name: column?.name ?? column?.column_name ?? '',
  type: column?.type ?? column?.data_type ?? '',
  nullable: column?.nullable ?? true,
  default: column?.default ?? null,
  comment: column?.comment ?? undefined,
  is_primary_key: column?.is_primary_key ?? column?.primary_key ?? false,
  is_foreign_key: column?.is_foreign_key ?? Boolean(column?.foreign_key),
  primary_key: column?.primary_key ?? column?.is_primary_key ?? false,
  foreign_key: column?.foreign_key ?? column?.foreign_key_reference ?? undefined,
});

export const normalizeTableSchema = (table: any): TableSchema => ({
  name: table?.name ?? table?.table_name ?? '',
  comment: table?.comment ?? table?.table_comment ?? undefined,
  columns: Array.isArray(table?.columns) ? table.columns.map(normalizeColumnInfo) : [],
  description: table?.description ?? undefined,
  updated_at: toIsoString(table?.updated_at),
  table_name: table?.table_name ?? table?.name ?? '',
  table_comment: table?.table_comment ?? table?.comment ?? undefined,
});

const normalizePagination = (pagination: any, fallbackResult: any = {}): PaginationInfo | undefined => {
  if (!pagination && !fallbackResult?.page_number && !fallbackResult?.page_size) {
    return undefined;
  }

  return {
    page_number: pagination?.page_number ?? fallbackResult?.page_number ?? 1,
    page_size: pagination?.page_size ?? fallbackResult?.page_size ?? 1000,
    offset: pagination?.offset ?? fallbackResult?.offset ?? 0,
    has_more: pagination?.has_more ?? fallbackResult?.has_more ?? false,
    total_row_count: pagination?.total_row_count ?? fallbackResult?.total_row_count ?? null,
    applied_limit: pagination?.applied_limit ?? fallbackResult?.applied_limit ?? null,
    total_count:
      pagination?.total_count ?? pagination?.total_row_count ?? fallbackResult?.total_row_count ?? null,
  };
};

export const normalizeExecutionResult = (result: any): QueryResult['result'] => {
  if (!result) {
    return null;
  }

  return {
    columns: Array.isArray(result.columns) ? result.columns.map(String) : [],
    rows: Array.isArray(result.rows)
      ? result.rows.map((row: any) => (row && typeof row === 'object' && !Array.isArray(row) ? row : {}))
      : [],
    row_count: result.row_count ?? 0,
    truncated: result.truncated ?? false,
    db_latency_ms: result.db_latency_ms ?? result.execution_time_ms ?? null,
    total_row_count: result.total_row_count ?? null,
    pagination: normalizePagination(result.pagination, result),
    source_sql: result.source_sql ?? result.sql ?? undefined,
    execution_time_ms: result.execution_time_ms ?? result.db_latency_ms ?? undefined,
  };
};

export const normalizeChartSuggestion = (chart: any): QueryResult['chart'] => {
  if (!chart) {
    return null;
  }

  return {
    chart_type: chart.chart_type ?? chart.type ?? 'table',
    x_axis: chart.x_axis ?? chart.xAxis ?? undefined,
    y_axis: chart.y_axis ?? chart.yAxis ?? undefined,
    reason: chart.reason ?? undefined,
  };
};

export const normalizeAgentStep = (step: any): AgentStep => ({
  step_type: step?.step_type ?? step?.type ?? 'rewrite',
  content: String(step?.content ?? ''),
  timestamp: toIsoString(step?.timestamp),
  metadata: step?.metadata ?? {},
  status: step?.status ?? (step?.step_type === 'done' ? 'success' : undefined),
  error_type: step?.error_type ?? undefined,
  error_suggestion: step?.error_suggestion ?? undefined,
  retry_count: step?.retry_count ?? undefined,
  max_retries: step?.max_retries ?? undefined,
});

export const normalizeQueryResult = (payload: any): QueryResult => {
  const result = normalizeExecutionResult(payload?.result ?? payload?.results ?? null);
  const chart = normalizeChartSuggestion(payload?.chart ?? payload?.chart_suggestion ?? null);
  const steps = Array.isArray(payload?.steps)
    ? payload.steps.map(normalizeAgentStep)
    : Array.isArray(payload?.agent_steps)
      ? payload.agent_steps.map(normalizeAgentStep)
      : [];
  const retryCount = payload?.retry_count ?? payload?.sql_attempts ?? 0;
  const llmLatency = payload?.llm_latency_ms ?? payload?.latency?.llm_ms ?? null;
  const dbLatency = payload?.db_latency_ms ?? payload?.latency?.db_ms ?? result?.db_latency_ms ?? null;

  return {
    question: payload?.question ?? '',
    rewritten_query: payload?.rewritten_query ?? payload?.question ?? '',
    retrieved_tables: Array.isArray(payload?.retrieved_tables)
      ? payload.retrieved_tables.map(normalizeTableSchema)
      : [],
    sql: payload?.sql ?? payload?.generated_sql ?? '',
    result,
    chart,
    steps,
    status: payload?.status ?? (result ? 'success' : 'failed'),
    retry_count: retryCount,
    llm_latency_ms: llmLatency,
    db_latency_ms: dbLatency,
    error_message: payload?.error_message ?? undefined,
    error_type: payload?.error_type ?? undefined,
    error_suggestion: payload?.error_suggestion ?? undefined,
    context_source_query_id: payload?.context_source_query_id ?? undefined,
    telemetry: payload?.telemetry ?? undefined,
    query_id: payload?.query_id ?? payload?.id ?? undefined,
    sql_attempts: payload?.sql_attempts ?? retryCount,
    results: result,
    chart_suggestion: chart,
    latency: {
      llm_ms: llmLatency ?? 0,
      db_ms: dbLatency ?? 0,
    },
  };
};

const normalizeQueryTelemetry = (payload: any): QueryTelemetry | undefined => {
  if (!payload || typeof payload !== 'object') {
    return undefined;
  }
  return {
    cache_hit: Boolean(payload.cache_hit),
    cache_key: payload.cache_key ?? undefined,
    queue_wait_ms: payload.queue_wait_ms ?? undefined,
    retrieval_latency_ms: payload.retrieval_latency_ms ?? undefined,
    active_concurrency: payload.active_concurrency ?? undefined,
    peak_concurrency: payload.peak_concurrency ?? undefined,
    schema_fingerprint: payload.schema_fingerprint ?? undefined,
    schema_table_count: payload.schema_table_count ?? undefined,
    llm_cache_hit: payload.llm_cache_hit ?? undefined,
    retrieval_backend: payload.retrieval_backend ?? undefined,
    embedding_backend: payload.embedding_backend ?? undefined,
    retrieval_candidates: payload.retrieval_candidates ?? undefined,
    retrieval_selected: payload.retrieval_selected ?? undefined,
    retrieval_top_score: payload.retrieval_top_score ?? undefined,
    relationship_count: payload.relationship_count ?? undefined,
    relationship_tables: Array.isArray(payload.relationship_tables) ? payload.relationship_tables : [],
    column_annotation_count: payload.column_annotation_count ?? undefined,
    packed_context_tables: payload.packed_context_tables ?? undefined,
    packed_context_chars: payload.packed_context_chars ?? undefined,
    packed_context_tokens: payload.packed_context_tokens ?? undefined,
    packed_context_truncated: payload.packed_context_truncated ?? undefined,
    packed_context_budget: payload.packed_context_budget ?? undefined,
    packed_context_limit_reason: payload.packed_context_limit_reason ?? undefined,
    packed_context_dropped_tables: Array.isArray(payload.packed_context_dropped_tables) ? payload.packed_context_dropped_tables : [],
    packed_context_dropped_columns: payload.packed_context_dropped_columns ?? {},
    packed_context_dropped_relationship_clues: Array.isArray(payload.packed_context_dropped_relationship_clues)
      ? payload.packed_context_dropped_relationship_clues
      : [],
    few_shot_example_ids: Array.isArray(payload.few_shot_example_ids) ? payload.few_shot_example_ids : [],
    audit_id: payload.audit_id ?? undefined,
  };
};

export const normalizeHistoryItem = (item: any): QueryHistoryItem => ({
  id: item?.id ?? item?.query_id ?? '',
  connection_id: item?.connection_id ?? '',
  question: item?.question ?? item?.user_question ?? item?.natural_language_query ?? '',
  rewritten_query: item?.rewritten_query ?? undefined,
  retrieved_tables: Array.isArray(item?.retrieved_tables) ? item.retrieved_tables : [],
  sql: item?.sql ?? item?.final_sql ?? item?.generated_sql ?? undefined,
  status: item?.status ?? 'partial',
  retry_count: item?.retry_count ?? item?.sql_attempts ?? 0,
  error_message: item?.error_message ?? undefined,
  llm_latency_ms: item?.llm_latency_ms ?? null,
  db_latency_ms: item?.db_latency_ms ?? null,
  created_at: toIsoString(item?.created_at),
  error_type: item?.error_type ?? undefined,
  error_suggestion: item?.error_suggestion ?? undefined,
  result_row_count: item?.result_row_count ?? item?.row_count ?? undefined,
  context_source_query_id: item?.context_source_query_id ?? undefined,
  user_question: item?.user_question ?? item?.question ?? undefined,
  final_sql: item?.final_sql ?? item?.sql ?? undefined,
  sql_attempts: item?.sql_attempts ?? item?.retry_count ?? 0,
  row_count: item?.row_count ?? item?.result_row_count ?? undefined,
  execution_time_ms: item?.execution_time_ms ?? item?.db_latency_ms ?? undefined,
  query_id: item?.query_id ?? item?.id ?? undefined,
  natural_language_query: item?.natural_language_query ?? item?.question ?? undefined,
  generated_sql: item?.generated_sql ?? item?.sql ?? undefined,
});

export const normalizeHistoryDetail = (detail: any): QueryHistoryDetail => ({
  ...normalizeHistoryItem(detail),
  steps: Array.isArray(detail?.steps) ? detail.steps.map(normalizeAgentStep) : [],
  result_row_count: detail?.result_row_count ?? detail?.row_count ?? null,
  result: detail?.result ? normalizeExecutionResult(detail.result) : null,
  chart: detail?.chart ? normalizeChartSuggestion(detail.chart) : null,
  retrieved_table_details: Array.isArray(detail?.retrieved_table_details)
    ? detail.retrieved_table_details.map(normalizeTableSchema)
    : [],
  telemetry: normalizeQueryTelemetry(detail?.telemetry),
  follow_up_context: detail?.follow_up_context ?? undefined,
});

export const normalizeQueryContext = (context: any): QueryContext => ({
  history_id: context?.history_id ?? context?.query_id ?? '',
  question: context?.question ?? '',
  connection_id: context?.connection_id ?? '',
  rewritten_query: context?.rewritten_query ?? undefined,
  retrieved_tables: Array.isArray(context?.retrieved_tables) ? context.retrieved_tables : [],
  final_sql: context?.final_sql ?? context?.sql ?? undefined,
  steps: Array.isArray(context?.steps) ? context.steps.map(normalizeAgentStep) : [],
  context_text: context?.context_text ?? context?.results_preview ?? '',
  follow_up_context: context?.follow_up_context ?? context?.context_text ?? undefined,
  query_id: context?.query_id ?? context?.history_id ?? undefined,
  sql: context?.sql ?? context?.final_sql ?? undefined,
  results_preview: context?.results_preview ?? undefined,
});

const normalizeAnalyticsSummary = (summary: any): AnalyticsSummary => {
  const recentSuccessRate =
    typeof summary?.recent_success_rate === 'number'
      ? summary.recent_success_rate
      : typeof summary?.success_rate === 'number'
        ? summary.success_rate / 100
        : 0;
  const averageLlm = summary?.average_llm_latency_ms ?? summary?.avg_generation_time_ms ?? 0;
  const averageDb = summary?.average_db_latency_ms ?? summary?.avg_execution_time_ms ?? 0;
  const topTables = Array.isArray(summary?.top_tables)
    ? summary.top_tables.map((item: any): TopTable => ({
        table: item?.table ?? item?.table_name ?? '',
        count: item?.count ?? item?.query_count ?? 0,
        table_name: item?.table_name ?? item?.table ?? '',
        query_count: item?.query_count ?? item?.count ?? 0,
        success_rate: item?.success_rate ?? undefined,
      }))
    : [];
  const errorDistribution = Array.isArray(summary?.error_distribution)
    ? summary.error_distribution.map((item: any): ErrorDistribution => ({
        type: item?.type ?? item?.error_type ?? '',
        error_type: item?.error_type ?? item?.type ?? '',
        count: item?.count ?? 0,
        percentage: item?.percentage ?? 0,
        recent_example: item?.recent_example ?? undefined,
      }))
    : [];

  const totalQueries = summary?.total_queries ?? 0;
  const successfulQueries =
    summary?.successful_queries ?? Math.round(totalQueries * recentSuccessRate);
  const failedQueries = summary?.failed_queries ?? Math.max(totalQueries - successfulQueries, 0);

  return {
    recent_success_rate: recentSuccessRate,
    average_llm_latency_ms: averageLlm,
    average_db_latency_ms: averageDb,
    top_tables: topTables,
    error_distribution: errorDistribution,
    total_queries: totalQueries,
    success_rate: summary?.success_rate ?? recentSuccessRate * 100,
    avg_execution_time_ms: summary?.avg_execution_time_ms ?? averageDb,
    avg_generation_time_ms: summary?.avg_generation_time_ms ?? averageLlm,
    successful_queries: successfulQueries,
    failed_queries: failedQueries,
    avg_sql_attempts: summary?.avg_sql_attempts ?? 0,
  };
};

export const normalizeAnalyticsReport = (report: any): AnalyticsReport => {
  const summary = normalizeAnalyticsSummary(report?.summary ?? {});
  const errors = Array.isArray(report?.errors)
    ? report.errors.map((item: any): ErrorDistribution => ({
        type: item?.type ?? item?.error_type ?? '',
        error_type: item?.error_type ?? item?.type ?? '',
        count: item?.count ?? 0,
        percentage: item?.percentage ?? 0,
        recent_example: item?.recent_example ?? undefined,
      }))
    : [];
  const topTables = Array.isArray(report?.top_tables)
    ? report.top_tables.map((item: any): TopTable => ({
        table: item?.table ?? item?.table_name ?? '',
        count: item?.count ?? item?.query_count ?? 0,
        table_name: item?.table_name ?? item?.table ?? '',
        query_count: item?.query_count ?? item?.count ?? 0,
        success_rate: item?.success_rate ?? undefined,
      }))
    : [];

  return {
    summary: {
      ...summary,
      top_tables: topTables,
      error_distribution: errors,
    },
    errors,
    top_tables: topTables,
    recent_queries: Array.isArray(report?.recent_queries)
      ? report.recent_queries.map((item: any): RecentQuery => ({
          query_id: item?.query_id ?? '',
          question: item?.question ?? item?.natural_language_query ?? '',
          status: item?.status ?? '',
          created_at: toIsoString(item?.created_at),
          execution_time_ms: item?.execution_time_ms ?? undefined,
          natural_language_query: item?.natural_language_query ?? item?.question ?? undefined,
        }))
      : [],
    time_range: report?.time_range ?? undefined,
    error_distribution: errors,
    top_queried_tables: topTables,
  };
};

export const normalizeRAGIndexState = (state: any): RAGIndexState => ({
  connection_id: state?.connection_id ?? '',
  schema_version: state?.schema_version ?? null,
  index_status: state?.index_status ?? 'pending',
  health_status: state?.health_status ?? 'unknown',
  index_mode: state?.index_mode ?? 'hybrid',
  is_indexed: state?.is_indexed ?? false,
  table_count: state?.table_count ?? 0,
  vector_count: state?.vector_count ?? 0,
  last_started_at: state?.last_started_at ?? null,
  last_completed_at: state?.last_completed_at ?? null,
  last_success_at: state?.last_success_at ?? null,
  last_error: state?.last_error ?? null,
  last_forced_rebuild: state?.last_forced_rebuild ?? false,
  updated_at: state?.updated_at ? toIsoString(state.updated_at) : undefined,
  created_at: state?.created_at ? toIsoString(state.created_at) : undefined,
});

export const normalizeRAGIndexJob = (job: any): RAGIndexJob => ({
  id: job?.id ?? '',
  connection_id: job?.connection_id ?? '',
  schema_version: job?.schema_version ?? null,
  action: job?.action ?? 'rebuild',
  status: job?.status ?? 'pending',
  force_full_rebuild: job?.force_full_rebuild ?? false,
  started_at: toIsoString(job?.started_at),
  completed_at: job?.completed_at ?? null,
  duration_ms: job?.duration_ms ?? null,
  error_message: job?.error_message ?? null,
  payload: job?.payload && typeof job.payload === 'object' ? job.payload : {},
});

export const normalizeRAGIndexHealthDetail = (detail: any): RAGIndexHealthDetail => ({
  state: normalizeRAGIndexState(detail?.state ?? {}),
  current_job: detail?.current_job ? normalizeRAGIndexJob(detail.current_job) : null,
  latest_jobs: Array.isArray(detail?.latest_jobs) ? detail.latest_jobs.map(normalizeRAGIndexJob) : [],
  vector_store_available: detail?.vector_store_available ?? false,
  bm25_enabled: detail?.bm25_enabled ?? false,
  degradation: detail?.degradation && typeof detail.degradation === 'object' ? detail.degradation : {},
  async_snapshot: detail?.async_snapshot && typeof detail.async_snapshot === 'object' ? detail.async_snapshot : {},
});

export const normalizeRAGTelemetryDashboard = (dashboard: any): RAGTelemetryDashboard => ({
  current: dashboard?.current && typeof dashboard.current === 'object' ? dashboard.current : {},
  latest_snapshot: dashboard?.latest_snapshot
    ? {
        id: dashboard.latest_snapshot?.id ?? null,
        logged_queries: dashboard.latest_snapshot?.logged_queries ?? 0,
        metrics:
          dashboard.latest_snapshot?.metrics && typeof dashboard.latest_snapshot.metrics === 'object'
            ? dashboard.latest_snapshot.metrics
            : {},
        created_at: dashboard.latest_snapshot?.created_at ?? null,
      }
    : null,
  recent_events: Array.isArray(dashboard?.recent_events) ? dashboard.recent_events : [],
  history_count: dashboard?.history_count ?? 0,
  store: dashboard?.store && typeof dashboard.store === 'object' ? dashboard.store : {},
  context_limit_events: Array.isArray(dashboard?.context_limit_events) ? dashboard.context_limit_events : [],
});

export const normalizeConnectionRAGStatus = (source: any): ConnectionRAGStatus => {
  const stateSource = source?.rag_index_state ?? source?.rag_state ?? source;
  const normalizedState =
    stateSource && typeof stateSource === 'object' && (stateSource.connection_id || stateSource.index_status)
      ? normalizeRAGIndexState(stateSource)
      : null;
  const detailSource = source?.rag_health_detail ?? source?.rag_detail;
  const normalizedDetail = detailSource ? normalizeRAGIndexHealthDetail(detailSource) : null;

  return {
    rag_index_state: normalizedState,
    rag_health_detail: normalizedDetail,
    rag_index_status: normalizedState?.index_status ?? source?.rag_index_status ?? source?.index_status,
    rag_health_status: normalizedState?.health_status ?? source?.rag_health_status ?? source?.health_status,
    rag_index_mode: normalizedState?.index_mode ?? source?.rag_index_mode ?? source?.index_mode,
    rag_schema_version: normalizedState?.schema_version ?? source?.rag_schema_version ?? source?.schema_version ?? null,
    rag_is_indexed: normalizedState?.is_indexed ?? source?.rag_is_indexed ?? source?.is_indexed,
    rag_table_count: normalizedState?.table_count ?? source?.rag_table_count ?? source?.table_count,
    rag_vector_count: normalizedState?.vector_count ?? source?.rag_vector_count ?? source?.vector_count,
    rag_last_started_at: normalizedState?.last_started_at ?? source?.rag_last_started_at ?? source?.last_started_at ?? null,
    rag_last_completed_at: normalizedState?.last_completed_at ?? source?.rag_last_completed_at ?? source?.last_completed_at ?? null,
    rag_last_success_at: normalizedState?.last_success_at ?? source?.rag_last_success_at ?? source?.last_success_at ?? null,
    rag_last_error: normalizedState?.last_error ?? source?.rag_last_error ?? source?.last_error ?? null,
    rag_last_forced_rebuild: normalizedState?.last_forced_rebuild ?? source?.rag_last_forced_rebuild ?? source?.last_forced_rebuild,
    rag_updated_at: normalizedState?.updated_at ?? source?.rag_updated_at ?? source?.updated_at,
    rag_created_at: normalizedState?.created_at ?? source?.rag_created_at ?? source?.created_at,
  };
};

apiClient.interceptors.request.use(
  (config) => config,
  (error) => Promise.reject(error)
);

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    const detail = error.response?.data?.detail;
    const message = extractMessage(detail ?? error.response?.data, error.message || '请求失败');
    return Promise.reject(new Error(message));
  }
);

export const connectionsApi = {
  list: async (): Promise<Connection[]> => {
    const response = await apiClient.get<Connection[]>('/api/connections');
    return response.data.map((connection: any) => ({
      ...connection,
      ...normalizeConnectionRAGStatus(connection),
    }));
  },

  create: async (config: ConnectionConfig): Promise<Connection> => {
    const response = await apiClient.post<Connection>('/api/connections', config);
    return {
      ...response.data,
      ...normalizeConnectionRAGStatus(response.data),
    };
  },

  test: async (id: string): Promise<ConnectionTestResult> => {
    const response = await apiClient.post<ConnectionTestResult>(`/api/connections/${id}/test`);
    return response.data;
  },

  delete: async (id: string): Promise<void> => {
    await apiClient.delete(`/api/connections/${id}`);
  },

  sync: async (id: string): Promise<void> => {
    await apiClient.post(`/api/connections/${id}/sync`);
  },

  getSchema: async (id: string): Promise<SchemaCacheEntry> => {
    const response = await apiClient.get<SchemaCacheEntry>(`/api/connections/${id}/schema`);
    return {
      ...response.data,
      tables: Array.isArray(response.data.tables) ? response.data.tables.map(normalizeTableSchema) : [],
    };
  },
};

export const ragApi = {
  listIndexStatus: async (): Promise<RAGIndexState[]> => {
    const response = await apiClient.get<RAGIndexState[]>('/api/rag/index/status');
    return Array.isArray(response.data) ? response.data.map(normalizeRAGIndexState) : [];
  },

  listIndexStates: async (): Promise<RAGIndexState[]> => {
    const response = await apiClient.get<RAGIndexState[]>('/api/rag/index/status');
    return Array.isArray(response.data) ? response.data.map(normalizeRAGIndexState) : [];
  },

  getIndexStatus: async (connectionId: string): Promise<RAGIndexState> => {
    const response = await apiClient.get<RAGIndexState>(`/api/rag/index/status/${connectionId}`);
    return normalizeRAGIndexState(response.data);
  },

  getIndexState: async (connectionId: string): Promise<RAGIndexState> => {
    const response = await apiClient.get<RAGIndexState>(`/api/rag/index/status/${connectionId}`);
    return normalizeRAGIndexState(response.data);
  },

  getIndexHealth: async (connectionId: string): Promise<RAGIndexHealthDetail> => {
    const response = await apiClient.get<RAGIndexHealthDetail>(`/api/rag/index/health/${connectionId}`);
    return normalizeRAGIndexHealthDetail(response.data);
  },

  rebuildIndex: async (connectionId: string, payloadOrForce: RAGIndexBuildRequest | boolean = false): Promise<RAGIndexState> => {
    const forceFullRebuild = typeof payloadOrForce === 'boolean' ? payloadOrForce : Boolean(payloadOrForce?.force_full_rebuild);
    const response = await apiClient.post<RAGIndexState>(`/api/rag/index/${connectionId}/rebuild`, {
      force_full_rebuild: forceFullRebuild,
    });
    return normalizeRAGIndexState(response.data);
  },

  telemetryDashboard: async (): Promise<RAGTelemetryDashboard> => {
    const response = await apiClient.get<RAGTelemetryDashboard>('/api/rag/telemetry/dashboard');
    return normalizeRAGTelemetryDashboard(response.data);
  },
};

export const queryApi = {
  execute: async (request: QueryRequest): Promise<QueryResult> => {
    const response = await apiClient.post<QueryResult>('/api/query', request);
    return normalizeQueryResult(response.data);
  },

  executeSQL: async (request: SQLExecutionRequest): Promise<QueryResult> => {
    const response = await apiClient.post<QueryResult>('/api/query/sql', request);
    return normalizeQueryResult(response.data);
  },

  streamURL: (): string => `${API_BASE_URL}/api/query/stream`,

  exportQuery: async (request: ExportRequest): Promise<Blob> => {
    const response = await apiClient.post('/api/query/export', request, {
      responseType: 'blob',
    });
    return response.data;
  },

  exportSQL: async (request: ExportRequest): Promise<Blob> => {
    const response = await apiClient.post('/api/query/sql/export', request, {
      responseType: 'blob',
    });
    return response.data;
  },
};

export const historyApi = {
  list: async (params?: {
    limit?: number;
    offset?: number;
    connection_id?: string;
  }): Promise<QueryHistoryItem[]> => {
    const response = await apiClient.get<QueryHistoryItem[]>('/api/history', { params });
    return response.data.map(normalizeHistoryItem);
  },

  get: async (id: string): Promise<QueryHistoryDetail> => {
    const response = await apiClient.get<QueryHistoryDetail>(`/api/history/${id}`);
    return normalizeHistoryDetail(response.data);
  },

  getContext: async (id: string): Promise<QueryContext> => {
    const response = await apiClient.get<QueryContext>(`/api/history/${id}/context`);
    return normalizeQueryContext(response.data);
  },

  delete: async (id: string): Promise<void> => {
    await apiClient.delete(`/api/history/${id}`);
  },

  deleteAll: async (): Promise<{ deleted_count: number }> => {
    const response = await apiClient.delete<{ deleted_count: number }>('/api/history');
    return response.data;
  },
};

export const analyticsApi = {
  summary: async (): Promise<AnalyticsSummary> => {
    const response = await apiClient.get<AnalyticsSummary>('/api/analytics/summary');
    return normalizeAnalyticsSummary(response.data);
  },

  errors: async (): Promise<ErrorDistribution[]> => {
    const response = await apiClient.get<ErrorDistribution[]>('/api/analytics/errors');
    return Array.isArray(response.data)
      ? response.data.map((item: any): ErrorDistribution => ({
          type: item?.type ?? item?.error_type ?? '',
          error_type: item?.error_type ?? item?.type ?? '',
          count: item?.count ?? 0,
          percentage: item?.percentage ?? 0,
          recent_example: item?.recent_example ?? undefined,
        }))
      : [];
  },

  topTables: async (): Promise<TopTable[]> => {
    const response = await apiClient.get<TopTable[]>('/api/analytics/top-tables');
    return Array.isArray(response.data)
      ? response.data.map((item: any): TopTable => ({
          table: item?.table ?? item?.table_name ?? '',
          count: item?.count ?? item?.query_count ?? 0,
          table_name: item?.table_name ?? item?.table ?? '',
          query_count: item?.query_count ?? item?.count ?? 0,
          success_rate: item?.success_rate ?? undefined,
        }))
      : [];
  },

  report: async (params?: { days?: number }): Promise<AnalyticsReport> => {
    const response = await apiClient.get<AnalyticsReport>('/api/analytics/report', { params });
    return normalizeAnalyticsReport(response.data);
  },
};

export const llmSettingsApi = {
  async get(): Promise<LLMSettings> {
    const response = await apiClient.get('/api/settings/llm');
    return response.data as LLMSettings;
  },

  async upsertProfile(payload: LLMProfileUpsert): Promise<LLMSettings> {
    const response = await apiClient.post('/api/settings/llm/profiles', payload);
    return response.data as LLMSettings;
  },

  async updateRouting(payload: LLMRoutingUpdate): Promise<LLMSettings> {
    const response = await apiClient.put('/api/settings/llm/routing', payload);
    return response.data as LLMSettings;
  },

  async removeProfile(profileId: string): Promise<LLMSettings> {
    const response = await apiClient.delete(`/api/settings/llm/profiles/${profileId}`);
    return response.data as LLMSettings;
  },

  async test(payload: LLMProfileUpsert): Promise<LLMTestResult> {
    const response = await apiClient.post('/api/settings/llm/test', payload);
    return response.data as LLMTestResult;
  },
};

export default apiClient;

export interface PromptTemplate {
  name: string;
  description: string;
  template: string;
  variables: string[];
}

export interface PromptUpdateRequest {
  template: string;
}

export const promptsApi = {
  list: async (): Promise<PromptTemplate[]> => {
    const response = await apiClient.get<PromptTemplate[]>('/api/prompts');
    return response.data;
  },

  get: async (name: string): Promise<PromptTemplate> => {
    const response = await apiClient.get<PromptTemplate>(`/api/prompts/${name}`);
    return response.data;
  },

  update: async (name: string, request: PromptUpdateRequest): Promise<{ message: string; note: string }> => {
    const response = await apiClient.put<{ message: string; note: string }>(`/api/prompts/${name}`, request);
    return response.data;
  },

  reset: async (name: string): Promise<{ message: string }> => {
    const response = await apiClient.post<{ message: string }>(`/api/prompts/${name}/reset`);
    return response.data;
  },
};

// AI Assistant API
export interface AssistantConfigPayload {
  enabled: boolean;
  provider: string;
  api_key: string;
  api_base: string;
  model: string;
  temperature: number;
  max_tokens: number;
  system_prompt: string;
}

export interface AssistantConfigTestResult {
  success: boolean;
  provider: string;
  model: string;
  latency_ms: number;
  message: string;
}

type SseFrame = {
  event: string;
  data: string;
};

const parseSseFrame = (frame: string): SseFrame | null => {
  const lines = frame.split(/\r?\n/);
  let event = 'message';
  const dataParts: string[] = [];

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    if (!line || line.startsWith(':')) {
      continue;
    }
    if (line.startsWith('event:')) {
      event = line.slice(6).trim() || event;
      continue;
    }
    if (line.startsWith('data:')) {
      dataParts.push(line.slice(5).trimStart());
    }
  }

  if (dataParts.length === 0) {
    return null;
  }

  return {
    event,
    data: dataParts.join('\n'),
  };
};

export const assistantApi = {
  chat: async (request: {
    message: string;
    history: Array<{ role: string; content: string }>;
  }): Promise<{ message: string; sources: string[] }> => {
    const response = await apiClient.post('/api/assistant/chat', request);
    return response.data;
  },

  streamChat: async (
    request: {
      message: string;
      history: Array<{ role: string; content: string }>;
    },
    handlers: {
      onChunk: (delta: string) => void;
      onDone: (payload: { message: string; sources: string[] }) => void;
      onError?: (message: string) => void;
    }
  ): Promise<void> => {
    const response = await fetch(`${API_BASE_URL}/api/assistant/chat/stream`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'text/event-stream',
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('Response body is not readable');
    }

    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      while (buffer.includes('\n\n')) {
        const splitIndex = buffer.indexOf('\n\n');
        const frameText = buffer.slice(0, splitIndex);
        buffer = buffer.slice(splitIndex + 2);

        const frame = parseSseFrame(frameText);
        if (!frame) {
          continue;
        }

        const payload = JSON.parse(frame.data);
        if (frame.event === 'chunk') {
          handlers.onChunk(String(payload?.delta ?? ''));
          continue;
        }
        if (frame.event === 'done') {
          handlers.onDone({
            message: String(payload?.message ?? ''),
            sources: Array.isArray(payload?.sources) ? payload.sources.map(String) : [],
          });
          continue;
        }
        if (frame.event === 'error') {
          const message = String(payload?.message ?? '助手服务错误');
          handlers.onError?.(message);
          throw new Error(message);
        }
      }
    }
  },

  getKnowledge: async (): Promise<any[]> => {
    const response = await apiClient.get('/api/assistant/knowledge');
    return response.data;
  },

  getKnowledgeItem: async (id: string): Promise<any> => {
    const response = await apiClient.get(`/api/assistant/knowledge/${id}`);
    return response.data;
  },

  updateKnowledgeItem: async (id: string, item: any): Promise<any> => {
    const response = await apiClient.put(`/api/assistant/knowledge/${id}`, item);
    return response.data;
  },

  createKnowledgeItem: async (item: any): Promise<any> => {
    const response = await apiClient.post('/api/assistant/knowledge', item);
    return response.data;
  },

  deleteKnowledgeItem: async (id: string): Promise<void> => {
    await apiClient.delete(`/api/assistant/knowledge/${id}`);
  },

  getConfig: async (): Promise<AssistantConfigPayload> => {
    const response = await apiClient.get<AssistantConfigPayload>('/api/assistant/config');
    return response.data;
  },

  updateConfig: async (config: AssistantConfigPayload): Promise<AssistantConfigPayload> => {
    const response = await apiClient.put<AssistantConfigPayload>('/api/assistant/config', config);
    return response.data;
  },

  testConfig: async (config: AssistantConfigPayload): Promise<AssistantConfigTestResult> => {
    const response = await apiClient.post<AssistantConfigTestResult>('/api/assistant/config/test', {
      provider: config.provider,
      api_key: config.api_key,
      api_base: config.api_base,
      model: config.model,
    });
    return response.data;
  },
};

// 文档管理 API
export const documentsApi = {
  upload: async (file: File, metadata: { title?: string; category?: string; description?: string }): Promise<any> => {
    const formData = new FormData();
    formData.append('file', file);
    if (metadata.title) formData.append('title', metadata.title);
    if (metadata.category) formData.append('category', metadata.category);
    if (metadata.description) formData.append('description', metadata.description);
    
    const response = await apiClient.post('/api/documents/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },

  list: async (): Promise<any[]> => {
    const response = await apiClient.get('/api/documents/list');
    return response.data;
  },

  get: async (documentId: string): Promise<any> => {
    const response = await apiClient.get(`/api/documents/${documentId}`);
    return response.data;
  },

  delete: async (documentId: string): Promise<void> => {
    await apiClient.delete(`/api/documents/${documentId}`);
  },

  search: async (query: string, topK: number = 5): Promise<any> => {
    const response = await apiClient.post('/api/documents/search', { query, top_k: topK });
    return response.data;
  },

  getStats: async (): Promise<any> => {
    const response = await apiClient.get('/api/documents/stats');
    return response.data;
  },
};
