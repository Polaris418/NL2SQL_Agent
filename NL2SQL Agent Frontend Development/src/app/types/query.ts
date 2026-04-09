import type { TableSchema } from './connection';

export interface QueryRequest {
  connection_id: string;
  question: string;
  context_history?: string[];
  stream?: boolean;
  // 分页参数
  page_number?: number;
  page_size?: number;
  // Follow-up 支持
  previous_query_id?: string;
  follow_up_instruction?: string;
  // 其他选项
  include_total_count?: boolean;
}

// SQL 执行请求
export interface SQLExecutionRequest {
  connection_id: string;
  sql: string;
  page_number?: number;
  page_size?: number;
  include_total_count?: boolean;
}

// 导出请求
export interface ExportRequest {
  query_id?: string;
  connection_id?: string;
  sql?: string;
  format?: 'csv' | 'json' | 'excel';
}

export interface ExecutionResult {
  columns: string[];
  rows: Record<string, any>[]; // 修正为对象数组
  row_count: number;
  truncated?: boolean;
  db_latency_ms?: number;
  total_row_count?: number;
  pagination?: PaginationInfo;
  source_sql?: string;
  // legacy alias
  execution_time_ms?: number;
}

export interface ChartSuggestion {
  chart_type: 'bar' | 'line' | 'pie' | 'table';
  x_axis?: string;
  y_axis?: string;
  reason?: string;
}

export type StepType = 
  | 'rewrite'
  | 'schema_retrieval'
  | 'sql_generation'
  | 'sql_execution'
  | 'error_reflection'
  | 'retry'
  | 'done';

export interface PaginationInfo {
  page_number: number;
  page_size: number;
  offset?: number;
  has_more?: boolean;
  total_row_count?: number | null;
  applied_limit?: number | null;
  total_count?: number | null;
}

export interface AgentStep {
  step_type: StepType;
  content: string;
  timestamp: string;
  metadata?: Record<string, any>;
  status?: 'loading' | 'success' | 'error';
  error_type?: string;
  error_suggestion?: string;
  retry_count?: number;
  max_retries?: number;
}

export interface QueryResult {
  question: string;
  rewritten_query: string;
  retrieved_tables: TableSchema[];
  sql: string;
  result?: ExecutionResult | null;
  chart?: ChartSuggestion | null;
  summary?: string | null;
  steps: AgentStep[];
  status: 'success' | 'failed' | 'partial';
  retry_count: number;
  llm_latency_ms?: number | null;
  db_latency_ms?: number | null;
  error_message?: string;
  error_type?: string;
  error_suggestion?: string;
  context_source_query_id?: string;
  telemetry?: QueryTelemetry;
  // legacy aliases
  query_id?: string;
  sql_attempts?: number;
  results?: ExecutionResult | null;
  chart_suggestion?: ChartSuggestion | null;
  latency?: {
    llm_ms: number;
    db_ms: number;
  };
}

export type QueryStatus = QueryResult['status'];

export interface QueryTelemetry {
  cache_hit?: boolean;
  cache_key?: string | null;
  queue_wait_ms?: number | null;
  retrieval_latency_ms?: number | null;
  active_concurrency?: number | null;
  peak_concurrency?: number | null;
  schema_fingerprint?: string | null;
  schema_table_count?: number | null;
  llm_cache_hit?: boolean;
  retrieval_backend?: string | null;
  embedding_backend?: string | null;
  retrieval_scope_count?: number | null;
  retrieval_candidates?: number | null;
  retrieval_selected?: number | null;
  retrieval_lexical_count?: number | null;
  retrieval_vector_count?: number | null;
  retrieval_top_score?: number | null;
  relationship_count?: number | null;
  relationship_tables?: string[];
  column_annotation_count?: number | null;
  packed_context_tables?: number | null;
  packed_context_chars?: number | null;
  packed_context_tokens?: number | null;
  packed_context_truncated?: boolean;
  packed_context_budget?: Record<string, any>;
  packed_context_limit_reason?: string | null;
  packed_context_dropped_tables?: string[];
  packed_context_dropped_columns?: Record<string, string[]>;
  packed_context_dropped_relationship_clues?: Array<Record<string, any>>;
  few_shot_example_ids?: string[];
  audit_id?: string | null;
}

export interface Message {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  sql?: string;
  generated_sql?: string;
  results?: ExecutionResult;
  execution_result?: ExecutionResult;
  chart_suggestion?: ChartSuggestion;
  chart?: ChartSuggestion;
  summary?: string;
  retrieved_tables?: TableSchema[];
  telemetry?: QueryTelemetry;
  steps?: AgentStep[];
  agent_steps?: AgentStep[];
  timestamp: Date;
  status?: 'loading' | 'success' | 'error';
  error_message?: string;
  error_type?: string;
  error_suggestion?: string;
  query_id?: string; // 关联的查询ID
  context_source_query_id?: string;
  retry_count?: number;
}

export interface QueryHistoryItem {
  id: string;
  connection_id: string;
  question: string;
  rewritten_query?: string;
  retrieved_tables?: string[];
  sql?: string;
  status: QueryResult['status'];
  retry_count?: number;
  error_message?: string;
  llm_latency_ms?: number;
  db_latency_ms?: number;
  created_at: string;
  error_type?: string;
  error_suggestion?: string;
  result_row_count?: number;
  context_source_query_id?: string;
  // legacy aliases
  user_question?: string;
  final_sql?: string;
  sql_attempts?: number;
  row_count?: number;
  execution_time_ms?: number;
  query_id?: string;
  natural_language_query?: string;
  generated_sql?: string;
}

// 查询上下文
export interface QueryContext {
  history_id: string;
  question: string;
  connection_id: string;
  rewritten_query?: string;
  retrieved_tables?: string[];
  final_sql?: string;
  steps?: AgentStep[];
  context_text: string;
  follow_up_context?: string;
  // legacy aliases
  query_id?: string;
  sql?: string;
  results_preview?: string;
}

export interface QueryHistoryDetail extends QueryHistoryItem {
  steps?: AgentStep[];
  result_row_count?: number | null;
  result?: ExecutionResult | null;
  chart?: ChartSuggestion | null;
  retrieved_table_details?: TableSchema[];
  telemetry?: QueryTelemetry;
  follow_up_context?: string | null;
}

export type QueryHistory = QueryHistoryDetail;
export type AssistantMessage = Message;
