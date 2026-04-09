export type RAGIndexStatus = 'pending' | 'indexing' | 'ready' | 'failed';
export type RAGHealthStatus = 'unknown' | 'healthy' | 'degraded' | 'unhealthy';
export type RAGIndexMode = 'hybrid' | 'bm25_only' | 'vector';

export interface ConnectionRAGStatus {
  rag_index_state?: RAGIndexState | null;
  rag_health_detail?: RAGIndexHealthDetail | null;
  rag_index_status?: RAGIndexStatus | null;
  rag_health_status?: RAGHealthStatus | null;
  rag_index_mode?: RAGIndexMode | null;
  rag_schema_version?: string | null;
  rag_is_indexed?: boolean;
  rag_table_count?: number;
  rag_vector_count?: number;
  rag_last_started_at?: string | null;
  rag_last_completed_at?: string | null;
  rag_last_success_at?: string | null;
  rag_last_error?: string | null;
  rag_last_forced_rebuild?: boolean;
  rag_updated_at?: string;
  rag_created_at?: string;
}

export interface RAGIndexBuildRequest {
  force_full_rebuild?: boolean;
}

export interface RAGIndexState {
  connection_id: string;
  schema_version?: string | null;
  index_status: RAGIndexStatus;
  health_status: RAGHealthStatus;
  index_mode: RAGIndexMode;
  is_indexed: boolean;
  table_count: number;
  vector_count: number;
  last_started_at?: string | null;
  last_completed_at?: string | null;
  last_success_at?: string | null;
  last_error?: string | null;
  last_forced_rebuild?: boolean;
  updated_at?: string;
  created_at?: string;
}

export interface RAGIndexJob {
  id: string;
  connection_id: string;
  schema_version?: string | null;
  action: string;
  status: string;
  force_full_rebuild?: boolean;
  started_at: string;
  completed_at?: string | null;
  duration_ms?: number | null;
  error_message?: string | null;
  payload?: Record<string, unknown>;
}

export interface RAGIndexHealthDetail {
  state: RAGIndexState;
  current_job?: RAGIndexJob | null;
  latest_jobs: RAGIndexJob[];
  vector_store_available: boolean;
  bm25_enabled: boolean;
  degradation: Record<string, unknown>;
  async_snapshot: Record<string, unknown>;
}

export interface RAGTelemetryDashboard {
  current: Record<string, unknown>;
  latest_snapshot?: {
    id?: string | null;
    logged_queries?: number;
    metrics?: Record<string, unknown>;
    created_at?: string | null;
  } | null;
  recent_events: Record<string, unknown>[];
  history_count: number;
  store: Record<string, unknown>;
  context_limit_events: Record<string, unknown>[];
}
