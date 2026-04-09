export interface AnalyticsSummary {
  recent_success_rate: number;
  average_llm_latency_ms: number;
  average_db_latency_ms: number;
  top_tables?: TopTable[];
  error_distribution?: ErrorDistribution[];
  // legacy aliases used by older UI
  total_queries?: number;
  success_rate?: number;
  avg_execution_time_ms?: number;
  avg_generation_time_ms?: number;
  successful_queries?: number;
  failed_queries?: number;
  avg_sql_attempts?: number;
}

export interface ErrorDistribution {
  type?: string;
  error_type: string;
  count: number;
  percentage: number;
  recent_example?: string;
}

export interface TopTable {
  table: string;
  count: number;
  // legacy aliases
  table_name?: string;
  query_count?: number;
  success_rate?: number;
}

// 分析报告
export interface AnalyticsReport {
  summary: AnalyticsSummary;
  errors: ErrorDistribution[];
  top_tables: TopTable[];
  recent_queries?: RecentQuery[];
  time_range?: {
    start_date: string;
    end_date: string;
  };
  // legacy aliases
  error_distribution?: ErrorDistribution[];
  top_queried_tables?: TopTable[];
}

// 最近查询
export interface RecentQuery {
  query_id: string;
  question: string;
  status: string;
  created_at: string;
  execution_time_ms?: number;
  // legacy aliases
  natural_language_query?: string;
}
