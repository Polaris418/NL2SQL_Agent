import type { ConnectionRAGStatus } from './rag';

export type DBType = 'postgresql' | 'mysql' | 'sqlite';

export interface ConnectionConfig {
  name: string;
  db_type: DBType;
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  database: string;
  // SQLite 文件路径
  file_path?: string;
}

export interface Connection extends ConnectionConfig, ConnectionRAGStatus {
  id: string;
  is_online: boolean;
  created_at: string;
  updated_at?: string;
  schema_updated_at?: string | null;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable?: boolean;
  default?: string | null;
  comment?: string;
  is_primary_key?: boolean;
  is_foreign_key?: boolean;
  // legacy aliases
  primary_key?: boolean;
  foreign_key?: string;
}

export interface TableSchema {
  name: string;
  comment?: string;
  columns: ColumnInfo[];
  description?: string;
  updated_at?: string;
  // legacy aliases
  table_name?: string;
  table_comment?: string;
}

// Schema 缓存条目
export interface SchemaCacheEntry {
  connection_id: string;
  tables: TableSchema[];
  updated_at: string;
}

export interface ConnectionStatus extends Partial<ConnectionRAGStatus> {
  id: string;
  name?: string;
  db_type?: DBType;
  database?: string;
  host?: string;
  port?: number;
  username?: string;
  file_path?: string;
  is_online?: boolean;
  created_at?: string;
  updated_at?: string;
  schema_updated_at?: string | null;
  status?: 'online' | 'offline';
  message?: string;
}

// 连接测试结果
export interface ConnectionTestResult {
  connection_id?: string;
  success: boolean;
  message: string;
  latency_ms?: number;
  tested_at?: string;
}
