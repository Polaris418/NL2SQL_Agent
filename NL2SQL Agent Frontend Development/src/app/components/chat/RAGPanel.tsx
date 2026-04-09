import {
  Activity,
  Box,
  Database,
  GitBranch,
  Layers3,
  ShieldCheck,
  Sparkles,
  TimerReset,
} from 'lucide-react';
import type { TableSchema } from '../../types/connection';
import type { AgentStep, QueryTelemetry } from '../../types/query';
import { Badge } from '../ui/badge';
import { Card } from '../ui/card';

interface RAGPanelProps {
  retrievedTables?: TableSchema[];
  telemetry?: QueryTelemetry;
  steps?: AgentStep[];
}

function formatMs(value?: number | null) {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return null;
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(2)}s`;
  }
  return `${Math.round(value)}ms`;
}

function getRelevantColumns(table: TableSchema) {
  return (table.columns || []).filter((column) => column.is_primary_key || column.is_foreign_key).slice(0, 4);
}

function getSchemaRetrievalSummary(steps: AgentStep[] = []) {
  const schemaStep = [...steps].reverse().find((step) => step.step_type === 'schema_retrieval');
  if (!schemaStep?.content) {
    return null;
  }
  return schemaStep.content;
}

export function RAGPanel({ retrievedTables = [], telemetry, steps = [] }: RAGPanelProps) {
  const retrievalSummary = getSchemaRetrievalSummary(steps);
  const hasPanelContent =
    retrievedTables.length > 0 ||
    Boolean(retrievalSummary) ||
    Boolean(telemetry?.retrieval_backend) ||
    typeof telemetry?.relationship_count === 'number' ||
    typeof telemetry?.column_annotation_count === 'number';

  if (!hasPanelContent) {
    return null;
  }

  const relationshipTables = telemetry?.relationship_tables ?? [];
  const droppedTables = telemetry?.packed_context_dropped_tables ?? [];
  const fewShotIds = telemetry?.few_shot_example_ids ?? [];
  const scopeCount = telemetry?.retrieval_scope_count ?? telemetry?.schema_table_count ?? 0;
  const candidateCount = telemetry?.retrieval_candidates ?? 0;
  const selectedCount = telemetry?.retrieval_selected ?? retrievedTables.length;
  const lexicalCount = telemetry?.retrieval_lexical_count ?? null;
  const vectorCount = telemetry?.retrieval_vector_count ?? null;

  return (
    <Card className="mb-4 overflow-hidden rounded-[24px] border-zinc-200 bg-white shadow-[0_8px_24px_rgba(0,0,0,0.04)]">
      <div className="flex items-center justify-between border-b border-zinc-200 bg-zinc-50 px-4 py-3">
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-black text-white">
            <Database className="h-4 w-4" />
          </div>
          <div>
            <div className="text-sm font-semibold text-zinc-900">RAG 检索详情</div>
            <div className="text-xs text-zinc-500">Schema 检索与上下文打包</div>
          </div>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          {telemetry?.retrieval_backend && (
            <Badge variant="outline" className="border-zinc-300 bg-white text-zinc-700">
              <Sparkles className="mr-1 h-3 w-3" />
              {telemetry.retrieval_backend}
            </Badge>
          )}
          {telemetry?.cache_hit && (
            <Badge className="bg-emerald-600 text-white hover:bg-emerald-600">缓存命中</Badge>
          )}
          {telemetry?.packed_context_truncated && (
            <Badge variant="outline" className="border-amber-300 bg-amber-50 text-amber-700">已截断</Badge>
          )}
        </div>
      </div>

      <div className="space-y-4 px-4 py-4">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-3 py-3">
            <div className="mb-2 flex items-center gap-2 text-xs uppercase tracking-[0.22em] text-zinc-500">
              <Layers3 className="h-3.5 w-3.5" />
              召回表
            </div>
            <div className="text-2xl font-semibold text-zinc-900">{retrievedTables.length}</div>
            <div className="mt-1 text-xs text-zinc-500">范围 {scopeCount} / 候选 {candidateCount} / 选中 {selectedCount}</div>
          </div>

          <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-3 py-3">
            <div className="mb-2 flex items-center gap-2 text-xs uppercase tracking-[0.22em] text-zinc-500">
              <GitBranch className="h-3.5 w-3.5" />
              关系
            </div>
            <div className="text-2xl font-semibold text-zinc-900">{telemetry?.relationship_count ?? 0}</div>
            <div className="mt-1 text-xs text-zinc-500">列注释 {telemetry?.column_annotation_count ?? 0}</div>
          </div>

          <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-3 py-3">
            <div className="mb-2 flex items-center gap-2 text-xs uppercase tracking-[0.22em] text-zinc-500">
              <TimerReset className="h-3.5 w-3.5" />
              延迟
            </div>
            <div className="text-2xl font-semibold text-zinc-900">{formatMs(telemetry?.retrieval_latency_ms) ?? '--'}</div>
            <div className="mt-1 text-xs text-zinc-500">词法 {lexicalCount ?? '--'} / 向量 {vectorCount ?? '--'}</div>
          </div>

          <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-3 py-3">
            <div className="mb-2 flex items-center gap-2 text-xs uppercase tracking-[0.22em] text-zinc-500">
              <ShieldCheck className="h-3.5 w-3.5" />
              Prompt 上下文
            </div>
            <div className="text-2xl font-semibold text-zinc-900">{telemetry?.packed_context_tables ?? 0}</div>
            <div className="mt-1 text-xs text-zinc-500">{telemetry?.packed_context_chars ?? 0} chars / {telemetry?.packed_context_tokens ?? 0} tokens</div>
          </div>
        </div>

        {retrievalSummary && (
          <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-sm font-medium text-zinc-900">
              <Activity className="h-4 w-4" />
              检索摘要
            </div>
            <p className="whitespace-pre-wrap break-words text-sm leading-6 text-zinc-700">{retrievalSummary}</p>
          </div>
        )}

        {retrievedTables.length > 0 && (
          <div className="rounded-2xl border border-zinc-200 bg-white px-4 py-4">
            <div className="mb-3 flex items-center justify-between">
              <div className="text-sm font-medium text-zinc-900">检索到的表</div>
              <div className="text-xs text-zinc-500">RAG 检索用于生成 SQL 的表</div>
            </div>
            <div className="grid gap-3 xl:grid-cols-2">
              {retrievedTables.map((table) => {
                const relevantColumns = getRelevantColumns(table);
                return (
                  <div key={table.name} className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3">
                    <div className="mb-2 flex items-start justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-zinc-900">{table.name}</div>
                        <div className="mt-1 text-xs text-zinc-500">{table.columns?.length ?? 0} 列{table.comment ? ` · ${table.comment}` : ''}</div>
                      </div>
                      <Badge variant="outline" className="border-zinc-300 bg-white text-zinc-700">schema hit</Badge>
                    </div>
                    {relevantColumns.length > 0 ? (
                      <div className="flex flex-wrap gap-2">
                        {relevantColumns.map((column) => (
                          <Badge key={`${table.name}-${column.name}`} variant="outline" className="border-zinc-300 bg-white text-zinc-700">
                            <Box className="mr-1 h-3 w-3" />
                            {column.name}
                            {column.is_primary_key ? ' · PK' : column.is_foreign_key ? ' · FK' : ''}
                          </Badge>
                        ))}
                      </div>
                    ) : (
                      <div className="text-xs text-zinc-500">无关键字段信息</div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {(relationshipTables.length > 0 || droppedTables.length > 0 || fewShotIds.length > 0) && (
          <div className="grid gap-3 lg:grid-cols-3">
            <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3">
              <div className="mb-2 text-sm font-medium text-zinc-900">关联表</div>
              {relationshipTables.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {relationshipTables.map((table) => (
                    <Badge key={table} variant="outline" className="border-zinc-300 bg-white text-zinc-700">{table}</Badge>
                  ))}
                </div>
              ) : (
                <div className="text-xs text-zinc-500">无额外关联表用于 join</div>
              )}
            </div>

            <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3">
              <div className="mb-2 text-sm font-medium text-zinc-900">丢弃的表</div>
              {droppedTables.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {droppedTables.map((table) => (
                    <Badge key={table} variant="outline" className="border-amber-300 bg-amber-50 text-amber-700">{table}</Badge>
                  ))}
                </div>
              ) : (
                <div className="text-xs text-zinc-500">所有表都包含在prompt中</div>
              )}
            </div>

            <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3">
              <div className="mb-2 text-sm font-medium text-zinc-900">Few-shot 示例</div>
              {fewShotIds.length > 0 ? (
                <div className="flex flex-wrap gap-2">
                  {fewShotIds.map((id) => (
                    <Badge key={id} variant="outline" className="border-zinc-300 bg-white text-zinc-700">{id}</Badge>
                  ))}
                </div>
              ) : (
                <div className="text-xs text-zinc-500">未使用 few-shot 示例</div>
              )}
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
