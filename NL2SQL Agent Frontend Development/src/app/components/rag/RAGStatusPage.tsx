import { useEffect, useMemo, useState } from 'react';
import { Activity, Database, RefreshCcw, ShieldCheck, Sparkles } from 'lucide-react';
import { useConnectionStore } from '../../store/connectionStore';
import { Button } from '../ui/button';

function formatTime(value?: string | null) {
  if (!value) {
    return '暂无';
  }
  try {
    return new Date(value).toLocaleString('zh-CN', {
      month: 'numeric',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return value;
  }
}

function getStatusTone(status?: string) {
  switch (status) {
    case 'ready':
    case 'healthy':
      return 'bg-emerald-50 text-emerald-700 border-emerald-200';
    case 'indexing':
    case 'degraded':
      return 'bg-amber-50 text-amber-700 border-amber-200';
    case 'failed':
    case 'unhealthy':
      return 'bg-rose-50 text-rose-700 border-rose-200';
    default:
      return 'bg-zinc-100 text-zinc-600 border-zinc-200';
  }
}

function getIndexStatusLabel(status?: string) {
  switch (status) {
    case 'ready':
      return '已预构建';
    case 'indexing':
      return '构建中';
    case 'failed':
      return '构建失败';
    default:
      return '待构建';
  }
}

export function RAGStatusPage() {
  const {
    connections,
    activeConnectionId,
    ragIndexStates,
    ragHealthDetails,
    ragTelemetryDashboard,
    fetchConnections,
    fetchRagStates,
    fetchRagHealth,
    rebuildRagIndex,
    setActiveConnection,
  } = useConnectionStore();
  const [selectedConnectionId, setSelectedConnectionId] = useState('');
  const [isRefreshing, setIsRefreshing] = useState(false);

  useEffect(() => {
    void Promise.all([fetchConnections(), fetchRagStates()]);
  }, [fetchConnections, fetchRagStates]);

  useEffect(() => {
    if (!selectedConnectionId) {
      const fallbackId = activeConnectionId ?? connections[0]?.id ?? '';
      if (fallbackId) {
        setSelectedConnectionId(fallbackId);
      }
    }
  }, [activeConnectionId, connections, selectedConnectionId]);

  useEffect(() => {
    if (selectedConnectionId) {
      void fetchRagHealth(selectedConnectionId);
    }
  }, [selectedConnectionId, fetchRagHealth]);

  const activeConnection = useMemo(
    () => connections.find((connection) => connection.id === selectedConnectionId) ?? null,
    [connections, selectedConnectionId]
  );
  const statusList = useMemo(
    () => Object.values(ragIndexStates),
    [ragIndexStates]
  );
  const activeState = selectedConnectionId ? ragIndexStates[selectedConnectionId] : undefined;
  const activeHealth = selectedConnectionId ? ragHealthDetails[selectedConnectionId] : undefined;
  const readyCount = statusList.filter((item) => item.index_status === 'ready').length;
  const indexingCount = statusList.filter((item) => item.index_status === 'indexing').length;
  const failedCount = statusList.filter((item) => item.index_status === 'failed').length;

  const handleRefresh = async () => {
    setIsRefreshing(true);
    await Promise.all([fetchConnections(), fetchRagStates()]);
    if (selectedConnectionId) {
      await fetchRagHealth(selectedConnectionId);
    }
    setIsRefreshing(false);
  };

  return (
    <div className="h-full overflow-auto">
      <div className="mx-auto flex min-h-full max-w-7xl flex-col gap-4 p-4 md:p-5">
        <section className="rounded-[28px] border border-zinc-200 bg-white p-5 shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
            <div className="min-w-0">
              <div className="mb-2 inline-flex items-center gap-2 rounded-full border border-zinc-200 bg-zinc-50 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.26em] text-zinc-500">
                <Sparkles className="h-3.5 w-3.5 text-zinc-700" />
                RAG workspace
              </div>
              <h1 className="text-2xl text-black md:text-3xl">每连接预构建的 RAG 状态</h1>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-zinc-600">
                每个数据库连接在创建或同步 Schema 后都会自动提前预构建检索增强。这里可以直接查看已构建状态、健康情况和最近一次构建信息。
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" className="rounded-2xl" onClick={() => void handleRefresh()}>
                <RefreshCcw className={`mr-2 h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
                刷新状态
              </Button>
              {selectedConnectionId ? (
                <Button className="rounded-2xl" onClick={() => void rebuildRagIndex(selectedConnectionId)}>
                  重新预构建
                </Button>
              ) : null}
            </div>
          </div>
        </section>

        <div className="grid min-h-0 gap-4 xl:grid-cols-[0.94fr_1.06fr]">
          <section className="rounded-[28px] border border-zinc-200 bg-white shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
            <div className="border-b border-zinc-200 px-5 py-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.26em] text-zinc-500">Connections</div>
              <h2 className="mt-1 text-xl text-black">数据库与预构建状态</h2>
            </div>
            <div className="space-y-3 p-4">
              {connections.map((connection) => {
                const state = ragIndexStates[connection.id];
                const isActive = connection.id === selectedConnectionId;
                return (
                  <button
                    key={connection.id}
                    type="button"
                    onClick={() => {
                      setSelectedConnectionId(connection.id);
                      setActiveConnection(connection.id);
                    }}
                    className={`w-full rounded-3xl border p-4 text-left transition ${
                      isActive
                        ? 'border-black bg-zinc-50 shadow-[0_12px_24px_rgba(0,0,0,0.06)]'
                        : 'border-zinc-200 bg-white hover:bg-zinc-50'
                    }`}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="truncate text-lg font-semibold text-black">{connection.name}</div>
                        <div className="mt-1 truncate text-sm text-zinc-500">
                          {connection.db_type} / {connection.database}
                        </div>
                      </div>
                      <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${getStatusTone(state?.index_status)}`}>
                        {getIndexStatusLabel(state?.index_status)}
                      </span>
                    </div>
                    <div className="mt-3 grid gap-2 text-xs text-zinc-500 sm:grid-cols-3">
                      <div>健康：{state?.health_status ?? 'unknown'}</div>
                      <div>表数：{state?.table_count ?? 0}</div>
                      <div>向量：{state?.vector_count ?? 0}</div>
                    </div>
                  </button>
                );
              })}
            </div>
          </section>

          <section className="rounded-[28px] border border-zinc-200 bg-white shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
            <div className="border-b border-zinc-200 px-5 py-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.26em] text-zinc-500">Active connection</div>
              <h2 className="mt-1 text-xl text-black">{activeConnection?.name ?? '请选择一个数据库连接'}</h2>
            </div>
            {activeConnection ? (
              <div className="space-y-4 p-5">
                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-3xl border border-zinc-200 bg-zinc-50 p-4">
                    <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-zinc-500">
                      <Database className="h-3.5 w-3.5" />
                      数据库
                    </div>
                    <div className="mt-2 truncate text-base font-semibold text-black">{activeConnection.database}</div>
                    <div className="text-xs text-zinc-500">{activeConnection.db_type}</div>
                  </div>
                  <div className="rounded-3xl border border-zinc-200 bg-zinc-50 p-4">
                    <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-zinc-500">
                      <Sparkles className="h-3.5 w-3.5" />
                      构建状态
                    </div>
                    <div className="mt-2 text-base font-semibold text-black">{activeState?.index_status ?? 'pending'}</div>
                    <div className="text-xs text-zinc-500">schema 版本 {activeState?.schema_version?.slice(0, 8) ?? '暂无'}</div>
                  </div>
                  <div className="rounded-3xl border border-zinc-200 bg-zinc-50 p-4">
                    <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-zinc-500">
                      <ShieldCheck className="h-3.5 w-3.5" />
                      健康状态
                    </div>
                    <div className="mt-2 text-base font-semibold text-black">{activeState?.health_status ?? 'unknown'}</div>
                    <div className="text-xs text-zinc-500">
                      {activeHealth?.vector_store_available ? '向量可用' : '向量不可用'} / {activeHealth?.bm25_enabled ? 'BM25 已启用' : 'BM25 未启用'}
                    </div>
                  </div>
                  <div className="rounded-3xl border border-zinc-200 bg-zinc-50 p-4">
                    <div className="flex items-center gap-2 text-xs uppercase tracking-[0.2em] text-zinc-500">
                      <Activity className="h-3.5 w-3.5" />
                      最近构建
                    </div>
                    <div className="mt-2 text-base font-semibold text-black">{formatTime(activeState?.last_completed_at)}</div>
                    <div className="text-xs text-zinc-500">最近成功 {formatTime(activeState?.last_success_at)}</div>
                  </div>
                </div>

                <div className="grid gap-4 lg:grid-cols-[0.94fr_1.06fr]">
                  <div className="rounded-3xl border border-zinc-200 p-4">
                    <div className="text-sm font-semibold text-black">索引概览</div>
                    <div className="mt-3 grid gap-3 sm:grid-cols-2">
                      <div className="rounded-2xl bg-zinc-50 p-3">
                        <div className="text-xs text-zinc-500">表数</div>
                        <div className="mt-1 text-xl font-semibold text-black">{activeState?.table_count ?? 0}</div>
                      </div>
                      <div className="rounded-2xl bg-zinc-50 p-3">
                        <div className="text-xs text-zinc-500">向量数</div>
                        <div className="mt-1 text-xl font-semibold text-black">{activeState?.vector_count ?? 0}</div>
                      </div>
                      <div className="rounded-2xl bg-zinc-50 p-3">
                        <div className="text-xs text-zinc-500">索引模式</div>
                        <div className="mt-1 text-sm font-semibold text-black">{activeState?.index_mode ?? 'hybrid'}</div>
                      </div>
                      <div className="rounded-2xl bg-zinc-50 p-3">
                        <div className="text-xs text-zinc-500">当前任务</div>
                        <div className="mt-1 text-sm font-semibold text-black">
                          {String(activeHealth?.async_snapshot?.job_state ?? activeHealth?.current_job?.status ?? 'idle')}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-3xl border border-zinc-200 p-4">
                    <div className="text-sm font-semibold text-black">RAG 运行摘要</div>
                    <div className="mt-3 space-y-3 text-sm leading-6 text-zinc-600">
                      <p>连接创建或同步 Schema 后，系统会自动提前预构建该数据库的 Schema RAG 索引。</p>
                      <p>查询时会直接复用这份预构建能力，不需要等第一次提问时再临时建索引。</p>
                      <p>
                        最近遥测快照记录了{' '}
                        <span className="font-semibold text-black">
                          {Number(ragTelemetryDashboard?.latest_snapshot?.logged_queries ?? 0)}
                        </span>{' '}
                        次检索请求，上下文裁剪事件{' '}
                        <span className="font-semibold text-black">
                          {Number(ragTelemetryDashboard?.context_limit_events?.length ?? 0)}
                        </span>{' '}
                        次。
                      </p>
                      {activeState?.last_error ? (
                        <div className="rounded-2xl border border-rose-200 bg-rose-50 p-3 text-rose-700">
                          最近错误：{activeState.last_error}
                        </div>
                      ) : null}
                    </div>
                  </div>
                </div>

                <div className="grid gap-3 sm:grid-cols-4">
                  <div className="rounded-2xl border border-zinc-200 bg-zinc-50 p-3">
                    <div className="text-xs text-zinc-500">已预构建</div>
                    <div className="mt-1 text-2xl font-semibold text-black">{readyCount}</div>
                  </div>
                  <div className="rounded-2xl border border-zinc-200 bg-zinc-50 p-3">
                    <div className="text-xs text-zinc-500">构建中</div>
                    <div className="mt-1 text-2xl font-semibold text-black">{indexingCount}</div>
                  </div>
                  <div className="rounded-2xl border border-zinc-200 bg-zinc-50 p-3">
                    <div className="text-xs text-zinc-500">失败</div>
                    <div className="mt-1 text-2xl font-semibold text-black">{failedCount}</div>
                  </div>
                  <div className="rounded-2xl border border-zinc-200 bg-zinc-50 p-3">
                    <div className="text-xs text-zinc-500">缓存命中率</div>
                    <div className="mt-1 text-2xl font-semibold text-black">
                      {typeof ragTelemetryDashboard?.current?.cache_hit_rate === 'number'
                        ? `${Math.round((ragTelemetryDashboard.current.cache_hit_rate as number) * 100)}%`
                        : '0%'}
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="p-5 text-sm text-zinc-500">先选择一个数据库连接，再查看对应的 RAG 预构建状态。</div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}
