import { useEffect } from 'react';
import { Database } from 'lucide-react';
import { useConnectionStore } from '../../store/connectionStore';
import { ConnectionList } from './ConnectionList';
import { ConnectionForm } from './ConnectionForm';

export function ConnectionsPage() {
  const { connections, activeConnectionId, fetchConnections } = useConnectionStore();

  useEffect(() => {
    void fetchConnections();
  }, [fetchConnections]);

  const activeConnection = connections.find((connection) => connection.id === activeConnectionId) ?? null;

  return (
    <div className="h-full overflow-auto">
      <div className="mx-auto flex min-h-full max-w-7xl flex-col gap-4 p-4 md:p-5">
        <section className="rounded-[28px] border border-zinc-200 bg-white p-5 shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
            <div className="min-w-0">
              <div className="mb-2 inline-flex items-center gap-2 rounded-full border border-zinc-200 bg-zinc-50 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.26em] text-zinc-500">
                <Database className="h-3.5 w-3.5 text-zinc-700" />
                Connections workspace
              </div>
              <h1 className="text-2xl text-black md:text-3xl">连接管理</h1>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-zinc-600">
                在这里创建、测试、切换和同步连接。查询页只保留对话工作流。
              </p>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 xl:w-[340px]">
              <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3">
                <div className="text-xs uppercase tracking-[0.24em] text-zinc-500">Total</div>
                <div className="mt-1.5 text-2xl font-semibold text-black">{connections.length}</div>
                <div className="text-xs text-zinc-500">已配置连接</div>
              </div>
              <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3">
                <div className="text-xs uppercase tracking-[0.24em] text-zinc-500">Active</div>
                <div className="mt-1.5 truncate text-lg font-semibold text-black">
                  {activeConnection?.name ?? '未选择'}
                </div>
                <div className="text-xs text-zinc-500">
                  {activeConnection?.is_online ? '在线可查询' : '请选择一个在线连接'}
                </div>
              </div>
            </div>
          </div>
        </section>

        <div className="grid min-h-0 gap-4 xl:items-start xl:grid-cols-[1.12fr_0.88fr]">
          <section>
            <ConnectionList />
          </section>
          <section>
            <ConnectionForm />
          </section>
        </div>
      </div>
    </div>
  );
}
