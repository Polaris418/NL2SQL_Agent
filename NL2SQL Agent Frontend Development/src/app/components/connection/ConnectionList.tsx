import { useEffect, useState } from 'react';
import { Database, Trash2, RefreshCw, CheckCircle2, XCircle, Zap, Loader2 } from 'lucide-react';
import { Button } from '../ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { Badge } from '../ui/badge';
import { ScrollArea } from '../ui/scroll-area';
import { useConnectionStore } from '../../store/connectionStore';

export function ConnectionList() {
  const {
    connections,
    activeConnectionId,
    isLoading,
    fetchConnections,
    deleteConnection,
    setActiveConnection,
    syncSchema,
    testConnection,
  } = useConnectionStore();
  const [testingConnectionId, setTestingConnectionId] = useState<string | null>(null);

  useEffect(() => {
    fetchConnections();
  }, [fetchConnections]);

  if (isLoading && connections.length === 0) {
    return (
      <Card className="rounded-[28px] border-zinc-200 bg-white text-black shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
        <CardHeader>
          <CardTitle className="text-black">数据库连接</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-zinc-500">加载中...</p>
        </CardContent>
      </Card>
    );
  }

  if (connections.length === 0) {
    return (
      <Card className="rounded-[28px] border-zinc-200 bg-white text-black shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
        <CardHeader>
          <CardTitle className="text-black">数据库连接</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-zinc-500">暂无连接，请先创建数据库连接</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="rounded-[24px] border-zinc-200 bg-white text-black shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center justify-between text-black">
          <div>
            <div className="text-[11px] uppercase tracking-[0.24em] text-zinc-500">Connections</div>
            <div className="mt-1 text-lg">数据库连接</div>
          </div>
          <Button variant="ghost" size="sm" onClick={fetchConnections} className="rounded-xl text-zinc-700 hover:bg-zinc-100 hover:text-black">
            <RefreshCw className="h-4 w-4" />
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent className="pt-0">
        <ScrollArea className="max-h-[320px] xl:max-h-[calc(100dvh-320px)]">
          <div className="space-y-3">
            {connections.map((connection) => (
              <div
                key={connection.id}
                className={`
                  cursor-pointer rounded-[24px] border p-3.5 transition-all
                  ${
                    activeConnectionId === connection.id
                      ? 'border-black bg-zinc-50 shadow-[0_10px_24px_rgba(0,0,0,0.06)]'
                      : 'border-zinc-200 bg-white hover:border-zinc-300 hover:bg-zinc-50'
                  }
                `}
                onClick={() => setActiveConnection(connection.id)}
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="flex items-start gap-2 flex-1 min-w-0">
                    <div className="mt-0.5 flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-2xl bg-zinc-100">
                      <Database className="h-5 w-5" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="mb-1 flex items-center gap-2">
                        <h4 className="truncate font-semibold text-black">{connection.name}</h4>
                        <Badge variant="outline" className="border-zinc-200 bg-zinc-50 text-[10px] uppercase tracking-[0.2em] text-zinc-600">
                          {connection.db_type}
                        </Badge>
                      </div>
                      <div className="flex items-center gap-2 text-xs text-zinc-600">
                        {connection.is_online ? (
                          <>
                            <CheckCircle2 className="h-3 w-3 text-black" />
                            <span>在线</span>
                          </>
                        ) : (
                          <>
                            <XCircle className="h-3 w-3 text-zinc-400" />
                            <span>离线</span>
                          </>
                        )}
                      </div>
                      {connection.db_type !== 'sqlite' ? (
                        <p className="mt-2 truncate text-xs text-zinc-500">
                          {connection.host}:{connection.port}/{connection.database}
                        </p>
                      ) : (
                        <p className="mt-2 truncate text-xs text-zinc-500">
                          {connection.file_path || connection.database}
                        </p>
                      )}
                    </div>
                  </div>
                  <div className="flex gap-1">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="rounded-xl text-zinc-600 hover:bg-zinc-100 hover:text-black"
                      onClick={async (e) => {
                        e.stopPropagation();
                        setTestingConnectionId(connection.id);
                        try {
                          await testConnection(connection.id);
                        } finally {
                          setTestingConnectionId((current) => (current === connection.id ? null : current));
                        }
                      }}
                      title="测试连接"
                    >
                      {testingConnectionId === connection.id ? (
                        <Loader2 className="h-3 w-3 animate-spin" />
                      ) : (
                        <Zap className="h-3 w-3" />
                      )}
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="rounded-xl text-zinc-600 hover:bg-zinc-100 hover:text-black"
                      onClick={(e) => {
                        e.stopPropagation();
                        syncSchema(connection.id);
                      }}
                      title="同步 Schema"
                    >
                      <RefreshCw className="h-3 w-3" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="rounded-xl text-zinc-600 hover:bg-zinc-100 hover:text-black"
                      onClick={(e) => {
                        e.stopPropagation();
                        if (confirm(`确定要删除连接 "${connection.name}" 吗？`)) {
                          deleteConnection(connection.id);
                        }
                      }}
                      title="删除连接"
                    >
                        <Trash2 className="h-3 w-3 text-zinc-500" />
                      </Button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}
