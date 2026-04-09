import { useEffect, useMemo, useState } from 'react';
import { ChevronRight, Clock, Database, History, MessageSquare, RefreshCw, Search, Trash2 } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { zhCN } from 'date-fns/locale';
import { Badge } from '../ui/badge';
import { Button } from '../ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { Input } from '../ui/input';
import { ScrollArea } from '../ui/scroll-area';
import { RAGPanel } from '../chat/RAGPanel';
import { ResultTable } from '../chat/ResultTable';
import { ChartPanel } from '../chat/ChartPanel';
import type { QueryHistoryDetail, QueryHistoryItem } from '../../types/query';
import { useConnectionStore } from '../../store/connectionStore';
import { toast } from 'sonner';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '../ui/alert-dialog';

interface HistoryPanelProps {
  histories: Array<QueryHistoryItem | QueryHistoryDetail>;
  onLoadDetail: (queryId: string) => Promise<QueryHistoryDetail>;
  onSelectHistory: (
    history: QueryHistoryItem | QueryHistoryDetail,
    threadItems?: Array<QueryHistoryItem | QueryHistoryDetail>,
  ) => void;
  onRetry: (queryId: string) => void;
  onFollowUp: (queryId: string, question: string) => void;
  onDelete: (queryId: string) => Promise<void>;
}

interface HistoryThread {
  id: string;
  root: QueryHistoryItem | QueryHistoryDetail;
  latest: QueryHistoryItem | QueryHistoryDetail;
  items: Array<QueryHistoryItem | QueryHistoryDetail>;
}

function getHistoryId(history: QueryHistoryItem | QueryHistoryDetail) {
  return history.id || history.query_id || '';
}

function getHistoryQuestion(history: QueryHistoryItem | QueryHistoryDetail) {
  return history.question || history.natural_language_query || '';
}

function getHistorySql(history: QueryHistoryItem | QueryHistoryDetail) {
  return history.sql || history.final_sql || history.generated_sql || '';
}

function getHistoryRowCount(history: QueryHistoryItem | QueryHistoryDetail) {
  return history.result_row_count ?? history.row_count ?? 0;
}

function getHistoryResult(history: QueryHistoryItem | QueryHistoryDetail) {
  return 'result' in history ? history.result ?? null : null;
}

function getHistoryChart(history: QueryHistoryItem | QueryHistoryDetail) {
  return 'chart' in history ? history.chart ?? null : null;
}

function getHistoryRetrievedTableDetails(history: QueryHistoryItem | QueryHistoryDetail) {
  return 'retrieved_table_details' in history ? history.retrieved_table_details ?? [] : [];
}

function getHistoryTelemetry(history: QueryHistoryItem | QueryHistoryDetail) {
  return 'telemetry' in history ? history.telemetry ?? undefined : undefined;
}

function getHistoryExecutionTime(history: QueryHistoryItem | QueryHistoryDetail) {
  return history.db_latency_ms ?? history.execution_time_ms ?? 0;
}

function formatExecutionTime(history: QueryHistoryItem | QueryHistoryDetail) {
  const value = getHistoryExecutionTime(history);
  if (!value) {
    return '';
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(value >= 10000 ? 1 : 2)}s`;
  }
  if (value >= 100) {
    return `${Math.round(value)}ms`;
  }
  return `${value.toFixed(2)}ms`;
}

function formatTime(timestamp: string) {
  try {
    return formatDistanceToNow(new Date(timestamp), { addSuffix: true, locale: zhCN });
  } catch {
    return timestamp;
  }
}

function getStatusBadge(status: string) {
  switch (status) {
    case 'success':
      return <Badge className="bg-green-500">成功</Badge>;
    case 'failed':
    case 'error':
      return <Badge variant="destructive">失败</Badge>;
    case 'partial':
      return <Badge variant="secondary">部分成功</Badge>;
    default:
      return <Badge variant="outline">{status}</Badge>;
  }
}

function formatConnectionFallback(connectionId: string) {
  if (!connectionId) {
    return '未知连接';
  }
  if (connectionId.length <= 24) {
    return connectionId;
  }
  return `${connectionId.slice(0, 24)}...`;
}

function getDisplayDatabaseLabel(rawValue?: string | null) {
  if (!rawValue) {
    return '';
  }

  const normalized = rawValue.replace(/\\/g, '/');
  const segments = normalized.split('/');
  return segments[segments.length - 1] || rawValue;
}

function getHistoryCreatedAt(history: QueryHistoryItem | QueryHistoryDetail) {
  return new Date(history.created_at).getTime();
}

export function HistoryPanel({ histories, onLoadDetail, onSelectHistory, onRetry, onFollowUp, onDelete }: HistoryPanelProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedHistory, setSelectedHistory] = useState<QueryHistoryDetail | null>(null);
  const [followUpInput, setFollowUpInput] = useState('');
  const [loadingDetailId, setLoadingDetailId] = useState<string | null>(null);
  const [showFollowUpContext, setShowFollowUpContext] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [historyToDelete, setHistoryToDelete] = useState<string | null>(null);
  const [deletingIds, setDeletingIds] = useState<Set<string>>(new Set());
  const { connections } = useConnectionStore();

  const connectionMap = useMemo(
    () => new Map(connections.map((connection) => [connection.id, connection])),
    [connections],
  );

  const historyMap = useMemo(
    () => new Map(histories.map((history) => [getHistoryId(history), history])),
    [histories],
  );

  const getThreadRootId = (history: QueryHistoryItem | QueryHistoryDetail) => {
    let current = history;
    const visited = new Set<string>();

    while (current.context_source_query_id) {
      const nextId = current.context_source_query_id;
      if (!nextId || visited.has(nextId)) {
        break;
      }
      visited.add(nextId);
      const next = historyMap.get(nextId);
      if (!next) {
        break;
      }
      current = next;
    }

    return getHistoryId(current);
  };

  const filteredHistories = useMemo(() => {
    const keyword = searchQuery.trim().toLowerCase();
    if (!keyword) {
      return histories;
    }
    return histories.filter((history) => {
      const question = getHistoryQuestion(history).toLowerCase();
      const sql = getHistorySql(history).toLowerCase();
      const errorMessage = (history.error_message || '').toLowerCase();
      return question.includes(keyword) || sql.includes(keyword) || errorMessage.includes(keyword);
    });
  }, [histories, searchQuery]);

  const threads = useMemo(() => {
    const grouped = new Map<string, Array<QueryHistoryItem | QueryHistoryDetail>>();

    for (const history of filteredHistories) {
      const rootId = getThreadRootId(history);
      const bucket = grouped.get(rootId) ?? [];
      bucket.push(history);
      grouped.set(rootId, bucket);
    }

    return Array.from(grouped.entries())
      .map(([threadId, items]): HistoryThread => {
        const sortedItems = [...items].sort((a, b) => getHistoryCreatedAt(b) - getHistoryCreatedAt(a));
        const latest = sortedItems[0];
        const root = historyMap.get(threadId) ?? sortedItems[sortedItems.length - 1];
        return {
          id: threadId,
          root,
          latest,
          items: sortedItems,
        };
      })
      .sort((a, b) => getHistoryCreatedAt(b.latest) - getHistoryCreatedAt(a.latest));
  }, [filteredHistories, historyMap]);

  const getConnectionLabel = (history: QueryHistoryItem | QueryHistoryDetail) => {
    const connection = connectionMap.get(history.connection_id);
    if (!connection) {
      return formatConnectionFallback(history.connection_id);
    }
    const databaseLabel = connection.db_type === 'sqlite'
      ? getDisplayDatabaseLabel(connection.file_path || connection.database)
      : connection.database;
    return `${connection.name} / ${databaseLabel}`;
  };

  const selectedThread = useMemo(() => {
    if (!selectedHistory) {
      return null;
    }
    const selectedRootId = getThreadRootId(selectedHistory);
    return threads.find((thread) => thread.id === selectedRootId) ?? null;
  }, [selectedHistory, threads]);

  const selectedThreadItems = useMemo(() => {
    if (!selectedThread) {
      return [];
    }
    return [...selectedThread.items].sort((a, b) => getHistoryCreatedAt(a) - getHistoryCreatedAt(b));
  }, [selectedThread]);

  const handleOpenHistory = async (history: QueryHistoryItem | QueryHistoryDetail) => {
    const historyId = getHistoryId(history);
    setLoadingDetailId(historyId);
    try {
      const detail = await onLoadDetail(historyId);
      setSelectedHistory(detail);
      setShowFollowUpContext(false);
    } catch (error: any) {
      toast.error(`加载查询详情失败: ${error?.message || '未知错误'}`);
    } finally {
      setLoadingDetailId(null);
    }
  };

  const handleFollowUp = () => {
    if (!selectedHistory || !followUpInput.trim()) {
      return;
    }
    onFollowUp(getHistoryId(selectedHistory), followUpInput.trim());
    setFollowUpInput('');
  };

  const handleDeleteClick = (queryId: string, event?: React.MouseEvent) => {
    if (event) {
      event.stopPropagation(); // 防止触发打开详情
    }
    setHistoryToDelete(queryId);
    setShowDeleteDialog(true);
  };

  const handleConfirmDelete = async () => {
    if (!historyToDelete) return;
    
    setDeletingIds(prev => new Set(prev).add(historyToDelete));
    
    try {
      await onDelete(historyToDelete);
      
      // 如果删除的是当前选中的历史记录，清空选中状态
      if (selectedHistory && getHistoryId(selectedHistory) === historyToDelete) {
        setSelectedHistory(null);
      }
      
      setShowDeleteDialog(false);
      setHistoryToDelete(null);
    } catch (error) {
      // 错误已在父组件处理
    } finally {
      setDeletingIds(prev => {
        const next = new Set(prev);
        next.delete(historyToDelete);
        return next;
      });
    }
  };

  useEffect(() => {
    if (threads.length === 0 || selectedHistory || loadingDetailId) {
      return;
    }

    void handleOpenHistory(threads[0].latest);
  }, [threads, selectedHistory, loadingDetailId]);

  return (
    <div className="grid h-full min-h-0 gap-3 lg:grid-cols-[340px_minmax(0,1fr)]">
      <Card className="flex min-h-0 flex-col overflow-hidden rounded-[24px] border-zinc-200">
        <CardHeader className="border-b">
          <div className="flex items-start justify-between gap-2">
            <div className="min-w-0">
              <CardTitle className="flex items-center gap-2">
                <History className="h-5 w-5" />
                对话查询历史
              </CardTitle>
              <p className="mt-1 text-xs text-muted-foreground">像聊天记录一样查看每条查询，点击左侧记录在右侧查看详情。</p>
            </div>
          </div>
          <div className="relative mt-2">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input placeholder="搜索历史记录..." value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)} className="pl-9" />
          </div>
        </CardHeader>
        <CardContent className="min-h-0 flex-1 p-0">
          <ScrollArea className="h-full">
            <div className="space-y-2 pr-2 pl-3 py-3">
              {threads.length === 0 ? (
                <div className="py-12 text-center text-muted-foreground">
                  <History className="mx-auto mb-4 h-12 w-12 opacity-20" />
                  <p>暂无历史记录</p>
                </div>
              ) : (
                threads.map((thread) => {
                  const isActive = selectedHistory && getThreadRootId(selectedHistory) === thread.id;
                  const turnCount = thread.items.length;
                  const primaryHistory = thread.root;
                  const latestHistory = thread.latest;
                  const historyId = getHistoryId(latestHistory);
                  const isDeleting = deletingIds.has(historyId);
                  
                  return (
                    <div
                      key={thread.id}
                      className={`relative w-full rounded-2xl border text-left transition ${
                        isActive ? 'border-black bg-zinc-50' : 'border-zinc-200 bg-white hover:bg-zinc-50'
                      } ${isDeleting ? 'opacity-50' : ''}`}
                    >
                      <button
                        type="button"
                        onClick={() => void handleOpenHistory(latestHistory)}
                        disabled={isDeleting}
                        title={getHistoryQuestion(primaryHistory)}
                        className="w-full p-4 text-left"
                      >
                        <div>
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0 flex-1">
                              <p className="line-clamp-2 text-sm font-medium text-black">{getHistoryQuestion(primaryHistory)}</p>
                              <div className="mt-2 flex items-center gap-1 text-xs text-muted-foreground" title={getConnectionLabel(latestHistory)}>
                                <Database className="h-3 w-3" />
                                <span className="block min-w-0 truncate">{getConnectionLabel(latestHistory)}</span>
                              </div>
                            </div>
                            <div className="flex items-center gap-2">
                              {turnCount > 1 ? <Badge variant="outline">{turnCount} 轮</Badge> : null}
                              {getStatusBadge(latestHistory.status)}
                            </div>
                          </div>
                          <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                            <div className="flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
                              <span className="flex items-center gap-1">
                                <Clock className="h-3 w-3" />
                                {formatTime(latestHistory.created_at)}
                              </span>
                              {loadingDetailId === historyId && <span>加载中...</span>}
                              {!!getHistoryExecutionTime(latestHistory) && <span>{formatExecutionTime(latestHistory)}</span>}
                              {!!getHistoryRowCount(latestHistory) && <span>{getHistoryRowCount(latestHistory)} 行</span>}
                            </div>
                          </div>
                        </div>
                      </button>
                      <button
                        type="button"
                        onClick={(e) => handleDeleteClick(historyId, e)}
                        disabled={isDeleting}
                        className="absolute bottom-3 right-3 rounded-lg p-1.5 text-muted-foreground hover:bg-destructive/10 hover:text-destructive transition-colors"
                        title="删除此记录"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    </div>
                  );
                })
              )}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>

      <Card className="flex min-h-0 flex-col overflow-hidden rounded-[24px] border-zinc-200">
        {selectedHistory ? (
          <>
            <div className="border-b px-6 py-5">
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <h2 className="text-xl font-semibold text-black">{getHistoryQuestion(selectedHistory)}</h2>
                  <div className="mt-2 flex flex-wrap items-center gap-3 text-sm text-zinc-500">
                    <span>{getConnectionLabel(selectedHistory)}</span>
                    {!!getHistoryExecutionTime(selectedHistory) && <span>{formatExecutionTime(selectedHistory)}</span>}
                    {!!getHistoryRowCount(selectedHistory) && <span>{getHistoryRowCount(selectedHistory)} 行</span>}
                    <span>{formatTime(selectedHistory.created_at)}</span>
                  </div>
                </div>
                {getStatusBadge(selectedHistory.status)}
              </div>
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto px-6 py-5">
              <div className="space-y-5 min-w-0">
                {getHistorySql(selectedHistory) && (
                  <div>
                    <label className="mb-2 block text-sm font-medium">生成的 SQL</label>
                    <pre className="max-w-full overflow-x-auto whitespace-pre-wrap rounded-2xl bg-muted p-4 text-xs font-mono">
                      {getHistorySql(selectedHistory)}
                    </pre>
                  </div>
                )}

                {selectedThreadItems.length > 1 && (
                  <div>
                    <label className="mb-3 block text-sm font-medium">本次对话记录</label>
                    <div className="space-y-2">
                      {selectedThreadItems.map((item, index) => {
                        const active = getHistoryId(item) === getHistoryId(selectedHistory);
                        return (
                          <button
                            key={getHistoryId(item)}
                            type="button"
                            onClick={() => void handleOpenHistory(item)}
                            className={`w-full rounded-2xl border px-4 py-3 text-left transition ${
                              active ? 'border-black bg-zinc-50' : 'border-zinc-200 bg-white hover:bg-zinc-50'
                            }`}
                          >
                            <div className="flex items-start justify-between gap-3">
                              <div className="min-w-0 flex-1">
                                <div className="flex items-center gap-2 text-xs text-zinc-500">
                                  <span>第 {index + 1} 轮</span>
                                  <span>{formatTime(item.created_at)}</span>
                                </div>
                                <p className="mt-1 line-clamp-2 text-sm font-medium text-black">
                                  {getHistoryQuestion(item)}
                                </p>
                              </div>
                              <div className="flex shrink-0 items-center gap-2">
                                {!!getHistoryRowCount(item) && (
                                  <span className="text-xs text-zinc-500">{getHistoryRowCount(item)} 行</span>
                                )}
                                {getStatusBadge(item.status)}
                              </div>
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                )}

                {(selectedHistory as QueryHistoryDetail).follow_up_context && (
                  <div>
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <label className="block text-sm font-medium">追问上下文</label>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="h-8 rounded-xl px-3 text-xs"
                        onClick={() => setShowFollowUpContext((value) => !value)}
                      >
                        {showFollowUpContext ? '收起' : '展开'}
                      </Button>
                    </div>
                    {showFollowUpContext ? (
                      <pre className="max-w-full overflow-x-auto whitespace-pre-wrap rounded-2xl bg-muted p-4 text-xs font-mono">
                        {(selectedHistory as QueryHistoryDetail).follow_up_context}
                      </pre>
                    ) : (
                      <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3 text-sm text-zinc-500">
                        这部分是系统保存的上下文信息，默认已收起，按需展开查看。
                      </div>
                    )}
                  </div>
                )}

                <RAGPanel
                  retrievedTables={getHistoryRetrievedTableDetails(selectedHistory)}
                  telemetry={getHistoryTelemetry(selectedHistory)}
                  steps={selectedHistory.steps || []}
                />

                {getHistoryResult(selectedHistory) && (
                  <div className="min-w-0 space-y-4">
                    {getHistoryChart(selectedHistory) && (
                      <div className="min-w-0 overflow-hidden">
                        <ChartPanel
                          result={getHistoryResult(selectedHistory)!}
                          suggestion={getHistoryChart(selectedHistory) || undefined}
                        />
                      </div>
                    )}
                    <div className="min-w-0 overflow-hidden">
                      <ResultTable result={getHistoryResult(selectedHistory)!} />
                    </div>
                  </div>
                )}

                <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-2xl border bg-muted/30 p-4">
                    <label className="text-xs text-muted-foreground">状态</label>
                    <div className="mt-2">{getStatusBadge(selectedHistory.status)}</div>
                  </div>
                  {!!getHistoryExecutionTime(selectedHistory) && (
                    <div className="rounded-2xl border bg-muted/30 p-4">
                      <label className="text-xs text-muted-foreground">执行时间</label>
                      <p className="mt-2 text-sm font-medium">{formatExecutionTime(selectedHistory)}</p>
                    </div>
                  )}
                  {!!getHistoryRowCount(selectedHistory) && (
                    <div className="rounded-2xl border bg-muted/30 p-4">
                      <label className="text-xs text-muted-foreground">结果行数</label>
                      <p className="mt-2 text-sm font-medium">{getHistoryRowCount(selectedHistory)} 行</p>
                    </div>
                  )}
                  <div className="rounded-2xl border bg-muted/30 p-4">
                    <label className="text-xs text-muted-foreground">创建时间</label>
                    <p className="mt-2 text-sm">{formatTime(selectedHistory.created_at)}</p>
                  </div>
                </div>

                {selectedHistory.error_message && (
                  <div>
                    <label className="mb-2 block text-sm font-medium text-destructive">错误信息</label>
                    <p className="rounded-2xl border border-destructive/20 bg-destructive/10 p-4 text-sm whitespace-pre-wrap">
                      {selectedHistory.error_message}
                    </p>
                    {selectedHistory.error_type && <p className="mt-2 text-xs text-muted-foreground">错误类型: {selectedHistory.error_type}</p>}
                    {selectedHistory.error_suggestion && <p className="mt-1 text-xs text-muted-foreground">建议: {selectedHistory.error_suggestion}</p>}
                  </div>
                )}
              </div>
            </div>

            <div className="border-t bg-background px-6 py-4">
              <div className="space-y-3">
                <div>
                  <label className="mb-2 block text-sm font-medium">基于此查询追问</label>
                  <div className="flex flex-col gap-2 sm:flex-row">
                    <Input
                      placeholder="输入追问问题..."
                      value={followUpInput}
                      onChange={(e) => setFollowUpInput(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' && !e.shiftKey) {
                          e.preventDefault();
                          handleFollowUp();
                        }
                      }}
                    />
                    <Button onClick={handleFollowUp} disabled={!followUpInput.trim()} className="sm:shrink-0">
                      <MessageSquare className="mr-1 h-4 w-4" />
                      追问
                    </Button>
                  </div>
                </div>
                <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-between">
                  <Button
                    variant="outline"
                    onClick={() => handleDeleteClick(getHistoryId(selectedHistory))}
                    className="text-destructive hover:text-destructive hover:bg-destructive/10"
                  >
                    <Trash2 className="mr-1 h-4 w-4" />
                    删除此记录
                  </Button>
                  <div className="flex flex-col-reverse gap-2 sm:flex-row">
                    <Button
                      variant="outline"
                      onClick={() => {
                        onRetry(getHistoryId(selectedHistory));
                      }}
                    >
                      <RefreshCw className="mr-1 h-4 w-4" />
                      重试
                    </Button>
                    <Button
                      onClick={() => {
                        onSelectHistory(selectedHistory, selectedThreadItems);
                      }}
                    >
                      <ChevronRight className="mr-1 h-4 w-4" />
                      查看完整结果
                    </Button>
                  </div>
                </div>
              </div>
            </div>
          </>
        ) : (
          <div className="flex h-full items-center justify-center p-8 text-center text-muted-foreground">
            <div>
              <History className="mx-auto mb-4 h-12 w-12 opacity-20" />
              <p className="text-base font-medium text-zinc-700">选择一条查询记录</p>
              <p className="mt-2 text-sm">左侧像聊天记录一样列出所有查询，点击任意一条即可在这里查看完整详情。</p>
            </div>
          </div>
        )}
      </Card>

      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认删除此历史记录？</AlertDialogTitle>
            <AlertDialogDescription>
              此操作将永久删除该查询历史记录，包括查询问题、SQL、结果等信息。此操作无法撤销。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction onClick={() => void handleConfirmDelete()} className="bg-destructive text-destructive-foreground hover:bg-destructive/90">
              确认删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
