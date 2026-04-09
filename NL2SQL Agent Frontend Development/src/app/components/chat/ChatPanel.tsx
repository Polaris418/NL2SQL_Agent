import { useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { AlertCircle, Database, Sparkles, Workflow, Waves } from 'lucide-react';
import { ScrollArea } from '../ui/scroll-area';
import { Alert, AlertDescription } from '../ui/alert';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { useChatStore } from '../../store/chatStore';
import { useConnectionStore } from '../../store/connectionStore';
import { API_BASE_URL, queryApi } from '../../api/client';
import { UserMessage } from './UserMessage';
import { AssistantMessage } from './AssistantMessage';
import { InputBar } from './InputBar';
import { WelcomeScreen } from './WelcomeScreen';
import { toast } from 'sonner';
import type {
  AgentStep,
  ChartSuggestion,
  ExecutionResult,
  Message,
  QueryContext,
  QueryHistoryDetail,
  QueryRequest,
} from '../../types/query';
import { normalizeTableSchema } from '../../api/client';

type BackendQueryResult = Record<string, any>;

function getDatabaseDisplayLabel(database?: string | null) {
  if (!database) {
    return '';
  }
  const normalized = database.replace(/\\/g, '/');
  const segments = normalized.split('/');
  return segments[segments.length - 1] || database;
}

function normalizeExecutionResult(payload: BackendQueryResult | undefined | null): ExecutionResult | undefined {
  if (!payload) {
    return undefined;
  }
  const result = (payload.result ?? payload.results ?? payload.execution_result) as BackendQueryResult | undefined;
  if (!result) {
    return undefined;
  }

  const pagination = result.pagination || (result.total_row_count != null || result.row_count != null
    ? {
        page_number: 1,
        page_size: Array.isArray(result.rows) ? result.rows.length || 0 : 0,
        offset: 0,
        has_more: Boolean(result.pagination?.has_more),
        total_row_count: result.total_row_count ?? null,
        applied_limit: result.pagination?.applied_limit ?? null,
      }
    : undefined);

  return {
    columns: Array.isArray(result.columns) ? result.columns : [],
    rows: Array.isArray(result.rows) ? result.rows : [],
    row_count: Number(result.row_count ?? result.rows?.length ?? 0),
    truncated: Boolean(result.truncated),
    db_latency_ms: typeof result.db_latency_ms === 'number' ? result.db_latency_ms : undefined,
    total_row_count: typeof result.total_row_count === 'number' ? result.total_row_count : undefined,
    pagination,
    source_sql: result.source_sql ?? payload.sql ?? undefined,
    execution_time_ms:
      typeof result.execution_time_ms === 'number'
        ? result.execution_time_ms
        : typeof result.db_latency_ms === 'number'
          ? result.db_latency_ms
          : undefined,
  };
}

function normalizeChartSuggestion(payload: BackendQueryResult | undefined | null): ChartSuggestion | undefined {
  if (!payload) {
    return undefined;
  }
  const chart = payload.chart ?? payload.chart_suggestion;
  if (!chart) {
    return undefined;
  }
  return {
    chart_type: chart.chart_type ?? chart.type ?? 'table',
    x_axis: chart.x_axis ?? chart.xAxis,
    y_axis: chart.y_axis ?? chart.yAxis,
    reason: chart.reason,
  };
}

function normalizeStep(payload: BackendQueryResult | AgentStep): AgentStep {
  const step = payload as BackendQueryResult;
  return {
    step_type: (step.step_type ?? step.type ?? 'rewrite') as AgentStep['step_type'],
    content: String(step.content ?? ''),
    timestamp: String(step.timestamp ?? new Date().toISOString()),
    metadata: (step.metadata ?? {}) as Record<string, any>,
    status:
      step.status ??
      ((step.step_type ?? step.type) === 'done'
        ? 'success'
        : step.error_type || step.metadata?.error_type
          ? 'error'
          : 'loading'),
    error_type: step.error_type ?? step.metadata?.error_type,
    error_suggestion: step.error_suggestion ?? step.metadata?.error_suggestion,
    retry_count: step.retry_count ?? step.metadata?.retry_count,
    max_retries: step.max_retries ?? step.metadata?.max_retries,
  };
}

function buildAssistantUpdate(payload: BackendQueryResult): Partial<Message> {
  const normalizedResult = normalizeExecutionResult(payload);
  const normalizedChart = normalizeChartSuggestion(payload);
  const normalizedSteps = Array.isArray(payload.steps) ? payload.steps.map(normalizeStep) : undefined;
  const status = (payload.status ?? payload.result?.status ?? 'success') as Message['status'];

  return {
    content: payload.question ?? payload.rewritten_query ?? '',
    sql: payload.sql ?? payload.rewritten_query ?? '',
    generated_sql: payload.sql ?? payload.rewritten_query ?? '',
    retrieved_tables: Array.isArray(payload.retrieved_tables)
      ? payload.retrieved_tables.map(normalizeTableSchema)
      : [],
    telemetry: payload.telemetry ?? undefined,
    results: normalizedResult,
    execution_result: normalizedResult,
    chart_suggestion: normalizedChart,
    chart: normalizedChart,
    summary: payload.summary ?? undefined,
    steps: normalizedSteps,
    agent_steps: normalizedSteps,
    status: status === 'failed' ? 'error' : 'success',
    error_message: payload.error_message,
    query_id: payload.query_id ?? payload.id,
    context_source_query_id: payload.context_source_query_id,
    retry_count: payload.retry_count ?? payload.sql_attempts,
  };
}

function buildStepFromEvent(payload: BackendQueryResult, fallbackStatus: AgentStep['status'] = 'loading'): AgentStep {
  const step = normalizeStep(payload);
  return {
    ...step,
    status: step.step_type === 'done' ? 'success' : step.status ?? fallbackStatus,
  };
}

async function readSseStream(
  response: Response,
  handlers: {
    onStep: (step: AgentStep) => void;
    onDone: (step: AgentStep) => void;
    onResult: (payload: BackendQueryResult) => void;
    onError: (message: string, payload?: BackendQueryResult) => void;
  },
) {
  const reader = response.body?.getReader();
  if (!reader) {
    throw new Error('无法读取流式响应');
  }

  const decoder = new TextDecoder();
  let buffer = '';

  const dispatchChunk = (chunk: string) => {
    if (!chunk.trim()) {
      return;
    }

    let eventName = 'message';
    const dataLines: string[] = [];
    for (const line of chunk.split(/\r?\n/)) {
      if (line.startsWith('event:')) {
        eventName = line.slice(6).trim();
      } else if (line.startsWith('data:')) {
        dataLines.push(line.slice(5).trim());
      }
    }

    if (dataLines.length === 0) {
      return;
    }

    const dataText = dataLines.join('\n');
    let payload: BackendQueryResult;
    try {
      payload = JSON.parse(dataText) as BackendQueryResult;
    } catch {
      return;
    }

    if (eventName === 'step') {
      handlers.onStep(buildStepFromEvent(payload));
      return;
    }
    if (eventName === 'done') {
      handlers.onDone(buildStepFromEvent(payload, 'success'));
      return;
    }
    if (eventName === 'result') {
      handlers.onResult(payload);
      return;
    }
    if (eventName === 'error') {
      handlers.onError(String(payload.message ?? '查询失败'), payload);
    }
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
    const parts = buffer.split(/\n\n/);
    buffer = parts.pop() || '';

    for (const part of parts) {
      dispatchChunk(part);
    }
  }

  if (buffer.trim()) {
    dispatchChunk(buffer);
  }
}

export function ChatPanel() {
  const navigate = useNavigate();
  const location = useLocation();
  const {
    messages,
    isStreaming,
    addUserMessage,
    addAssistantMessage,
    updateAssistantMessage,
    addStep,
    setStreaming,
    clearCurrentSteps,
    clearHistory,
    setActiveConnection,
  } = useChatStore();
  const { activeConnectionId, connections, setActiveConnection: setStoreActiveConnection } = useConnectionStore();
  const scrollRef = useRef<HTMLDivElement>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const activeConnection = connections.find((connection) => connection.id === activeConnectionId);
  const handledRouteStateRef = useRef(false);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isStreaming]);

  useEffect(() => {
    return () => {
      abortControllerRef.current?.abort();
      abortControllerRef.current = null;
    };
  }, []);

  useEffect(() => {
    const routeState = location.state as
      | {
          action?: 'view-history' | 'retry-history' | 'follow-up-history';
          history?: QueryHistoryDetail;
          histories?: QueryHistoryDetail[];
          context?: QueryContext;
          question?: string;
        }
      | undefined;

    if (!routeState?.action) {
      handledRouteStateRef.current = false;
      return;
    }

    if (handledRouteStateRef.current) {
      return;
    }

    if (routeState.action === 'view-history' && (routeState.histories?.length || routeState.history)) {
      const historyItems = routeState.histories?.length
        ? routeState.histories
        : routeState.history
          ? [routeState.history]
          : [];
      clearHistory();
      historyItems.forEach((history, index) => {
        const assistantMessageId = `msg_${Date.now()}_history_${index}`;
        addUserMessage(history.question || '历史查询');
        addAssistantMessage(assistantMessageId);
        updateAssistantMessage(assistantMessageId, {
          content: history.question || '历史查询',
          sql: history.sql || '',
          generated_sql: history.sql || '',
          retrieved_tables: history.retrieved_table_details || [],
          results: history.result || undefined,
          execution_result: history.result || undefined,
          chart_suggestion: history.chart || undefined,
          chart: history.chart || undefined,
          summary: (history as any).summary || undefined,
          steps: history.steps || [],
          agent_steps: history.steps || [],
          telemetry: history.telemetry || undefined,
          status: history.status === 'success' ? 'success' : history.status === 'failed' ? 'error' : 'success',
          error_message: history.error_message,
          query_id: history.id,
          retry_count: history.retry_count,
          context_source_query_id: history.context_source_query_id,
        });
      });
      handledRouteStateRef.current = true;
      navigate(location.pathname, { replace: true, state: null });
      return;
    }

    if (routeState.action === 'retry-history' && routeState.history) {
      handledRouteStateRef.current = true;
      navigate(location.pathname, { replace: true, state: null });
      void handleSend(routeState.history.question || '');
      return;
    }

    if (routeState.action === 'follow-up-history' && routeState.context && routeState.question) {
      handledRouteStateRef.current = true;
      navigate(location.pathname, { replace: true, state: null });
      void handleSend(routeState.question, {
        previousQueryId: routeState.context.history_id,
        followUpInstruction: routeState.question,
      });
    }
  }, [location.state, navigate, addAssistantMessage, addUserMessage, updateAssistantMessage, clearHistory]);

  const applyBackendResult = (assistantMessageId: string, payload: BackendQueryResult) => {
    updateAssistantMessage(assistantMessageId, {
      ...buildAssistantUpdate(payload),
      status: payload.status === 'failed' ? 'error' : 'success',
    });
  };

  const executeStreamQuery = async (question: string, assistantMessageId: string, requestOverrides?: Partial<QueryRequest>) => {
    if (!activeConnectionId) {
      return;
    }

    setStreaming(true);
    clearCurrentSteps();

    abortControllerRef.current?.abort();
    abortControllerRef.current = new AbortController();

    const request: QueryRequest = {
      connection_id: activeConnectionId,
      question,
      stream: true,
      ...requestOverrides,
    };

    let encounteredError = false;

    try {
      const response = await fetch(`${API_BASE_URL}/api/query/stream`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'text/event-stream',
        },
        body: JSON.stringify(request),
        signal: abortControllerRef.current.signal,
      });

      if (!response.ok) {
        const detail = await response.text();
        throw new Error(detail || `HTTP ${response.status}`);
      }

      await readSseStream(response, {
        onStep: (step) => {
          addStep(assistantMessageId, step);
        },
        onDone: (step) => {
          addStep(assistantMessageId, step);
        },
        onResult: (payload) => {
          applyBackendResult(assistantMessageId, payload);
        },
        onError: (message, payload) => {
          encounteredError = true;
          updateAssistantMessage(assistantMessageId, {
            status: 'error',
            error_message: payload?.message ? String(payload.message) : message,
          });
          toast.error(payload?.message ? String(payload.message) : message);
        },
      });

      if (!encounteredError) {
        updateAssistantMessage(assistantMessageId, { status: 'success' });
      }
    } catch (error: any) {
      if (error?.name === 'AbortError') {
        toast.info('查询已取消');
      } else {
        updateAssistantMessage(assistantMessageId, {
          status: 'error',
          error_message: error?.message || '查询失败',
        });
        toast.error(error?.message || '查询失败');
      }
    } finally {
      setStreaming(false);
      abortControllerRef.current = null;
    }
  };

  const executeSqlQuery = async (sql: string, assistantMessageId: string, pageNumber = 1, pageSize = 5) => {
    if (!activeConnectionId) {
      return;
    }

    setStreaming(true);
    abortControllerRef.current?.abort();

    try {
      const response = await queryApi.executeSQL({
        connection_id: activeConnectionId,
        sql,
        page_number: pageNumber,
        page_size: pageSize,
        include_total_count: true,
      });
      const payload = response as unknown as BackendQueryResult;
      updateAssistantMessage(assistantMessageId, {
        ...buildAssistantUpdate(payload),
        content: 'SQL 执行结果',
        status: payload.status === 'failed' ? 'error' : 'success',
      });
    } catch (error: any) {
      updateAssistantMessage(assistantMessageId, {
        status: 'error',
        error_message: error?.message || 'SQL 执行失败',
      });
      toast.error(error?.message || 'SQL 执行失败');
    } finally {
      setStreaming(false);
    }
  };

  const handleSend = async (
    content: string,
    options?: {
      previousQueryId?: string;
      followUpInstruction?: string;
    },
  ) => {
    if (!activeConnectionId) {
      return;
    }

    addUserMessage(content);
    const assistantMessageId = `msg_${Date.now()}_assistant`;
    addAssistantMessage(assistantMessageId);
    await executeStreamQuery(content, assistantMessageId, {
      previous_query_id: options?.previousQueryId,
      follow_up_instruction: options?.followUpInstruction,
      page_size: 5,
      include_total_count: true,
    });
  };

  const handleExecuteSQL = async (sql: string) => {
    if (!activeConnectionId) {
      return;
    }

    addUserMessage(`执行 SQL:\n${sql}`);
    const assistantMessageId = `msg_${Date.now()}_assistant`;
    addAssistantMessage(assistantMessageId);
    await executeSqlQuery(sql, assistantMessageId, 1, 5);
  };

  const handlePageChange = async (messageId: string, sql: string, pageNumber: number, pageSize: number) => {
    await executeSqlQuery(sql, messageId, pageNumber, pageSize);
  };

  const handleSwitchConnection = (connectionId: string) => {
    if (!connectionId || connectionId === activeConnectionId) {
      return;
    }

    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
    setStreaming(false);
    clearCurrentSteps();
    clearHistory();
    setStoreActiveConnection(connectionId);
    setActiveConnection(connectionId);

    const nextConnection = connections.find((connection) => connection.id === connectionId);
    toast.success(
      nextConnection
        ? `已切换到 ${nextConnection.name}，开始新的聊天`
        : '已切换数据库，开始新的聊天',
    );
  };

  if (!activeConnectionId) {
    return (
      <div className="flex h-full min-h-0 flex-col">
        <div className="flex-1 flex items-center justify-center p-8">
          <Alert className="max-w-md">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription className="mt-2">
              <p className="mb-4">请先在左侧创建并选择一个数据库连接</p>
              <p className="text-xs text-muted-foreground">
                💡 提示：点击左侧导航栏的“连接管理”，创建新的数据库连接或选择已有连接
              </p>
            </AlertDescription>
          </Alert>
        </div>
      </div>
    );
  }

  if (activeConnection && !activeConnection.is_online) {
    return (
      <div className="flex h-full min-h-0 flex-col">
        <div className="flex-1 flex items-center justify-center p-8">
          <Alert variant="destructive" className="max-w-md">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription className="mt-2">
              <p className="mb-2 font-medium">当前连接已离线</p>
              <p className="text-sm mb-4">连接: {activeConnection.name}</p>
              <p className="text-xs">请检查数据库连接配置或选择其他在线连接</p>
            </AlertDescription>
          </Alert>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      {activeConnection && (
        <div className="border-b border-zinc-200 bg-white px-6 py-3">
          <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="flex items-center gap-2 text-sm font-semibold text-black">
                <Sparkles className="h-4 w-4 text-zinc-700" />
                对话式 SQL 工作台
              </div>
              <p className="mt-1 text-xs text-zinc-500">当前会话绑定所选数据库，直接提问即可。</p>
            </div>
            <div className="flex flex-wrap items-center gap-2 text-xs text-zinc-600">
              <div className="inline-flex items-center gap-2 rounded-full border border-zinc-200 bg-zinc-50 px-2 py-1.5">
                <Database className="ml-1 h-3.5 w-3.5" />
                <Select value={activeConnection.id} onValueChange={handleSwitchConnection}>
                  <SelectTrigger className="h-8 min-w-[240px] border-0 bg-transparent px-2 py-0 text-xs text-black shadow-none focus-visible:ring-0">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="border-zinc-200 bg-white text-black">
                    {connections.map((connection) => {
                      const databaseLabel =
                        connection.db_type === 'sqlite'
                          ? getDatabaseDisplayLabel(connection.file_path || connection.database)
                          : connection.database;
                      return (
                        <SelectItem key={connection.id} value={connection.id} className="text-black">
                          {connection.name} / {databaseLabel}
                        </SelectItem>
                      );
                    })}
                  </SelectContent>
                </Select>
              </div>
              <div className="inline-flex items-center gap-2 rounded-full border border-zinc-200 bg-zinc-50 px-3 py-1.5">
                <Workflow className="h-3.5 w-3.5" />
                <span>Schema-aware agent</span>
              </div>
              <div className="inline-flex items-center gap-2 rounded-full border border-zinc-200 bg-zinc-50 px-3 py-1.5">
                <Waves className="h-3.5 w-3.5" />
                <span className={`h-2 w-2 rounded-full ${activeConnection.is_online ? 'bg-black' : 'bg-zinc-400'}`} />
                <span>{isStreaming ? '处理中' : activeConnection.is_online ? '在线' : '离线'}</span>
              </div>
            </div>
          </div>
        </div>
      )}

      <ScrollArea className="min-h-0 flex-1">
        <div className="p-4 md:p-6">
          <div className="mx-auto max-w-6xl">
            {messages.length === 0 ? (
              <div className="mt-4 md:mt-5">
                <WelcomeScreen onSelectQuery={handleSend} />
              </div>
            ) : (
              <div className="px-1 py-2 md:px-2">
                {messages.map((message) =>
                  message.type === 'user' ? (
                    <UserMessage key={message.id} content={message.content} />
                  ) : (
                    <AssistantMessage
                      key={message.id}
                      message={message}
                      onExecuteSQL={handleExecuteSQL}
                      onPageChange={(pageNumber, pageSize) => handlePageChange(message.id, message.sql || message.generated_sql || '', pageNumber, pageSize)}
                    />
                  ),
                )}
                <div ref={scrollRef} />
              </div>
            )}
          </div>
        </div>
      </ScrollArea>

      <InputBar onSend={handleSend} disabled={!activeConnection?.is_online} isLoading={isStreaming} />
    </div>
  );
}
