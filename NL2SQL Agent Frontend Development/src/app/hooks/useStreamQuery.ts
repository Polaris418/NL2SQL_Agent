import { useCallback, useRef } from 'react';
import { useChatStore } from '../store/chatStore';
import type { QueryRequest, AgentStep } from '../types/query';
import { normalizeAgentStep, normalizeQueryResult } from '../api/client';
import { toast } from 'sonner';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

type SseFrame = {
  event: string;
  data: string;
};

const parseSseFrame = (frame: string): SseFrame | null => {
  const lines = frame.split(/\r?\n/);
  let event = 'message';
  const dataParts: string[] = [];

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    if (!line || line.startsWith(':')) {
      continue;
    }
    if (line.startsWith('event:')) {
      event = line.slice(6).trim() || event;
      continue;
    }
    if (line.startsWith('data:')) {
      dataParts.push(line.slice(5).trimStart());
    }
  }

  if (dataParts.length === 0) {
    return null;
  }

  return {
    event,
    data: dataParts.join('\n'),
  };
};

export const useStreamQuery = () => {
  const { addStep, updateAssistantMessage, setStreaming, clearCurrentSteps } = useChatStore();
  const abortControllerRef = useRef<AbortController | null>(null);

  const executeStreamQuery = useCallback(
    async (request: QueryRequest, messageId: string) => {
      setStreaming(true);
      clearCurrentSteps();
      abortControllerRef.current = new AbortController();

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
          const message = `HTTP error! status: ${response.status}`;
          throw new Error(message);
        }

        const reader = response.body?.getReader();
        if (!reader) {
          throw new Error('Response body is not readable');
        }

        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }

          buffer += decoder.decode(value, { stream: true });

          while (buffer.includes('\n\n')) {
            const splitIndex = buffer.indexOf('\n\n');
            const frameText = buffer.slice(0, splitIndex);
            buffer = buffer.slice(splitIndex + 2);

            const frame = parseSseFrame(frameText);
            if (!frame) {
              continue;
            }

            try {
              const payload = JSON.parse(frame.data);

              if (frame.event === 'step') {
                const step: AgentStep = normalizeAgentStep(payload);
                addStep(messageId, step);
                continue;
              }

              if (frame.event === 'result') {
                const normalized = normalizeQueryResult(payload);
                updateAssistantMessage(messageId, {
                  sql: normalized.sql,
                  generated_sql: normalized.sql,
                  results: normalized.result ?? undefined,
                  execution_result: normalized.result ?? undefined,
                  chart_suggestion: normalized.chart ?? undefined,
                  chart: normalized.chart ?? undefined,
                  steps: normalized.steps,
                  agent_steps: normalized.steps,
                  status: normalized.status === 'failed' ? 'error' : 'success',
                  error_message: normalized.error_message,
                  query_id: normalized.query_id,
                  context_source_query_id: normalized.context_source_query_id,
                  retry_count: normalized.retry_count,
                });
                continue;
              }

              if (frame.event === 'error') {
                const errorMessage = payload?.message || payload?.error || '查询失败';
                updateAssistantMessage(messageId, {
                  status: 'error',
                  error_message: errorMessage,
                });
                toast.error(`查询失败: ${errorMessage}`);
                continue;
              }
            } catch (error) {
              console.error('Error parsing SSE frame:', error, 'Raw frame:', frameText);
            }
          }
        }

        if (buffer.trim()) {
          const frame = parseSseFrame(buffer);
          if (frame) {
            try {
              const payload = JSON.parse(frame.data);
              if (frame.event === 'step') {
                addStep(messageId, normalizeAgentStep(payload));
              } else if (frame.event === 'result') {
                const normalized = normalizeQueryResult(payload);
                updateAssistantMessage(messageId, {
                  sql: normalized.sql,
                  generated_sql: normalized.sql,
                  results: normalized.result ?? undefined,
                  execution_result: normalized.result ?? undefined,
                  chart_suggestion: normalized.chart ?? undefined,
                  chart: normalized.chart ?? undefined,
                  steps: normalized.steps,
                  agent_steps: normalized.steps,
                  status: normalized.status === 'failed' ? 'error' : 'success',
                  error_message: normalized.error_message,
                  query_id: normalized.query_id,
                  context_source_query_id: normalized.context_source_query_id,
                  retry_count: normalized.retry_count,
                });
              } else if (frame.event === 'error') {
                const errorMessage = payload?.message || payload?.error || '查询失败';
                updateAssistantMessage(messageId, {
                  status: 'error',
                  error_message: errorMessage,
                });
              }
            } catch (error) {
              console.error('Error parsing trailing SSE frame:', error, 'Raw frame:', buffer);
            }
          }
        }
      } catch (error: any) {
        if (error?.name === 'AbortError') {
          toast.info('查询已取消');
        } else {
          updateAssistantMessage(messageId, {
            status: 'error',
            error_message: error?.message || '查询失败',
          });
          toast.error(`查询失败: ${error?.message || '未知错误'}`);
        }
      } finally {
        setStreaming(false);
        abortControllerRef.current = null;
      }
    },
    [addStep, updateAssistantMessage, setStreaming, clearCurrentSteps]
  );

  const cancelQuery = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
  }, []);

  return { executeStreamQuery, cancelQuery };
};
