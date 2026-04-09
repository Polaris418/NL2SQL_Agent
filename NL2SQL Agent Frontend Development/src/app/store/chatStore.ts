import { create } from 'zustand';
import type { Message, AgentStep, QueryRequest } from '../types/query';
import { queryApi } from '../api/client';
import { toast } from 'sonner';

interface ChatStore {
  // 状态
  messages: Message[];
  isStreaming: boolean;
  currentStreamSteps: AgentStep[];
  activeConnectionId: string | null;

  // Actions
  setActiveConnection: (connectionId: string | null) => void;
  addUserMessage: (content: string) => string;
  addAssistantMessage: (id: string) => void;
  updateAssistantMessage: (id: string, updates: Partial<Message>) => void;
  addStep: (messageId: string, step: AgentStep) => void;
  updateStep: (messageId: string, stepIndex: number, updates: Partial<AgentStep>) => void;
  setStreaming: (isStreaming: boolean) => void;
  clearHistory: () => void;
  clearCurrentSteps: () => void;
  
  // 高级功能
  sendMessage: (question: string, options?: {
    previousQueryId?: string;
    followUpInstruction?: string;
    pageNumber?: number;
    pageSize?: number;
  }) => Promise<string>;
  executeSQL: (sql: string, options?: {
    pageNumber?: number;
    pageSize?: number;
  }) => Promise<string>;
  retryQuery: (messageId: string) => Promise<void>;
  updatePagination: (messageId: string, pageNumber: number, pageSize: number) => Promise<void>;
}

export const useChatStore = create<ChatStore>((set, get) => ({
  messages: [],
  isStreaming: false,
  currentStreamSteps: [],
  activeConnectionId: null,

  setActiveConnection: (connectionId) => {
    set({ activeConnectionId: connectionId });
  },

  addUserMessage: (content) => {
    const id = `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const message: Message = {
      id,
      type: 'user',
      content,
      timestamp: new Date(),
    };
    set((state) => ({
      messages: [...state.messages, message],
    }));
    return id;
  },

  addAssistantMessage: (id) => {
    const message: Message = {
      id,
      type: 'assistant',
      content: '',
      timestamp: new Date(),
      status: 'loading',
      steps: [],
      agent_steps: [],
    };
    set((state) => ({
      messages: [...state.messages, message],
    }));
  },

  updateAssistantMessage: (id, updates) => {
    set((state) => ({
      messages: state.messages.map((msg) =>
        msg.id === id
          ? {
              ...msg,
              ...updates,
              steps:
                updates.status === 'success'
                  ? (updates.steps ?? msg.steps)?.map((step) =>
                      step.status === 'error' ? step : { ...step, status: 'success' }
                    )
                  : updates.steps ?? msg.steps,
              agent_steps:
                updates.status === 'success'
                  ? (updates.agent_steps ?? msg.agent_steps ?? msg.steps)?.map((step) =>
                      step.status === 'error' ? step : { ...step, status: 'success' }
                    )
                  : updates.agent_steps ?? msg.agent_steps,
            }
          : msg
      ),
    }));
  },

  addStep: (messageId, step) => {
    set((state) => ({
      messages: state.messages.map((msg) =>
        msg.id === messageId
          ? {
              ...msg,
              steps: [...(msg.steps || []), step],
              agent_steps: [...(msg.agent_steps || msg.steps || []), step],
            }
          : msg
      ),
      currentStreamSteps: [...state.currentStreamSteps, step],
    }));
  },

  updateStep: (messageId, stepIndex, updates) => {
    set((state) => ({
      messages: state.messages.map((msg) =>
        msg.id === messageId
          ? {
              ...msg,
              steps: msg.steps?.map((step, idx) =>
                idx === stepIndex ? { ...step, ...updates } : step
              ),
              agent_steps: msg.agent_steps?.map((step, idx) =>
                idx === stepIndex ? { ...step, ...updates } : step
              ),
            }
          : msg
      ),
    }));
  },

  setStreaming: (isStreaming) => {
    set({ isStreaming });
  },

  clearHistory: () => {
    set({ messages: [], currentStreamSteps: [] });
  },

  clearCurrentSteps: () => {
    set({ currentStreamSteps: [] });
  },

  // 发送自然语言查询
  sendMessage: async (question, options = {}) => {
    const state = get();
    
    if (!state.activeConnectionId) {
      toast.error('请先选择一个数据库连接');
      throw new Error('No active connection');
    }

    // 添加用户消息
    const userMessageId = state.addUserMessage(question);
    
    // 创建助手消息占位符
    const assistantMessageId = `msg_${Date.now()}_assistant`;
    state.addAssistantMessage(assistantMessageId);

    try {
      const request: QueryRequest = {
        connection_id: state.activeConnectionId,
        question,
        previous_query_id: options.previousQueryId,
        follow_up_instruction: options.followUpInstruction,
        page_number: options.pageNumber,
        page_size: options.pageSize,
      };

      const result = await queryApi.execute(request);
      const executionResult = result.result ?? result.results ?? undefined;
      const chartSuggestion = result.chart ?? result.chart_suggestion ?? undefined;
      const steps = result.steps ?? [];

      // 更新助手消息
      state.updateAssistantMessage(assistantMessageId, {
        content: question,
        sql: result.sql,
        generated_sql: result.sql,
        retrieved_tables: result.retrieved_tables ?? [],
        telemetry: result.telemetry,
        results: executionResult,
        execution_result: executionResult,
        chart_suggestion: chartSuggestion,
        chart: chartSuggestion,
        steps,
        agent_steps: steps,
        status: result.status === 'failed' ? 'error' : 'success',
        error_message: result.error_message,
        error_type: result.error_type,
        error_suggestion: result.error_suggestion,
        query_id: result.query_id,
        context_source_query_id: result.context_source_query_id,
        retry_count: result.retry_count,
      });

      if (result.status === 'success') {
        toast.success('查询成功');
      } else {
        toast.error(result.error_message || '查询失败');
      }

      return assistantMessageId;
    } catch (error: any) {
      state.updateAssistantMessage(assistantMessageId, {
        status: 'error',
        error_message: error.message || '查询失败',
        error_type: error.error_type,
        error_suggestion: error.error_suggestion,
      });
      toast.error(error.message || '查询失败');
      throw error;
    }
  },

  // 执行 SQL
  executeSQL: async (sql, options = {}) => {
    const state = get();
    
    if (!state.activeConnectionId) {
      toast.error('请先选择一个数据库连接');
      throw new Error('No active connection');
    }

    // 添加用户消息
    const userMessageId = state.addUserMessage(`执行 SQL:\n${sql}`);
    
    // 创建助手消息占位符
    const assistantMessageId = `msg_${Date.now()}_assistant`;
    state.addAssistantMessage(assistantMessageId);

    try {
      const result = await queryApi.executeSQL({
        connection_id: state.activeConnectionId,
        sql,
        page_number: options.pageNumber,
        page_size: options.pageSize,
      });
      const executionResult = result.result ?? result.results ?? undefined;
      const chartSuggestion = result.chart ?? result.chart_suggestion ?? undefined;
      const steps = result.steps ?? [];

      // 更新助手消息
      state.updateAssistantMessage(assistantMessageId, {
        content: 'SQL 执行结果',
        sql: result.sql,
        generated_sql: result.sql,
        retrieved_tables: result.retrieved_tables ?? [],
        telemetry: result.telemetry,
        results: executionResult,
        execution_result: executionResult,
        chart_suggestion: chartSuggestion,
        chart: chartSuggestion,
        steps,
        agent_steps: steps,
        status: result.status === 'failed' ? 'error' : 'success',
        error_message: result.error_message,
        error_type: result.error_type,
        error_suggestion: result.error_suggestion,
        query_id: result.query_id,
        retry_count: result.retry_count,
      });

      if (result.status === 'success') {
        toast.success('SQL 执行成功');
      } else {
        toast.error(result.error_message || 'SQL 执行失败');
      }

      return assistantMessageId;
    } catch (error: any) {
      state.updateAssistantMessage(assistantMessageId, {
        status: 'error',
        error_message: error.message || 'SQL 执行失败',
        error_type: error.error_type,
        error_suggestion: error.error_suggestion,
      });
      toast.error(error.message || 'SQL 执行失败');
      throw error;
    }
  },

  // 重试查询
  retryQuery: async (messageId) => {
    const state = get();
    const message = state.messages.find(m => m.id === messageId);
    
    if (!message || message.type !== 'user') {
      toast.error('无法重试该消息');
      return;
    }

    // 重新发送查询
    await state.sendMessage(message.content);
  },

  // 更新分页
  updatePagination: async (messageId, pageNumber, pageSize) => {
    const state = get();
    const message = state.messages.find(m => m.id === messageId);
    
    if (!message || !message.query_id || !state.activeConnectionId) {
      toast.error('无法更新分页');
      return;
    }

    try {
      // 如果有 SQL，重新执行
      if (message.sql) {
        const result = await queryApi.executeSQL({
          connection_id: state.activeConnectionId,
          sql: message.sql ?? message.generated_sql ?? '',
          page_number: pageNumber,
          page_size: pageSize,
        });

        state.updateAssistantMessage(messageId, {
          results: result.result ?? result.results ?? undefined,
          execution_result: result.result ?? result.results ?? undefined,
        });

        toast.success('分页更新成功');
      }
    } catch (error: any) {
      toast.error(error.message || '分页更新失败');
    }
  },
}));
