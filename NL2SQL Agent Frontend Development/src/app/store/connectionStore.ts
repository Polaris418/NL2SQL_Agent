import { create } from 'zustand';
import apiClient, { connectionsApi, ragApi } from '../api/client';
import type { Connection, ConnectionConfig, SchemaCacheEntry } from '../types/connection';
import type { RAGIndexHealthDetail, RAGIndexState, RAGTelemetryDashboard } from '../types/rag';
import { toast } from 'sonner';
import { useChatStore } from './chatStore';

const autoBootstrapRagConnections = new Set<string>();

interface ConnectionStore {
  // 状态
  connections: Connection[];
  activeConnectionId: string | null;
  schemaCache: Record<string, SchemaCacheEntry>;
  ragIndexStates: Record<string, RAGIndexState>;
  ragHealthDetails: Record<string, RAGIndexHealthDetail>;
  ragTelemetryDashboard: RAGTelemetryDashboard | null;
  isLoading: boolean;
  isRagLoading: boolean;

  // Actions
  fetchConnections: () => Promise<void>;
  fetchRagStates: () => Promise<void>;
  fetchRagHealth: (id: string) => Promise<RAGIndexHealthDetail | null>;
  rebuildRagIndex: (id: string, forceFullRebuild?: boolean) => Promise<void>;
  addConnection: (config: ConnectionConfig) => Promise<Connection | null>;
  deleteConnection: (id: string) => Promise<void>;
  setActiveConnection: (id: string) => void;
  syncSchema: (id: string) => Promise<void>;
  testConnection: (id: string) => Promise<void>;
  getSchema: (id: string) => Promise<SchemaCacheEntry | null>;
}

export const useConnectionStore = create<ConnectionStore>((set, get) => ({
  connections: [],
  activeConnectionId: null,
  schemaCache: {},
  ragIndexStates: {},
  ragHealthDetails: {},
  ragTelemetryDashboard: null,
  isLoading: false,
  isRagLoading: false,

  fetchConnections: async () => {
    set({ isLoading: true });
    try {
      const connections = await connectionsApi.list();
      set({ connections });
      
      // 如果当前活动连接不存在于最新列表，或者还没有活动连接，则自动选择一个可用连接
      const { activeConnectionId } = get();
      const activeExists = activeConnectionId ? connections.some((c) => c.id === activeConnectionId) : false;
      if ((!activeConnectionId || !activeExists) && connections.length > 0) {
        const onlineConnection = connections.find((c) => c.is_online);
        const newActiveId = onlineConnection?.id || connections[0].id;
        set({ activeConnectionId: newActiveId });
        // 同步到 chatStore
        useChatStore.getState().setActiveConnection(newActiveId);
        void get().fetchRagHealth(newActiveId);
      } else if (connections.length === 0 && activeConnectionId) {
        set({ activeConnectionId: null });
        useChatStore.getState().setActiveConnection(null);
      }
      void get().fetchRagStates();
    } catch (error) {
      toast.error('获取连接列表失败');
      console.error('Error fetching connections:', error);
    } finally {
      set({ isLoading: false });
    }
  },

  fetchRagStates: async () => {
    set({ isRagLoading: true });
    try {
      const [states, dashboard] = await Promise.all([
        ragApi.listIndexStatus(),
        ragApi.telemetryDashboard().catch(() => null),
      ]);
      const ragIndexStates = states.reduce<Record<string, RAGIndexState>>((acc, state) => {
        acc[state.connection_id] = state;
        return acc;
      }, {});
      set({
        ragIndexStates,
        ragTelemetryDashboard: dashboard,
      });

      const { connections } = get();
      const bootstrapTargets = connections.filter((connection) => {
        const state = ragIndexStates[connection.id];
        if (!connection.is_online) {
          return false;
        }
        if (autoBootstrapRagConnections.has(connection.id)) {
          return false;
        }
        return !state || state.index_status === 'pending';
      });

      for (const connection of bootstrapTargets) {
        autoBootstrapRagConnections.add(connection.id);
        ragApi
          .rebuildIndex(connection.id, { force_full_rebuild: false })
          .then((nextState) => {
            set((state) => ({
              ragIndexStates: { ...state.ragIndexStates, [connection.id]: nextState },
            }));
            void get().fetchRagHealth(connection.id);
          })
          .catch((error) => {
            console.error('Error auto-bootstrapping RAG index:', error);
            autoBootstrapRagConnections.delete(connection.id);
          });
      }

      const { activeConnectionId } = get();
      if (activeConnectionId) {
        void get().fetchRagHealth(activeConnectionId);
      }
    } catch (error) {
      console.error('Error fetching RAG states:', error);
    } finally {
      set({ isRagLoading: false });
    }
  },

  fetchRagHealth: async (id) => {
    try {
      const detail = await ragApi.getIndexHealth(id);
      set((state) => ({
        ragHealthDetails: { ...state.ragHealthDetails, [id]: detail },
        ragIndexStates: { ...state.ragIndexStates, [id]: detail.state },
      }));
      return detail;
    } catch (error) {
      console.error('Error fetching RAG health:', error);
      return null;
    }
  },

  rebuildRagIndex: async (id, forceFullRebuild = false) => {
    try {
      toast.loading('正在预构建 RAG 检索增强...', { id: `rag-rebuild-${id}` });
      const nextState = await ragApi.rebuildIndex(id, { force_full_rebuild: forceFullRebuild });
      set((state) => ({
        ragIndexStates: { ...state.ragIndexStates, [id]: nextState },
      }));
      await get().fetchRagHealth(id);
      toast.success('已开始重建当前数据库的 RAG 检索增强', { id: `rag-rebuild-${id}` });
    } catch (error: any) {
      toast.error(`RAG 重建失败: ${error.message}`, { id: `rag-rebuild-${id}` });
      console.error('Error rebuilding RAG index:', error);
    }
  },

  addConnection: async (config) => {
    set({ isLoading: true });
    try {
      const connection = await connectionsApi.create(config);
      set((state) => ({
        connections: [...state.connections, connection],
        activeConnectionId: connection.id,
      }));
      
      // 同步到 chatStore
      useChatStore.getState().setActiveConnection(connection.id);
      
      toast.success('连接创建成功');
      
      // 自动同步 Schema
      if (connection.is_online) {
        void get().syncSchema(connection.id);
      }
      void get().fetchRagStates();
      
      return connection;
    } catch (error: any) {
      toast.error(`创建连接失败: ${error.message}`);
      console.error('Error creating connection:', error);
      return null;
    } finally {
      set({ isLoading: false });
    }
  },

  deleteConnection: async (id) => {
    try {
      await connectionsApi.delete(id);
      set((state) => {
        const newSchemaCache = { ...state.schemaCache };
        delete newSchemaCache[id];
        
        const remainingConnections = state.connections.filter((c) => c.id !== id);
        const nextActiveId =
          state.activeConnectionId === id
            ? remainingConnections.find((c) => c.is_online)?.id || remainingConnections[0]?.id || null
            : state.activeConnectionId;
        
        // 同步到 chatStore
        useChatStore.getState().setActiveConnection(nextActiveId);
        
        return {
          connections: remainingConnections,
          activeConnectionId: nextActiveId,
          schemaCache: newSchemaCache,
        };
      });
      toast.success('连接已删除');
    } catch (error: any) {
      toast.error(`删除连接失败: ${error.message}`);
      console.error('Error deleting connection:', error);
    }
  },

  setActiveConnection: (id) => {
    set({ activeConnectionId: id });
    // 同步到 chatStore
    useChatStore.getState().setActiveConnection(id);
    void get().fetchRagHealth(id);
  },

  syncSchema: async (id) => {
    try {
      toast.loading('正在同步 Schema...', { id: 'sync-schema' });
      await connectionsApi.sync(id);
      
      // 重新获取Schema
      const schemaEntry = await connectionsApi.getSchema(id);
      set((state) => ({
        schemaCache: { ...state.schemaCache, [id]: schemaEntry },
      }));
      void get().fetchRagStates();
      void get().fetchRagHealth(id);
      
      toast.success(`Schema 同步成功，共 ${schemaEntry.tables.length} 张表`, { id: 'sync-schema' });
    } catch (error: any) {
      toast.error(`Schema 同步失败: ${error.message}`, { id: 'sync-schema' });
      console.error('Error syncing schema:', error);
    }
  },

  testConnection: async (id) => {
    try {
      toast.loading('正在测试连接...', { id: `test-connection-${id}` });
      const response = await apiClient.post(`/api/connections/${id}/test`);
      const payload = response.data as {
        success?: boolean;
        message?: string;
        latency_ms?: number;
      };
      await get().fetchConnections();
      toast.success(
        payload.success
          ? `连接测试成功${payload.latency_ms ? ` (${Math.round(payload.latency_ms)}ms)` : ''}`
          : payload.message || '连接测试完成',
        { id: `test-connection-${id}` }
      );
    } catch (error: any) {
      toast.error(`连接测试失败: ${error.message}`, { id: `test-connection-${id}` });
      console.error('Error testing connection:', error);
      await get().fetchConnections();
    }
  },

  getSchema: async (id) => {
    const { schemaCache } = get();
    
    // 如果缓存中有，直接返回
    if (schemaCache[id]) {
      return schemaCache[id];
    }

    // 否则从服务器获取
    try {
      const schemaEntry = await connectionsApi.getSchema(id);
      set((state) => ({
        schemaCache: { ...state.schemaCache, [id]: schemaEntry },
      }));
      return schemaEntry;
    } catch (error) {
      console.error('Error getting schema:', error);
      return null;
    }
  },
}));
