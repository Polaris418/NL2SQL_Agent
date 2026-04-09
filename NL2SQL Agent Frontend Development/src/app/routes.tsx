import { createBrowserRouter } from 'react-router';
import { Layout } from './components/Layout';
import { ChatPanel } from './components/chat/ChatPanel';
import { AnalyticsPanel } from './components/analytics/AnalyticsPanel';
import { HistoryPage } from './components/history/HistoryPage';
import { ConnectionsPage } from './components/connection/ConnectionsPage';
import { LLMSettingsPage } from './components/settings/LLMSettingsPage';
import { SettingsPage } from './components/settings/SettingsPage';
import { RAGStatusPage } from './components/rag/RAGStatusPage';
import { PromptsPage } from './components/prompts/PromptsPage';
import { AssistantConfigPage } from './components/assistant/AssistantConfigPage';
import { UnifiedKnowledgeBasePage } from './components/assistant/UnifiedKnowledgeBasePage';

export const router = createBrowserRouter([
  {
    path: '/',
    Component: Layout,
    children: [
      {
        index: true,
        Component: ChatPanel,
      },
      {
        path: 'chat',
        Component: ChatPanel,
      },
      {
        path: 'analytics',
        Component: AnalyticsPanel,
      },
      {
        path: 'connections',
        Component: ConnectionsPage,
      },
      {
        path: 'rag',
        Component: RAGStatusPage,
      },
      {
        path: 'settings',
        Component: SettingsPage,
      },
      {
        path: 'settings/llm',
        Component: LLMSettingsPage,
      },
      {
        path: 'history',
        Component: HistoryPage,
      },
      {
        path: 'prompts',
        Component: PromptsPage,
      },
      {
        path: 'assistant-config',
        Component: AssistantConfigPage,
      },
      {
        path: 'knowledge-base',
        Component: UnifiedKnowledgeBasePage,
      },
    ],
  },
]);
