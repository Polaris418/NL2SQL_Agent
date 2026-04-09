import React, { useState } from 'react';
import { Book, Database } from 'lucide-react';
import { Breadcrumb, breadcrumbConfigs } from '../ui/breadcrumb';
import { KnowledgeBasePage } from './KnowledgeBasePage';
import { DocumentsPage } from './DocumentsPage';

export const UnifiedKnowledgeBasePage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'structured' | 'documents'>('structured');

  return (
    <div className="h-full flex flex-col bg-gray-50">
      {/* 面包屑导航 */}
      <div className="px-6 pt-6 pb-2">
        <Breadcrumb items={breadcrumbConfigs.knowledgeBase} />
      </div>

      {/* 标题和标签页 */}
      <div className="px-6 pb-4">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-2xl font-bold text-gray-800 flex items-center gap-3">
              <Book className="w-7 h-7 text-blue-500" />
              知识库管理
            </h1>
            <p className="text-sm text-gray-600 mt-1">
              管理 AI 助手的知识库，包括结构化知识条目和文档
            </p>
          </div>
        </div>

        {/* 标签页切换 */}
        <div className="flex gap-2 border-b border-gray-200">
          <button
            onClick={() => setActiveTab('structured')}
            className={`px-4 py-2 text-sm font-medium transition-colors relative ${
              activeTab === 'structured'
                ? 'text-blue-600'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <div className="flex items-center gap-2">
              <Book className="w-4 h-4" />
              <span>结构化知识</span>
            </div>
            {activeTab === 'structured' && (
              <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600" />
            )}
          </button>
          <button
            onClick={() => setActiveTab('documents')}
            className={`px-4 py-2 text-sm font-medium transition-colors relative ${
              activeTab === 'documents'
                ? 'text-blue-600'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <div className="flex items-center gap-2">
              <Database className="w-4 h-4" />
              <span>文档知识库</span>
            </div>
            {activeTab === 'documents' && (
              <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600" />
            )}
          </button>
        </div>
      </div>

      {/* 内容区域 */}
      <div className="flex-1 min-h-0">
        {activeTab === 'structured' ? (
          <KnowledgeBasePage hideHeader={true} />
        ) : (
          <DocumentsPage hideHeader={true} />
        )}
      </div>
    </div>
  );
};
