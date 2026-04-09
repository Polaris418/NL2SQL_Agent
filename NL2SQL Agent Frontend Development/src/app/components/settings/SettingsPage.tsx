import React, { useState } from 'react';
import { Settings, Cpu, FileText, MessageSquare, Book, Database, ChevronRight } from 'lucide-react';
import { useNavigate } from 'react-router';
import { Breadcrumb, breadcrumbConfigs } from '../ui/breadcrumb';

interface SettingItem {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
  path: string;
  category: string;
}

export const SettingsPage: React.FC = () => {
  const navigate = useNavigate();
  const [selectedCategory, setSelectedCategory] = useState<string>('all');

  const settingItems: SettingItem[] = [
    {
      id: 'llm-settings',
      title: 'API 配置',
      description: '配置 LLM API Key、模型、温度等参数',
      icon: <Cpu className="w-6 h-6" />,
      path: '/settings/llm',
      category: 'system',
    },
    {
      id: 'prompts',
      title: 'Prompt 配置',
      description: '自定义 SQL 生成、查询改写、错误分析等 Prompt',
      icon: <FileText className="w-6 h-6" />,
      path: '/prompts',
      category: 'system',
    },
    {
      id: 'assistant-config',
      title: 'AI 助手配置',
      description: '配置 AI 助手的 LLM API、模型、系统提示词',
      icon: <MessageSquare className="w-6 h-6" />,
      path: '/assistant-config',
      category: 'assistant',
    },
    {
      id: 'knowledge-base',
      title: '知识库管理',
      description: '管理结构化知识条目和文档，支持 RAG 智能检索',
      icon: <Book className="w-6 h-6" />,
      path: '/knowledge-base',
      category: 'assistant',
    },
  ];

  const categories = [
    { id: 'all', name: '全部' },
    { id: 'system', name: '系统配置' },
    { id: 'assistant', name: 'AI 助手' },
  ];

  const filteredItems = selectedCategory === 'all'
    ? settingItems
    : settingItems.filter(item => item.category === selectedCategory);

  return (
    <div className="h-full overflow-auto bg-gray-50">
      <div className="max-w-6xl mx-auto p-6">
        {/* 面包屑导航 - 设置首页不显示返回按钮 */}
        <Breadcrumb items={breadcrumbConfigs.settings} className="mb-4" showBackButton={false} />
        
        {/* 标题 */}
        <div className="mb-6">
          <div className="flex items-center gap-3 mb-2">
            <Settings className="w-8 h-8 text-blue-500" />
            <h1 className="text-2xl font-bold text-gray-800">设置</h1>
          </div>
          <p className="text-gray-600">
            管理系统配置、AI 助手和知识库
          </p>
        </div>

        {/* 分类标签 */}
        <div className="flex gap-2 mb-6">
          {categories.map((category) => (
            <button
              key={category.id}
              onClick={() => setSelectedCategory(category.id)}
              className={`px-4 py-2 rounded-lg transition-colors ${
                selectedCategory === category.id
                  ? 'bg-blue-500 text-white'
                  : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-200'
              }`}
            >
              {category.name}
            </button>
          ))}
        </div>

        {/* 设置项列表 */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {filteredItems.map((item) => (
            <button
              key={item.id}
              onClick={() => navigate(item.path)}
              className="bg-white rounded-lg border border-gray-200 p-6 hover:border-blue-300 hover:shadow-md transition-all text-left group"
            >
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-4 flex-1">
                  <div className="p-3 bg-blue-50 rounded-lg text-blue-500 group-hover:bg-blue-100 transition-colors">
                    {item.icon}
                  </div>
                  <div className="flex-1">
                    <h3 className="text-lg font-semibold text-gray-800 mb-1">
                      {item.title}
                    </h3>
                    <p className="text-sm text-gray-600">
                      {item.description}
                    </p>
                  </div>
                </div>
                <ChevronRight className="w-5 h-5 text-gray-400 group-hover:text-blue-500 transition-colors" />
              </div>
            </button>
          ))}
        </div>

        {/* 提示信息 */}
        <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
          <div className="text-sm text-blue-800">
            <p className="font-medium mb-2">💡 配置说明</p>
            <ul className="space-y-1 text-blue-700">
              <li>• <strong>API 配置</strong>：配置主系统的 LLM API，用于 SQL 生成和查询</li>
              <li>• <strong>Prompt 配置</strong>：自定义 AI 的行为和生成策略</li>
              <li>• <strong>AI 助手配置</strong>：配置悬浮球助手的独立 LLM API</li>
              <li>• <strong>知识库管理</strong>：管理结构化知识和文档，支持 RAG 检索</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
};
