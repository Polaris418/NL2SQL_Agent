import React, { useState, useEffect } from 'react';
import { Save, Plus, Trash2, Edit2, X, Book, Search, RefreshCw, AlertCircle, CheckCircle } from 'lucide-react';
import { assistantApi } from '../../api/client';
import { Breadcrumb, breadcrumbConfigs } from '../ui/breadcrumb';

interface KnowledgeItem {
  id: string;
  title: string;
  content: string;
  category: string;
  updated_at: string;
}

interface KnowledgeBasePageProps {
  hideHeader?: boolean;
}

export const KnowledgeBasePage: React.FC<KnowledgeBasePageProps> = ({ hideHeader = false }) => {
  const [items, setItems] = useState<KnowledgeItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedItem, setSelectedItem] = useState<KnowledgeItem | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [editForm, setEditForm] = useState<Partial<KnowledgeItem>>({});
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadKnowledge();
  }, []);

  const loadKnowledge = async () => {
    try {
      setLoading(true);
      const data = await assistantApi.getKnowledge();
      setItems(data);
    } catch (error) {
      console.error('Failed to load knowledge:', error);
      showMessage('error', '加载知识库失败');
    } finally {
      setLoading(false);
    }
  };

  const showMessage = (type: 'success' | 'error', text: string) => {
    setMessage({ type, text });
    setTimeout(() => setMessage(null), 3000);
  };

  const handleSelectItem = (item: KnowledgeItem) => {
    setSelectedItem(item);
    setIsEditing(false);
    setIsCreating(false);
  };

  const handleEdit = () => {
    if (!selectedItem) return;
    setEditForm(selectedItem);
    setIsEditing(true);
  };

  const handleCreate = () => {
    setEditForm({
      id: '',
      title: '',
      content: '',
      category: '基础',
      updated_at: new Date().toISOString(),
    });
    setIsCreating(true);
    setIsEditing(false);
    setSelectedItem(null);
  };

  const handleSave = async () => {
    if (!editForm.id || !editForm.title || !editForm.content) {
      showMessage('error', '请填写所有必填字段');
      return;
    }

    try {
      setSaving(true);
      if (isCreating) {
        await assistantApi.createKnowledgeItem(editForm as KnowledgeItem);
        showMessage('success', '知识条目已创建');
      } else {
        await assistantApi.updateKnowledgeItem(editForm.id, editForm as KnowledgeItem);
        showMessage('success', '知识条目已更新');
      }
      await loadKnowledge();
      setIsEditing(false);
      setIsCreating(false);
      setSelectedItem(editForm as KnowledgeItem);
    } catch (error: any) {
      console.error('Failed to save:', error);
      showMessage('error', error.response?.data?.detail || '保存失败');
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('确定要删除这个知识条目吗？')) return;

    try {
      await assistantApi.deleteKnowledgeItem(id);
      showMessage('success', '知识条目已删除');
      await loadKnowledge();
      if (selectedItem?.id === id) {
        setSelectedItem(null);
      }
    } catch (error) {
      console.error('Failed to delete:', error);
      showMessage('error', '删除失败');
    }
  };

  const handleCancel = () => {
    setIsEditing(false);
    setIsCreating(false);
    setEditForm({});
  };

  const filteredItems = items.filter(
    (item) =>
      item.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
      item.category.toLowerCase().includes(searchQuery.toLowerCase()) ||
      item.content.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const categories = Array.from(new Set(items.map((item) => item.category)));

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-gray-500">加载中...</div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col bg-gray-50">
      {/* 面包屑导航 */}
      {!hideHeader && (
        <div className="px-6 pt-6 pb-2">
          <Breadcrumb items={breadcrumbConfigs.knowledgeBase} />
        </div>
      )}
      
      <div className="flex-1 flex min-h-0">
        {/* 左侧列表 */}
        <div className="w-80 border-r border-gray-200 bg-white flex flex-col">
        {/* 头部 */}
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <Book className="w-5 h-5 text-blue-500" />
              <h2 className="font-semibold text-gray-800">知识库</h2>
            </div>
            <button
              onClick={handleCreate}
              className="p-2 text-blue-500 hover:bg-blue-50 rounded-lg transition-colors"
              title="添加知识"
            >
              <Plus className="w-5 h-5" />
            </button>
          </div>

          {/* 搜索框 */}
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="搜索知识..."
              className="w-full pl-10 pr-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
            />
          </div>
        </div>

        {/* 知识列表 */}
        <div className="flex-1 overflow-y-auto">
          {filteredItems.length === 0 ? (
            <div className="p-4 text-center text-gray-500 text-sm">
              {searchQuery ? '没有找到匹配的知识' : '暂无知识条目'}
            </div>
          ) : (
            <div className="p-2 space-y-1">
              {filteredItems.map((item) => (
                <div
                  key={item.id}
                  onClick={() => handleSelectItem(item)}
                  className={`p-3 rounded-lg cursor-pointer transition-colors ${
                    selectedItem?.id === item.id
                      ? 'bg-blue-50 border border-blue-200'
                      : 'hover:bg-gray-50 border border-transparent'
                  }`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex-1 min-w-0">
                      <div className="font-medium text-gray-800 text-sm truncate">
                        {item.title}
                      </div>
                      <div className="text-xs text-gray-500 mt-1">
                        {item.category}
                      </div>
                    </div>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDelete(item.id);
                      }}
                      className="p-1 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded transition-colors"
                      title="删除"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* 底部统计 */}
        <div className="p-4 border-t border-gray-200 bg-gray-50">
          <div className="text-xs text-gray-600">
            共 {items.length} 个知识条目
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {categories.length} 个分类
          </div>
        </div>
      </div>

      {/* 右侧详情/编辑 */}
      <div className="flex-1 flex flex-col">
        {/* 消息提示 */}
        {message && (
          <div
            className={`m-4 p-4 rounded-lg flex items-center gap-2 ${
              message.type === 'success'
                ? 'bg-green-50 text-green-800 border border-green-200'
                : 'bg-red-50 text-red-800 border border-red-200'
            }`}
          >
            {message.type === 'success' ? (
              <CheckCircle className="w-5 h-5" />
            ) : (
              <AlertCircle className="w-5 h-5" />
            )}
            <span>{message.text}</span>
          </div>
        )}

        {/* 内容区域 */}
        {!selectedItem && !isCreating ? (
          <div className="flex-1 flex items-center justify-center text-gray-400">
            <div className="text-center">
              <Book className="w-16 h-16 mx-auto mb-4 opacity-50" />
              <p>选择一个知识条目查看详情</p>
              <p className="text-sm mt-2">或点击左上角的 + 按钮添加新知识</p>
            </div>
          </div>
        ) : isEditing || isCreating ? (
          // 编辑模式
          <div className="flex-1 overflow-y-auto p-6">
            <div className="max-w-4xl mx-auto">
              <div className="flex items-center justify-between mb-6">
                <h2 className="text-xl font-bold text-gray-800">
                  {isCreating ? '添加知识' : '编辑知识'}
                </h2>
                <div className="flex gap-2">
                  <button
                    onClick={handleCancel}
                    className="px-4 py-2 text-gray-700 bg-gray-200 rounded-lg hover:bg-gray-300 transition-colors"
                  >
                    取消
                  </button>
                  <button
                    onClick={handleSave}
                    disabled={saving}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
                  >
                    {saving ? (
                      <>
                        <RefreshCw className="w-4 h-4 animate-spin" />
                        <span>保存中...</span>
                      </>
                    ) : (
                      <>
                        <Save className="w-4 h-4" />
                        <span>保存</span>
                      </>
                    )}
                  </button>
                </div>
              </div>

              <div className="space-y-4">
                {/* ID */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    ID <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={editForm.id || ''}
                    onChange={(e) => setEditForm({ ...editForm, id: e.target.value })}
                    disabled={!isCreating}
                    placeholder="knowledge-id"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:bg-gray-100"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    唯一标识符，创建后不可修改
                  </p>
                </div>

                {/* 标题 */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    标题 <span className="text-red-500">*</span>
                  </label>
                  <input
                    type="text"
                    value={editForm.title || ''}
                    onChange={(e) => setEditForm({ ...editForm, title: e.target.value })}
                    placeholder="知识标题"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>

                {/* 分类 */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    分类 <span className="text-red-500">*</span>
                  </label>
                  <select
                    value={editForm.category || ''}
                    onChange={(e) => setEditForm({ ...editForm, category: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="基础">基础</option>
                    <option value="使用指南">使用指南</option>
                    <option value="配置">配置</option>
                    <option value="高级">高级</option>
                    <option value="帮助">帮助</option>
                  </select>
                </div>

                {/* 内容 */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    内容 <span className="text-red-500">*</span>
                  </label>
                  <textarea
                    value={editForm.content || ''}
                    onChange={(e) => setEditForm({ ...editForm, content: e.target.value })}
                    rows={20}
                    placeholder="支持 Markdown 格式..."
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    支持 Markdown 格式，AI 助手会使用这些内容回答用户问题
                  </p>
                </div>
              </div>
            </div>
          </div>
        ) : (
          // 查看模式
          <div className="flex-1 overflow-y-auto p-6">
            <div className="max-w-4xl mx-auto">
              <div className="flex items-center justify-between mb-6">
                <div>
                  <h2 className="text-2xl font-bold text-gray-800">{selectedItem.title}</h2>
                  <div className="flex items-center gap-3 mt-2 text-sm text-gray-500">
                    <span className="px-2 py-1 bg-blue-100 text-blue-700 rounded">
                      {selectedItem.category}
                    </span>
                    <span>ID: {selectedItem.id}</span>
                    <span>
                      更新于: {new Date(selectedItem.updated_at).toLocaleString('zh-CN')}
                    </span>
                  </div>
                </div>
                <button
                  onClick={handleEdit}
                  className="flex items-center gap-2 px-4 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors"
                >
                  <Edit2 className="w-4 h-4" />
                  <span>编辑</span>
                </button>
              </div>

              <div className="prose prose-sm max-w-none bg-white rounded-lg border border-gray-200 p-6">
                <pre className="whitespace-pre-wrap font-sans text-gray-700 leading-relaxed">
                  {selectedItem.content}
                </pre>
              </div>
            </div>
          </div>
        )}
      </div>
      </div>
    </div>
  );
};
