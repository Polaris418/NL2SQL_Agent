import React, { useState, useEffect } from 'react';
import { Upload, FileText, Trash2, Search, Download, AlertCircle, CheckCircle, RefreshCw, File, Database } from 'lucide-react';
import { documentsApi } from '../../api/client';
import { Breadcrumb, breadcrumbConfigs } from '../ui/breadcrumb';

interface Document {
  document_id: string;
  filename: string;
  file_type: string;
  file_size: number;
  chunk_count: number;
  uploaded_at: string;
  metadata: {
    title?: string;
    category?: string;
    description?: string;
  };
}

interface CollectionStats {
  total_documents: number;
  total_chunks: number;
  total_size: number;
  vector_count: number;
  vector_dimension: number;
}

interface DocumentsPageProps {
  hideHeader?: boolean;
}

export const DocumentsPage: React.FC<DocumentsPageProps> = ({ hideHeader = false }) => {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [stats, setStats] = useState<CollectionStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploadForm, setUploadForm] = useState({
    title: '',
    category: '基础',
    description: '',
  });

  useEffect(() => {
    loadDocuments();
    loadStats();
  }, []);

  const loadDocuments = async () => {
    try {
      setLoading(true);
      const data = await documentsApi.list();
      setDocuments(data);
    } catch (error) {
      console.error('Failed to load documents:', error);
      showMessage('error', '加载文档列表失败');
    } finally {
      setLoading(false);
    }
  };

  const loadStats = async () => {
    try {
      const data = await documentsApi.getStats();
      setStats(data);
    } catch (error) {
      console.error('Failed to load stats:', error);
    }
  };

  const showMessage = (type: 'success' | 'error', text: string) => {
    setMessage({ type, text });
    setTimeout(() => setMessage(null), 3000);
  };

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      setSelectedFile(file);
      if (!uploadForm.title) {
        setUploadForm({ ...uploadForm, title: file.name.replace(/\.[^/.]+$/, '') });
      }
    }
  };

  const handleUpload = async () => {
    if (!selectedFile) {
      showMessage('error', '请选择文件');
      return;
    }

    try {
      setUploading(true);
      await documentsApi.upload(selectedFile, uploadForm);
      showMessage('success', '文档上传成功');
      setSelectedFile(null);
      setUploadForm({ title: '', category: '基础', description: '' });
      await loadDocuments();
      await loadStats();
    } catch (error: any) {
      console.error('Failed to upload:', error);
      showMessage('error', error.response?.data?.detail || '上传失败');
    } finally {
      setUploading(false);
    }
  };

  const handleDelete = async (documentId: string, filename: string) => {
    if (!confirm(`确定要删除文档 "${filename}" 吗？`)) return;

    try {
      await documentsApi.delete(documentId);
      showMessage('success', '文档已删除');
      await loadDocuments();
      await loadStats();
    } catch (error) {
      console.error('Failed to delete:', error);
      showMessage('error', '删除失败');
    }
  };

  const filteredDocuments = documents.filter(
    (doc) =>
      doc.filename.toLowerCase().includes(searchQuery.toLowerCase()) ||
      doc.metadata.title?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      doc.metadata.category?.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const formatFileSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString('zh-CN');
  };

  return (
    <div className="h-full overflow-auto bg-gray-50">
      <div className="max-w-7xl mx-auto p-6">
        {/* 面包屑导航 */}
        {!hideHeader && (
          <Breadcrumb items={breadcrumbConfigs.knowledgeBase} className="mb-4" />
        )}
        
        {/* 标题和统计 */}
        {!hideHeader && (
          <div className="mb-6">
            <div className="flex items-center justify-between mb-4">
              <div>
                <div className="flex items-center gap-3 mb-2">
                  <Database className="w-8 h-8 text-blue-500" />
                  <h1 className="text-2xl font-bold text-gray-800">文档知识库</h1>
                </div>
                <p className="text-gray-600">
                  上传文档到知识库，AI 助手将自动检索相关内容回答问题
                </p>
              </div>
            </div>
          </div>
        )}

        {/* 统计信息 */}
        <div className="mb-6">

          {/* 统计卡片 */}
          {stats && (
            <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
              <div className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="text-sm text-gray-500">文档数量</div>
                <div className="text-2xl font-bold text-gray-800 mt-1">{stats.total_documents}</div>
              </div>
              <div className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="text-sm text-gray-500">文本块数</div>
                <div className="text-2xl font-bold text-gray-800 mt-1">{stats.total_chunks}</div>
              </div>
              <div className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="text-sm text-gray-500">总大小</div>
                <div className="text-2xl font-bold text-gray-800 mt-1">{formatFileSize(stats.total_size)}</div>
              </div>
              <div className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="text-sm text-gray-500">向量数量</div>
                <div className="text-2xl font-bold text-gray-800 mt-1">{stats.vector_count}</div>
              </div>
              <div className="bg-white rounded-lg border border-gray-200 p-4">
                <div className="text-sm text-gray-500">向量维度</div>
                <div className="text-2xl font-bold text-gray-800 mt-1">{stats.vector_dimension}</div>
              </div>
            </div>
          )}
        </div>

        {/* 消息提示 */}
        {message && (
          <div
            className={`mb-4 p-4 rounded-lg flex items-center gap-2 ${
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

        {/* 上传区域 */}
        <div className="bg-white rounded-lg border border-gray-200 p-6 mb-6">
          <h2 className="text-lg font-semibold text-gray-800 mb-4">上传文档</h2>
          
          <div className="space-y-4">
            {/* 文件选择 */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                选择文件 <span className="text-red-500">*</span>
              </label>
              <div className="flex items-center gap-3">
                <label className="flex-1 flex items-center justify-center px-4 py-3 border-2 border-dashed border-gray-300 rounded-lg cursor-pointer hover:border-blue-500 transition-colors">
                  <input
                    type="file"
                    accept=".md,.markdown,.txt,.text"
                    onChange={handleFileSelect}
                    className="hidden"
                  />
                  <Upload className="w-5 h-5 text-gray-400 mr-2" />
                  <span className="text-sm text-gray-600">
                    {selectedFile ? selectedFile.name : '点击选择文件或拖拽到此处'}
                  </span>
                </label>
                {selectedFile && (
                  <button
                    onClick={() => setSelectedFile(null)}
                    className="px-4 py-2 text-gray-600 hover:text-gray-800"
                  >
                    清除
                  </button>
                )}
              </div>
              <p className="text-xs text-gray-500 mt-1">
                支持 Markdown (.md) 和纯文本 (.txt) 文件，最大 10MB
              </p>
            </div>

            {/* 标题 */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                标题
              </label>
              <input
                type="text"
                value={uploadForm.title}
                onChange={(e) => setUploadForm({ ...uploadForm, title: e.target.value })}
                placeholder="文档标题（留空使用文件名）"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {/* 分类 */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                分类
              </label>
              <select
                value={uploadForm.category}
                onChange={(e) => setUploadForm({ ...uploadForm, category: e.target.value })}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="基础">基础</option>
                <option value="使用指南">使用指南</option>
                <option value="配置">配置</option>
                <option value="高级">高级</option>
                <option value="帮助">帮助</option>
              </select>
            </div>

            {/* 描述 */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                描述
              </label>
              <textarea
                value={uploadForm.description}
                onChange={(e) => setUploadForm({ ...uploadForm, description: e.target.value })}
                placeholder="文档描述（可选）"
                rows={3}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {/* 上传按钮 */}
            <button
              onClick={handleUpload}
              disabled={!selectedFile || uploading}
              className="flex items-center gap-2 px-6 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-600 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors"
            >
              {uploading ? (
                <>
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  <span>上传中...</span>
                </>
              ) : (
                <>
                  <Upload className="w-4 h-4" />
                  <span>上传文档</span>
                </>
              )}
            </button>
          </div>
        </div>

        {/* 文档列表 */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold text-gray-800">文档列表</h2>
            
            {/* 搜索框 */}
            <div className="relative w-64">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="搜索文档..."
                className="w-full pl-10 pr-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
              />
            </div>
          </div>

          {loading ? (
            <div className="text-center py-8 text-gray-500">加载中...</div>
          ) : filteredDocuments.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              {searchQuery ? '没有找到匹配的文档' : '暂无文档，请上传文档到知识库'}
            </div>
          ) : (
            <div className="space-y-2">
              {filteredDocuments.map((doc) => (
                <div
                  key={doc.document_id}
                  className="flex items-center justify-between p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
                >
                  <div className="flex items-start gap-3 flex-1">
                    <FileText className="w-5 h-5 text-blue-500 mt-1" />
                    <div className="flex-1 min-w-0">
                      <div className="font-medium text-gray-800 truncate">
                        {doc.metadata.title || doc.filename}
                      </div>
                      <div className="flex items-center gap-3 mt-1 text-sm text-gray-500">
                        <span className="px-2 py-0.5 bg-blue-100 text-blue-700 rounded text-xs">
                          {doc.metadata.category || '未分类'}
                        </span>
                        <span>{formatFileSize(doc.file_size)}</span>
                        <span>{doc.chunk_count} 个块</span>
                        <span>{formatDate(doc.uploaded_at)}</span>
                      </div>
                      {doc.metadata.description && (
                        <div className="text-sm text-gray-600 mt-1 truncate">
                          {doc.metadata.description}
                        </div>
                      )}
                    </div>
                  </div>
                  
                  <button
                    onClick={() => handleDelete(doc.document_id, doc.metadata.title || doc.filename)}
                    className="p-2 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded transition-colors"
                    title="删除"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* 提示信息 */}
        <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
          <div className="flex items-start gap-2">
            <AlertCircle className="w-5 h-5 text-blue-600 mt-0.5" />
            <div className="text-sm text-blue-800">
              <p className="font-medium mb-1">使用说明：</p>
              <ul className="list-disc list-inside space-y-1">
                <li>上传的文档会自动分块并向量化，存储到知识库</li>
                <li>AI 助手对话时会自动检索相关文档内容</li>
                <li>支持 Markdown 和纯文本格式</li>
                <li>文档会被分成约 500 字符的块，便于精确检索</li>
                <li>删除文档会同时删除所有相关的向量数据</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
