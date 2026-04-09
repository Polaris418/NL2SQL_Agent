import React, { useEffect, useState } from 'react';
import {
  AlertCircle,
  CheckCircle,
  Eye,
  EyeOff,
  RefreshCw,
  Save,
  Settings,
  Wifi,
} from 'lucide-react';

import {
  assistantApi,
  type AssistantConfigPayload,
  type AssistantConfigTestResult,
} from '../../api/client';
import { Breadcrumb, breadcrumbConfigs } from '../ui/breadcrumb';

type BannerMessage = {
  type: 'success' | 'error';
  text: string;
};

const providerExamples: Record<string, string> = {
  anthropic: '例如: claude-3-5-sonnet-latest, claude-3-7-sonnet-latest',
  openai: '例如: gpt-4.1, gpt-4.1-mini, gpt-4o-mini',
  custom: '填写你的模型名称，接口需兼容 OpenAI Chat Completions',
};

export const AssistantConfigPage: React.FC = () => {
  const [config, setConfig] = useState<AssistantConfigPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [showApiKey, setShowApiKey] = useState(false);
  const [message, setMessage] = useState<BannerMessage | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [testResult, setTestResult] = useState<AssistantConfigTestResult | null>(null);

  useEffect(() => {
    void loadConfig();
  }, []);

  const loadConfig = async () => {
    try {
      setLoading(true);
      const data = await assistantApi.getConfig();
      setConfig(data);
      setHasChanges(false);
      setTestResult(null);
      setMessage(null);
    } catch (error) {
      console.error('Failed to load config:', error);
      setMessage({ type: 'error', text: '加载 AI 助手配置失败。' });
    } finally {
      setLoading(false);
    }
  };

  const handleChange = <K extends keyof AssistantConfigPayload>(field: K, value: AssistantConfigPayload[K]) => {
    setConfig((current) => {
      if (!current) {
        return current;
      }
      return { ...current, [field]: value };
    });
    setHasChanges(true);
    setTestResult(null);
  };

  const handleSave = async () => {
    if (!config) {
      return;
    }

    try {
      setSaving(true);
      const saved = await assistantApi.updateConfig(config);
      setConfig(saved);
      setHasChanges(false);
      setMessage({ type: 'success', text: 'AI 助手配置已保存。' });
      setTimeout(() => setMessage(null), 3000);
    } catch (error) {
      console.error('Failed to save config:', error);
      setMessage({
        type: 'error',
        text: error instanceof Error ? error.message : '保存 AI 助手配置失败。',
      });
    } finally {
      setSaving(false);
    }
  };

  const handleTestConnection = async () => {
    if (!config) {
      return;
    }

    try {
      setTesting(true);
      setMessage(null);
      const result = await assistantApi.testConfig(config);
      setTestResult(result);
      setMessage({
        type: result.success ? 'success' : 'error',
        text: result.success ? '模型连接测试成功。' : '模型连接测试失败。',
      });
    } catch (error) {
      console.error('Failed to test assistant config:', error);
      const text = error instanceof Error ? error.message : '模型连接测试失败。';
      setTestResult(null);
      setMessage({ type: 'error', text });
    } finally {
      setTesting(false);
    }
  };

  const handleReset = () => {
    void loadConfig();
  };

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-gray-500">加载中...</div>
      </div>
    );
  }

  if (!config) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-red-500">无法加载 AI 助手配置。</div>
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto bg-gray-50">
      <div className="mx-auto max-w-4xl p-6">
        <Breadcrumb items={breadcrumbConfigs.assistantConfig} className="mb-4" />

        <div className="mb-6">
          <div className="mb-2 flex items-center gap-3">
            <Settings className="h-8 w-8 text-blue-500" />
            <h1 className="text-2xl font-bold text-gray-800">AI 助手配置</h1>
          </div>
          <p className="text-gray-600">配置 AI 助手的独立 LLM API，不影响主查询链路的系统级 LLM 配置。</p>
        </div>

        {message && (
          <div
            className={`mb-4 flex items-center gap-2 rounded-lg border p-4 ${
              message.type === 'success'
                ? 'border-green-200 bg-green-50 text-green-800'
                : 'border-red-200 bg-red-50 text-red-800'
            }`}
          >
            {message.type === 'success' ? <CheckCircle className="h-5 w-5" /> : <AlertCircle className="h-5 w-5" />}
            <span>{message.text}</span>
          </div>
        )}

        <div className="space-y-6 rounded-lg border border-gray-200 bg-white p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <label className="text-sm font-medium text-gray-700">启用 AI 助手</label>
              <p className="mt-1 text-xs text-gray-500">关闭后悬浮球助手不会显示。</p>
            </div>
            <label className="relative inline-flex cursor-pointer items-center">
              <input
                type="checkbox"
                checked={config.enabled}
                onChange={(event) => handleChange('enabled', event.target.checked)}
                className="peer sr-only"
              />
              <div className="h-6 w-11 rounded-full bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 peer-checked:bg-blue-600 peer-checked:after:translate-x-full peer-checked:after:border-white after:absolute after:left-[2px] after:top-[2px] after:h-5 after:w-5 after:rounded-full after:border after:border-gray-300 after:bg-white after:transition-all after:content-['']" />
            </label>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">LLM 提供商</label>
            <select
              value={config.provider}
              onChange={(event) => handleChange('provider', event.target.value)}
              className="w-full rounded-lg border border-gray-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="anthropic">Anthropic (Claude)</option>
              <option value="openai">OpenAI (GPT)</option>
              <option value="custom">自定义兼容接口</option>
            </select>
            <p className="mt-1 text-xs text-gray-500">选择 AI 助手调用的模型服务。</p>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">API Key</label>
            <div className="relative">
              <input
                type={showApiKey ? 'text' : 'password'}
                value={config.api_key}
                onChange={(event) => handleChange('api_key', event.target.value)}
                placeholder="sk-..."
                className="w-full rounded-lg border border-gray-300 px-3 py-2 pr-10 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <button
                type="button"
                onClick={() => setShowApiKey((current) => !current)}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
              >
                {showApiKey ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
              </button>
            </div>
            <p className="mt-1 text-xs text-gray-500">留空则尝试使用系统默认 LLM 配置。</p>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">API Base URL（可选）</label>
            <input
              type="text"
              value={config.api_base}
              onChange={(event) => handleChange('api_base', event.target.value)}
              placeholder="https://api.openai.com/v1"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <p className="mt-1 text-xs text-gray-500">
              支持填写基础地址，也兼容直接粘贴完整的 `chat/completions` 或 `messages` 端点。
            </p>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">模型</label>
            <input
              type="text"
              value={config.model}
              onChange={(event) => handleChange('model', event.target.value)}
              placeholder="gpt-4.1-mini"
              className="w-full rounded-lg border border-gray-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <p className="mt-1 text-xs text-gray-500">{providerExamples[config.provider] ?? providerExamples.custom}</p>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">温度（Temperature）：{config.temperature}</label>
            <input
              type="range"
              min="0"
              max="2"
              step="0.1"
              value={config.temperature}
              onChange={(event) => handleChange('temperature', Number.parseFloat(event.target.value))}
              className="w-full"
            />
            <div className="mt-1 flex justify-between text-xs text-gray-500">
              <span>更精确（0.0）</span>
              <span>更有创造性（2.0）</span>
            </div>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">最大 Tokens：{config.max_tokens}</label>
            <input
              type="range"
              min="100"
              max="4000"
              step="100"
              value={config.max_tokens}
              onChange={(event) => handleChange('max_tokens', Number.parseInt(event.target.value, 10))}
              className="w-full"
            />
            <div className="mt-1 flex justify-between text-xs text-gray-500">
              <span>100</span>
              <span>4000</span>
            </div>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-gray-700">系统提示词（System Prompt）</label>
            <textarea
              value={config.system_prompt}
              onChange={(event) => handleChange('system_prompt', event.target.value)}
              rows={10}
              placeholder="留空则使用默认系统提示词..."
              className="w-full rounded-lg border border-gray-300 px-3 py-2 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <p className="mt-1 text-xs text-gray-500">自定义 AI 助手的行为和知识边界，留空则使用默认知识库提示。</p>
          </div>
        </div>

        <div className="mt-6 flex flex-wrap gap-3">
          <button
            onClick={handleSave}
            disabled={!hasChanges || saving}
            className="flex items-center gap-2 rounded-lg bg-blue-500 px-6 py-2 text-white transition-colors hover:bg-blue-600 disabled:cursor-not-allowed disabled:bg-gray-300"
          >
            {saving ? (
              <>
                <RefreshCw className="h-4 w-4 animate-spin" />
                <span>保存中...</span>
              </>
            ) : (
              <>
                <Save className="h-4 w-4" />
                <span>保存配置</span>
              </>
            )}
          </button>

          <button
            onClick={handleTestConnection}
            disabled={testing || saving}
            className="flex items-center gap-2 rounded-lg bg-emerald-500 px-6 py-2 text-white transition-colors hover:bg-emerald-600 disabled:cursor-not-allowed disabled:bg-emerald-300"
          >
            {testing ? (
              <>
                <RefreshCw className="h-4 w-4 animate-spin" />
                <span>测试中...</span>
              </>
            ) : (
              <>
                <Wifi className="h-4 w-4" />
                <span>测试连接</span>
              </>
            )}
          </button>

          <button
            onClick={handleReset}
            disabled={saving || testing || !hasChanges}
            className="flex items-center gap-2 rounded-lg bg-gray-200 px-6 py-2 text-gray-700 transition-colors hover:bg-gray-300 disabled:cursor-not-allowed disabled:bg-gray-100"
          >
            <RefreshCw className="h-4 w-4" />
            <span>放弃修改</span>
          </button>
        </div>

        {testResult && (
          <div
            className={`mt-4 rounded-lg border p-4 ${
              testResult.success
                ? 'border-emerald-200 bg-emerald-50 text-emerald-900'
                : 'border-red-200 bg-red-50 text-red-900'
            }`}
          >
            <div className="flex items-start gap-3">
              {testResult.success ? <CheckCircle className="mt-0.5 h-5 w-5" /> : <AlertCircle className="mt-0.5 h-5 w-5" />}
              <div className="space-y-1 text-sm">
                <p className="font-medium">
                  {testResult.success ? '连接测试成功' : '连接测试失败'}
                </p>
                <p>提供商：{testResult.provider}</p>
                <p>模型：{testResult.model}</p>
                <p>延迟：{testResult.latency_ms.toFixed(2)} ms</p>
                <p className="break-all">{testResult.message}</p>
              </div>
            </div>
          </div>
        )}

        <div className="mt-6 rounded-lg border border-blue-200 bg-blue-50 p-4">
          <div className="flex items-start gap-2">
            <AlertCircle className="mt-0.5 h-5 w-5 text-blue-600" />
            <div className="text-sm text-blue-800">
              <p className="mb-1 font-medium">配置说明</p>
              <ul className="list-inside list-disc space-y-1">
                <li>AI 助手使用独立的 LLM 配置，不影响主查询功能。</li>
                <li>“测试连接”会直接使用当前表单值，不要求先保存。</li>
                <li>如果未配置 API Key，会尝试使用系统默认 LLM 配置；部分提供商会因此测试失败。</li>
                <li>保存后立即生效，不需要重启服务。</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
