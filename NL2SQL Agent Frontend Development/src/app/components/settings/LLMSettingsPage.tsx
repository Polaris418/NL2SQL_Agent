import { useEffect, useMemo, useState } from 'react';
import { CheckCircle2, Cpu, KeyRound, RefreshCw, Save, Server, ShieldAlert, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { llmSettingsApi } from '../../api/client';
import type {
  LLMProfile,
  LLMProfileUpsert,
  LLMProvider,
  LLMProviderOption,
  LLMSettings,
} from '../../types/llm';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';
import { Button } from '../ui/button';
import { Label } from '../ui/label';
import { Input } from '../ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Breadcrumb, breadcrumbConfigs } from '../ui/breadcrumb';

function getProviderOption(options: LLMProviderOption[], provider: LLMProvider) {
  return options.find((option) => option.id === provider) ?? null;
}

const emptyForm = {
  id: undefined as string | undefined,
  display_name: '',
  provider: 'nvidia' as LLMProvider,
  model: '',
  base_url: '',
  api_key: '',
};

export function LLMSettingsPage() {
  const [settings, setSettings] = useState<LLMSettings | null>(null);
  const [form, setForm] = useState(emptyForm);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [isTesting, setIsTesting] = useState(false);
  const [isUpdatingRouting, setIsUpdatingRouting] = useState(false);

  const loadSettings = async () => {
    setIsLoading(true);
    try {
      const data = await llmSettingsApi.get();
      setSettings(data);
      const firstProvider = data.providers[0];
      setForm((current) => ({
        ...current,
        provider: current.provider || firstProvider?.id || 'nvidia',
        model: current.model || firstProvider?.default_model || '',
        base_url: current.base_url || firstProvider?.default_base_url || '',
      }));
    } catch (error: any) {
      toast.error(`加载模型配置失败: ${error?.message || '未知错误'}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void loadSettings();
  }, []);

  const providerOption = useMemo(
    () => (settings ? getProviderOption(settings.providers, form.provider) : null),
    [settings, form.provider],
  );

  const setProvider = (provider: LLMProvider) => {
    const option = settings ? getProviderOption(settings.providers, provider) : null;
    setForm((current) => ({
      ...current,
      provider,
      model: option?.default_model ?? current.model,
      base_url: option?.default_base_url ?? current.base_url,
      display_name: current.display_name || option?.label || '',
    }));
  };

  const buildPayload = (): LLMProfileUpsert => ({
    id: form.id,
    display_name: form.display_name.trim() || providerOption?.label || form.provider,
    provider: form.provider,
    model: form.model.trim(),
    base_url: form.base_url.trim() || null,
    api_key: form.api_key.trim() || undefined,
  });

  const resetForm = () => {
    const firstProvider = settings?.providers[0];
    setForm({
      ...emptyForm,
      provider: firstProvider?.id || 'nvidia',
      model: firstProvider?.default_model || '',
      base_url: firstProvider?.default_base_url || '',
      display_name: firstProvider?.label || '',
    });
  };

  const handleSave = async () => {
    if (!form.model.trim()) {
      toast.error('请先填写模型名');
      return;
    }
    setIsSaving(true);
    try {
      const data = await llmSettingsApi.upsertProfile(buildPayload());
      setSettings(data);
      toast.success(form.id ? 'API 配置已更新' : 'API 配置已保存');
      resetForm();
    } catch (error: any) {
      toast.error(`保存失败: ${error?.message || '未知错误'}`);
    } finally {
      setIsSaving(false);
    }
  };

  const handleTest = async () => {
    if (providerOption?.api_key_required && !form.api_key.trim() && !form.id) {
      toast.error('这个提供商需要 API Key，请先填写后再测试。');
      return;
    }
    setIsTesting(true);
    try {
      const result = await llmSettingsApi.test(buildPayload());
      if (result.success) {
        toast.success(`测试成功，耗时 ${Math.round(result.latency_ms)}ms`);
      } else {
        toast.error(result.message);
      }
    } catch (error: any) {
      toast.error(`测试失败: ${error?.message || '未知错误'}`);
    } finally {
      setIsTesting(false);
    }
  };

  const handleSelectProfile = (profile: LLMProfile) => {
    setForm({
      id: profile.id,
      display_name: profile.display_name,
      provider: profile.provider,
      model: profile.model,
      base_url: profile.base_url ?? '',
      api_key: '',
    });
  };

  const handleDeleteProfile = async (profileId: string) => {
    try {
      const data = await llmSettingsApi.removeProfile(profileId);
      setSettings(data);
      if (form.id === profileId) {
        resetForm();
      }
      toast.success('API 配置已删除');
    } catch (error: any) {
      toast.error(`删除失败: ${error?.message || '未知错误'}`);
    }
  };

  const handleRoutingChange = async (primaryProfileId: string, fallbackProfileId?: string | null) => {
    setIsUpdatingRouting(true);
    try {
      const data = await llmSettingsApi.updateRouting({
        primary_profile_id: primaryProfileId,
        fallback_profile_id: fallbackProfileId || null,
      });
      setSettings(data);
      toast.success('已更新主用与备用 API');
    } catch (error: any) {
      toast.error(`切换失败: ${error?.message || '未知错误'}`);
    } finally {
      setIsUpdatingRouting(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <RefreshCw className="h-6 w-6 animate-spin text-zinc-500" />
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto">
      <div className="mx-auto flex max-w-6xl flex-col gap-6 p-4 md:p-6">
        {/* 面包屑导航 */}
        <Breadcrumb items={breadcrumbConfigs.settingsLlm} />
        
        <section className="rounded-[24px] border border-zinc-200 bg-white p-6 shadow-sm">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <div className="mb-2 inline-flex items-center gap-2 rounded-full border border-zinc-200 bg-zinc-50 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.24em] text-zinc-500">
                <Cpu className="h-3.5 w-3.5 text-zinc-700" />
                API routing
              </div>
              <h1 className="text-3xl text-black">模型 API 配置</h1>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-zinc-600">
                这里可以保存多个模型 API，指定当前使用的主 API 和备用 API。当主 API 不可用时，后端会自动尝试切换到备用 API。
              </p>
            </div>
            <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3 text-sm">
              <div className="font-semibold text-black">当前已配置 {settings?.profiles.length ?? 0} 个 API</div>
              <div className="mt-1 text-xs text-zinc-500">建议至少保留 1 个主 API 和 1 个备用 API。</div>
            </div>
          </div>
        </section>

        <div className="grid gap-6 xl:grid-cols-[1fr_1fr]">
          <Card className="rounded-[24px] border-zinc-200 bg-white shadow-sm">
            <CardHeader>
              <CardTitle>已配置的 API</CardTitle>
              <CardDescription>这里展示当前已经保存的 API，并可以选择主用与备用。</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {settings?.profiles.length ? (
                settings.profiles.map((profile) => {
                  const isPrimary = settings.active_profile_id === profile.id;
                  const isFallback = settings.fallback_profile_id === profile.id;
                  return (
                    <div key={profile.id} className="rounded-2xl border border-zinc-200 p-4">
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="font-semibold text-black">{profile.display_name}</div>
                            {isPrimary && (
                              <span className="rounded-full bg-black px-2.5 py-1 text-[11px] text-white">主用 API</span>
                            )}
                            {isFallback && (
                              <span className="rounded-full bg-zinc-200 px-2.5 py-1 text-[11px] text-zinc-700">备用 API</span>
                            )}
                          </div>
                          <div className="mt-2 text-sm text-zinc-600">
                            {profile.provider} · {profile.model}
                          </div>
                          <div className="mt-1 truncate text-xs text-zinc-500">{profile.base_url || '使用默认地址'}</div>
                          <div className="mt-1 text-xs text-zinc-500">
                            {profile.api_key_masked ? `Key: ${profile.api_key_masked}` : '未配置 API Key'}
                          </div>
                        </div>
                        <div className="flex shrink-0 gap-2">
                          <Button variant="outline" size="sm" onClick={() => handleSelectProfile(profile)}>编辑</Button>
                          <Button variant="outline" size="sm" onClick={() => void handleDeleteProfile(profile.id)}>
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="rounded-2xl border border-dashed border-zinc-200 p-6 text-sm text-zinc-500">
                  还没有配置任何 API。先在右侧新增一个主 API。
                </div>
              )}

              {!!settings?.profiles.length && (
                <div className="grid gap-4 rounded-2xl border border-zinc-200 bg-zinc-50 p-4">
                  <div className="flex items-center gap-2 text-sm font-semibold text-black">
                    <CheckCircle2 className="h-4 w-4" />
                    使用策略
                  </div>
                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="space-y-2">
                      <Label>主用 API</Label>
                      <Select
                        value={settings.active_profile_id || undefined}
                        onValueChange={(value) => void handleRoutingChange(value, settings.fallback_profile_id)}
                        disabled={isUpdatingRouting}
                      >
                        <SelectTrigger className="h-11 rounded-2xl border-zinc-200 bg-white">
                          <SelectValue placeholder="选择主用 API" />
                        </SelectTrigger>
                        <SelectContent>
                          {settings.profiles.map((profile) => (
                            <SelectItem key={profile.id} value={profile.id}>
                              {profile.display_name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>备用 API</Label>
                      <Select
                        value={settings.fallback_profile_id || 'none'}
                        onValueChange={(value) =>
                          void handleRoutingChange(settings.active_profile_id || settings.profiles[0].id, value === 'none' ? null : value)
                        }
                        disabled={isUpdatingRouting}
                      >
                        <SelectTrigger className="h-11 rounded-2xl border-zinc-200 bg-white">
                          <SelectValue placeholder="选择备用 API" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="none">不设置备用 API</SelectItem>
                          {settings.profiles
                            .filter((profile) => profile.id !== settings.active_profile_id)
                            .map((profile) => (
                              <SelectItem key={profile.id} value={profile.id}>
                                {profile.display_name}
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <div className="flex items-start gap-2 rounded-2xl border border-zinc-200 bg-white px-3 py-3 text-xs text-zinc-600">
                    <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0 text-zinc-500" />
                    当主 API 请求失败时，后端会自动尝试备用 API，尽量不中断 NL2SQL 生成流程。
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          <Card className="rounded-[24px] border-zinc-200 bg-white shadow-sm">
            <CardHeader>
              <CardTitle>{form.id ? '编辑 API 配置' : '新增 API 配置'}</CardTitle>
              <CardDescription>保存后会加入左侧已配置列表，然后你可以选择它作为主用或备用。</CardDescription>
            </CardHeader>
            <CardContent className="space-y-5">
              <div className="space-y-2">
                <Label>配置名称</Label>
                <Input
                  value={form.display_name}
                  onChange={(e) => setForm((current) => ({ ...current, display_name: e.target.value }))}
                  placeholder="例如：NVIDIA 主用 / OpenAI 备用"
                  className="h-11 rounded-2xl border-zinc-200 bg-zinc-50"
                />
              </div>

              <div className="space-y-2">
                <Label>提供商</Label>
                <Select value={form.provider} onValueChange={(value) => setProvider(value as LLMProvider)}>
                  <SelectTrigger className="h-11 rounded-2xl border-zinc-200 bg-zinc-50">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {settings?.providers.map((option) => (
                      <SelectItem key={option.id} value={option.id}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>模型名</Label>
                <Input
                  value={form.model}
                  onChange={(e) => setForm((current) => ({ ...current, model: e.target.value }))}
                  className="h-11 rounded-2xl border-zinc-200 bg-zinc-50"
                />
              </div>

              <div className="space-y-2">
                <Label className="flex items-center gap-2">
                  <Server className="h-4 w-4" />
                  Base URL
                </Label>
                <Input
                  value={form.base_url}
                  onChange={(e) => setForm((current) => ({ ...current, base_url: e.target.value }))}
                  placeholder={providerOption?.default_base_url ?? 'https://your-endpoint/v1'}
                  className="h-11 rounded-2xl border-zinc-200 bg-zinc-50"
                />
              </div>

              <div className="space-y-2">
                <Label className="flex items-center gap-2">
                  <KeyRound className="h-4 w-4" />
                  API Key
                </Label>
                <Input
                  type="password"
                  value={form.api_key}
                  onChange={(e) => setForm((current) => ({ ...current, api_key: e.target.value }))}
                  placeholder={form.id ? '留空则继续使用已保存的 API Key' : '输入新的 API Key'}
                  className="h-11 rounded-2xl border-zinc-200 bg-zinc-50"
                />
                <p className="text-xs text-zinc-500">
                  {providerOption?.api_key_required ? '该提供商需要 API Key。' : '该提供商可以不填 API Key。'}
                </p>
              </div>

              <div className="flex flex-wrap gap-3 pt-2">
                <Button onClick={() => void handleTest()} disabled={isTesting} variant="outline" className="h-11 rounded-2xl">
                  {isTesting ? <RefreshCw className="mr-2 h-4 w-4 animate-spin" /> : <Server className="mr-2 h-4 w-4" />}
                  测试连接
                </Button>
                <Button onClick={() => void handleSave()} disabled={isSaving} className="h-11 rounded-2xl bg-black text-white hover:bg-zinc-800">
                  {isSaving ? <RefreshCw className="mr-2 h-4 w-4 animate-spin" /> : <Save className="mr-2 h-4 w-4" />}
                  {form.id ? '更新配置' : '保存为新配置'}
                </Button>
                {form.id && (
                  <Button variant="outline" onClick={resetForm} className="h-11 rounded-2xl">
                    取消编辑
                  </Button>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
