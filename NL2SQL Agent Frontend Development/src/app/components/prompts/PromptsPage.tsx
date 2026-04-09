import { useEffect, useState } from 'react';
import { FileText, RefreshCw, Save, RotateCcw, Code2 } from 'lucide-react';
import { promptsApi, type PromptTemplate } from '../../api/client';
import { Button } from '../ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { toast } from 'sonner';
import { Textarea } from '../ui/textarea';
import { Badge } from '../ui/badge';
import { Breadcrumb, breadcrumbConfigs } from '../ui/breadcrumb';

export function PromptsPage() {
  const [prompts, setPrompts] = useState<PromptTemplate[]>([]);
  const [selectedPrompt, setSelectedPrompt] = useState<PromptTemplate | null>(null);
  const [editedTemplate, setEditedTemplate] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [hasChanges, setHasChanges] = useState(false);

  const loadPrompts = async () => {
    setIsLoading(true);
    try {
      const data = await promptsApi.list();
      setPrompts(data);
      if (!selectedPrompt && data.length > 0) {
        setSelectedPrompt(data[0]);
        setEditedTemplate(data[0].template);
      }
    } catch (error: any) {
      toast.error(`加载 Prompt 失败: ${error?.message || '未知错误'}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void loadPrompts();
  }, []);

  useEffect(() => {
    if (selectedPrompt) {
      setHasChanges(editedTemplate !== selectedPrompt.template);
    }
  }, [editedTemplate, selectedPrompt]);

  const handleSelectPrompt = (prompt: PromptTemplate) => {
    if (hasChanges) {
      if (!confirm('有未保存的更改，确定要切换吗？')) {
        return;
      }
    }
    setSelectedPrompt(prompt);
    setEditedTemplate(prompt.template);
    setHasChanges(false);
  };

  const handleSave = async () => {
    if (!selectedPrompt) return;

    setIsSaving(true);
    try {
      const result = await promptsApi.update(selectedPrompt.name, { template: editedTemplate });
      toast.success(result.message);
      if (result.note) {
        toast.info(result.note);
      }
      
      // 更新本地状态
      const updatedPrompts = prompts.map(p =>
        p.name === selectedPrompt.name ? { ...p, template: editedTemplate } : p
      );
      setPrompts(updatedPrompts);
      setSelectedPrompt({ ...selectedPrompt, template: editedTemplate });
      setHasChanges(false);
    } catch (error: any) {
      toast.error(`保存失败: ${error?.message || '未知错误'}`);
    } finally {
      setIsSaving(false);
    }
  };

  const handleReset = async () => {
    if (!selectedPrompt) return;

    if (!confirm('确定要重置此 Prompt 到默认值吗？')) {
      return;
    }

    try {
      await promptsApi.reset(selectedPrompt.name);
      toast.success('已重置到默认值');
      await loadPrompts();
    } catch (error: any) {
      toast.error(`重置失败: ${error?.message || '未知错误'}`);
    }
  };

  const handleDiscard = () => {
    if (selectedPrompt) {
      setEditedTemplate(selectedPrompt.template);
      setHasChanges(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center p-4">
        <Card className="w-full max-w-xl">
          <CardContent className="flex flex-col items-center justify-center py-16">
            <RefreshCw className="h-8 w-8 animate-spin text-primary mb-3" />
            <p className="text-muted-foreground">加载 Prompt 配置中...</p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="h-full p-4 md:p-4">
      <div className="h-full flex flex-col gap-3">
        {/* 面包屑导航 */}
        <Breadcrumb items={breadcrumbConfigs.prompts} />
        
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-2">
              <FileText className="h-7 w-7" />
              Prompt 配置
            </h1>
            <p className="text-sm text-muted-foreground mt-1">
              查看和配置大模型的 Prompt 模板，控制 AI 的行为和输出格式
            </p>
          </div>
          <Button variant="outline" onClick={() => void loadPrompts()}>
            <RefreshCw className="h-4 w-4 mr-2" />
            刷新
          </Button>
        </div>

        <div className="flex-1 min-h-0 grid gap-3 lg:grid-cols-[320px_minmax(0,1fr)]">
          {/* Prompt 列表 */}
          <Card className="flex min-h-0 flex-col overflow-hidden rounded-[24px] border-zinc-200">
            <CardHeader className="border-b">
              <CardTitle className="flex items-center gap-2">
                <Code2 className="h-5 w-5" />
                Prompt 模板
              </CardTitle>
              <p className="text-xs text-muted-foreground mt-1">
                选择要查看或编辑的 Prompt
              </p>
            </CardHeader>
            <CardContent className="min-h-0 flex-1 p-0">
              <div className="space-y-2 p-3">
                {prompts.map((prompt) => {
                  const isActive = selectedPrompt?.name === prompt.name;
                  return (
                    <button
                      key={prompt.name}
                      type="button"
                      onClick={() => handleSelectPrompt(prompt)}
                      className={`w-full rounded-2xl border text-left transition p-4 ${
                        isActive ? 'border-black bg-zinc-50' : 'border-zinc-200 bg-white hover:bg-zinc-50'
                      }`}
                    >
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0 flex-1">
                          <p className="text-sm font-medium text-black">{prompt.name}</p>
                          <p className="mt-1 text-xs text-muted-foreground line-clamp-2">
                            {prompt.description}
                          </p>
                          <div className="mt-2 flex flex-wrap gap-1">
                            {prompt.variables.map((variable) => (
                              <Badge key={variable} variant="outline" className="text-xs">
                                {variable}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </CardContent>
          </Card>

          {/* Prompt 编辑器 */}
          <Card className="flex min-h-0 flex-col overflow-hidden rounded-[24px] border-zinc-200">
            {selectedPrompt ? (
              <>
                <CardHeader className="border-b">
                  <div className="flex items-start justify-between gap-4">
                    <div>
                      <CardTitle>{selectedPrompt.name}</CardTitle>
                      <p className="text-sm text-muted-foreground mt-1">
                        {selectedPrompt.description}
                      </p>
                      <div className="mt-2 flex flex-wrap gap-2">
                        <Badge variant="outline">
                          变量: {selectedPrompt.variables.join(', ')}
                        </Badge>
                        {hasChanges && (
                          <Badge className="bg-amber-500">未保存</Badge>
                        )}
                      </div>
                    </div>
                  </div>
                </CardHeader>

                <div className="min-h-0 flex-1 overflow-y-auto p-6">
                  <div className="space-y-4">
                    <div>
                      <label className="mb-2 block text-sm font-medium">Prompt 模板</label>
                      <Textarea
                        value={editedTemplate}
                        onChange={(e) => setEditedTemplate(e.target.value)}
                        className="min-h-[400px] font-mono text-sm"
                        placeholder="输入 Prompt 模板..."
                      />
                      <p className="mt-2 text-xs text-muted-foreground">
                        使用 {'{'}变量名{'}'} 来插入动态内容。可用变量: {selectedPrompt.variables.join(', ')}
                      </p>
                    </div>

                    <div className="rounded-2xl border border-amber-200 bg-amber-50 p-4">
                      <p className="text-sm text-amber-900">
                        <strong>注意：</strong> Prompt 更改目前不会持久化保存。重启服务后会恢复到默认值。
                        如需永久更改，请直接修改 <code className="bg-amber-100 px-1 rounded">app/prompts/</code> 目录下的 Python 文件。
                      </p>
                    </div>
                  </div>
                </div>

                <div className="border-t bg-background px-6 py-4">
                  <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-between">
                    <Button
                      variant="outline"
                      onClick={handleReset}
                      className="text-destructive hover:text-destructive hover:bg-destructive/10"
                    >
                      <RotateCcw className="mr-1 h-4 w-4" />
                      重置到默认值
                    </Button>
                    <div className="flex flex-col-reverse gap-2 sm:flex-row">
                      {hasChanges && (
                        <Button variant="outline" onClick={handleDiscard}>
                          放弃更改
                        </Button>
                      )}
                      <Button onClick={() => void handleSave()} disabled={!hasChanges || isSaving}>
                        <Save className="mr-1 h-4 w-4" />
                        {isSaving ? '保存中...' : '保存更改'}
                      </Button>
                    </div>
                  </div>
                </div>
              </>
            ) : (
              <div className="flex h-full items-center justify-center p-8 text-center text-muted-foreground">
                <div>
                  <FileText className="mx-auto mb-4 h-12 w-12 opacity-20" />
                  <p className="text-base font-medium text-zinc-700">选择一个 Prompt 模板</p>
                  <p className="mt-2 text-sm">左侧列表中选择要查看或编辑的 Prompt</p>
                </div>
              </div>
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}
