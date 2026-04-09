import { useState } from 'react';
import { Copy, Check, Edit, Play, Loader2 } from 'lucide-react';
import { Button } from '../ui/button';
import { Card } from '../ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '../ui/dialog';
import { Alert, AlertDescription } from '../ui/alert';
import Editor from '@monaco-editor/react';
import hljs from 'highlight.js/lib/core';
import sql from 'highlight.js/lib/languages/sql';
import 'highlight.js/styles/github.css';

// 注册SQL语言
hljs.registerLanguage('sql', sql);

interface SQLBlockProps {
  sql: string;
  onExecute?: (sql: string) => void;
  isExecuting?: boolean;
  executionError?: string;
}

export function SQLBlock({ sql, onExecute, isExecuting, executionError }: SQLBlockProps) {
  const [copied, setCopied] = useState(false);
  const [editedSql, setEditedSql] = useState(sql);
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (error) {
      console.error('Failed to copy:', error);
    }
  };

  const handleExecuteEdited = () => {
    if (onExecute && editedSql.trim()) {
      onExecute(editedSql.trim());
      setIsDialogOpen(false);
    }
  };

  // 打开对话框时重置编辑的 SQL
  const handleOpenChange = (open: boolean) => {
    if (open) {
      setEditedSql(sql);
    }
    setIsDialogOpen(open);
  };

  // 语法高亮
  const highlightedCode = hljs.highlight(sql, { language: 'sql' }).value;

  return (
    <Card className="mb-4 overflow-hidden border-zinc-900/15 shadow-[0_10px_24px_rgba(15,23,42,0.06)]">
      <div className="flex items-center justify-between border-b border-zinc-900/10 bg-zinc-900 px-4 py-3 text-white">
        <div>
          <div className="text-[11px] uppercase tracking-[0.24em] text-white/55">SQL Draft</div>
          <span className="mt-1 block text-sm font-semibold">生成的 SQL</span>
        </div>
        <div className="flex gap-2">
          <Button variant="ghost" size="sm" className="text-white hover:bg-white/10 hover:text-white" onClick={handleCopy}>
            {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
            <span className="ml-1">{copied ? '已复制' : '复制'}</span>
          </Button>
          {onExecute && (
            <Dialog open={isDialogOpen} onOpenChange={handleOpenChange}>
              <DialogTrigger asChild>
                <Button variant="ghost" size="sm" className="text-white hover:bg-white/10 hover:text-white">
                  <Edit className="h-4 w-4" />
                  <span className="ml-1">编辑</span>
                </Button>
              </DialogTrigger>
              <DialogContent className="max-w-4xl max-h-[80vh]">
                <DialogHeader>
                  <DialogTitle>编辑并执行 SQL</DialogTitle>
                </DialogHeader>
                <div className="space-y-4">
                  {executionError && (
                    <Alert variant="destructive">
                      <AlertDescription>{executionError}</AlertDescription>
                    </Alert>
                  )}
                  <div className="border rounded-lg overflow-hidden">
                    <Editor
                      height="400px"
                      defaultLanguage="sql"
                      value={editedSql}
                      onChange={(value) => setEditedSql(value || '')}
                      theme="vs-dark"
                      options={{
                        minimap: { enabled: false },
                        fontSize: 14,
                        lineNumbers: 'on',
                        scrollBeyondLastLine: false,
                        automaticLayout: true,
                        tabSize: 2,
                        wordWrap: 'on',
                      }}
                    />
                  </div>
                  <div className="flex justify-end gap-2">
                    <Button variant="outline" onClick={() => setIsDialogOpen(false)}>
                      取消
                    </Button>
                    <Button 
                      onClick={handleExecuteEdited} 
                      disabled={!editedSql.trim() || isExecuting}
                    >
                      {isExecuting ? (
                        <>
                          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                          执行中...
                        </>
                      ) : (
                        <>
                          <Play className="h-4 w-4 mr-1" />
                          执行
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              </DialogContent>
            </Dialog>
          )}
        </div>
      </div>
      <div className="overflow-x-auto bg-zinc-50 p-4">
        <pre className="whitespace-pre-wrap break-words rounded-2xl border border-zinc-900/10 bg-white px-4 py-4 text-sm leading-7 text-black shadow-[inset_0_1px_0_rgba(255,255,255,0.8)]">
          <code
            dangerouslySetInnerHTML={{ __html: highlightedCode }}
            className="language-sql block min-w-full whitespace-pre-wrap break-words font-mono text-[14px] leading-7 text-black"
          />
        </pre>
      </div>
    </Card>
  );
}
