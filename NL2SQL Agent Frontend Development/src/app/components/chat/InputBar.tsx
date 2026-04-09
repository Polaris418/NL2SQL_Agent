import { useState, KeyboardEvent, useRef, useEffect } from 'react';
import { Send, Loader2, Sparkles } from 'lucide-react';
import { Button } from '../ui/button';
import { Textarea } from '../ui/textarea';
import { Badge } from '../ui/badge';

interface InputBarProps {
  onSend: (message: string) => void;
  disabled?: boolean;
  placeholder?: string;
  isLoading?: boolean;
}

export function InputBar({ onSend, disabled, placeholder, isLoading }: InputBarProps) {
  const [input, setInput] = useState('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const charCount = input.length;
  const maxChars = 2000;
  const isOverLimit = charCount > maxChars;

  const handleSend = () => {
    if (input.trim() && !disabled && !isOverLimit && !isLoading) {
      onSend(input.trim());
      setInput('');
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    // Enter 发送，Shift+Enter 换行
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  // 自动聚焦到输入框
  useEffect(() => {
    if (!disabled && !isLoading && textareaRef.current) {
      textareaRef.current.focus();
    }
  }, [disabled, isLoading]);

  const canSend = input.trim() && !disabled && !isOverLimit && !isLoading;

  return (
    <div className="sticky bottom-0 border-t border-zinc-200 bg-white px-4 py-2">
      <div className="mx-auto max-w-4xl">
        <div className="rounded-[24px] border border-zinc-200 bg-white px-3 py-2 shadow-[0_6px_18px_rgba(0,0,0,0.03)]">
          <div className="flex items-end gap-2">
            <div className="relative flex-1">
              <Textarea
                ref={textareaRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder={placeholder || '用自然语言提问数据库，例如：上个月每个城市的订单数量和总金额是多少？'}
                className="min-h-[56px] max-h-[132px] resize-none rounded-[18px] border-zinc-200 bg-zinc-50 px-4 py-4 pr-16 text-[14px] leading-6 shadow-none focus-visible:ring-2"
                disabled={disabled || isLoading}
              />
              <div className="absolute bottom-3 right-3 flex items-center gap-2">
                {charCount > 0 && (
                  <Badge
                    variant={isOverLimit ? 'destructive' : 'secondary'}
                    className="rounded-full border-0 bg-zinc-100 px-2 py-0.5 text-[11px] text-zinc-600"
                  >
                    {charCount}/{maxChars}
                  </Badge>
                )}
              </div>
            </div>
              <Button
                onClick={handleSend}
                disabled={!canSend}
                size="icon"
                className="h-10 w-10 shrink-0 rounded-xl bg-black text-white shadow-none hover:bg-zinc-800"
              >
                {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
              </Button>
            </div>
        </div>
        <div className="mt-1 flex flex-wrap items-center justify-between gap-3 px-2 text-[11px] text-zinc-500">
          <div className="flex flex-wrap items-center gap-4">
            <span className="flex items-center gap-1">
              <kbd className="rounded-md border bg-white px-1.5 py-0.5">Enter</kbd> 发送
            </span>
            <span className="flex items-center gap-1">
              <kbd className="rounded-md border bg-white px-1.5 py-0.5">Shift</kbd> + 
              <kbd className="rounded-md border bg-white px-1.5 py-0.5">Enter</kbd> 换行
            </span>
          </div>
          <span className="flex items-center gap-1 rounded-full bg-zinc-100 px-2.5 py-1 text-zinc-700">
            <Sparkles className="h-3 w-3" /> AI 驱动
          </span>
        </div>
      </div>
    </div>
  );
}
