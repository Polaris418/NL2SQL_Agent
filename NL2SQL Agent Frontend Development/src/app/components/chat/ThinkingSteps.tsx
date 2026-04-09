import { useState, type ReactNode } from 'react';
import {
  AlertCircle,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Database,
  FileEdit,
  Loader2,
  Play,
  RefreshCw,
  Sparkles,
  XCircle,
} from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';
import { zhCN } from 'date-fns/locale';
import { Badge } from '../ui/badge';
import { Card } from '../ui/card';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '../ui/collapsible';
import type { AgentStep, StepType } from '../../types/query';

interface ThinkingStepsProps {
  steps: AgentStep[];
}

const stepIcons: Record<StepType, ReactNode> = {
  rewrite: <FileEdit className="h-4 w-4 text-blue-500" />,
  schema_retrieval: <Database className="h-4 w-4 text-purple-500" />,
  sql_generation: <Sparkles className="h-4 w-4 text-green-500" />,
  sql_execution: <Play className="h-4 w-4 text-orange-500" />,
  error_reflection: <AlertCircle className="h-4 w-4 text-red-500" />,
  retry: <RefreshCw className="h-4 w-4 text-yellow-500" />,
  done: <CheckCircle2 className="h-4 w-4 text-green-500" />,
};

const stepLabels: Record<StepType, string> = {
  rewrite: '问题改写',
  schema_retrieval: 'Schema 检索',
  sql_generation: 'SQL 生成',
  sql_execution: 'SQL 执行',
  error_reflection: '错误反思',
  retry: '重试',
  done: '完成',
};

const stepDescriptions: Record<StepType, string> = {
  rewrite: '将自然语言问题转换为更适合检索和生成 SQL 的表达',
  schema_retrieval: '从数据库结构中检索相关的表和字段信息',
  sql_generation: '根据问题和 Schema 生成 SQL 查询语句',
  sql_execution: '在数据库中执行生成的 SQL',
  error_reflection: '分析执行错误并准备修复',
  retry: '对修复后的 SQL 进行再次执行',
  done: '查询流程完成',
};

function formatTimestamp(timestamp: string | Date | undefined) {
  if (!timestamp) {
    return '';
  }
  try {
    return formatDistanceToNow(new Date(timestamp), { addSuffix: true, locale: zhCN });
  } catch {
    return '';
  }
}

function deriveStatus(step: AgentStep): 'loading' | 'success' | 'error' {
  if (step.status) {
    return step.status;
  }
  if (step.step_type === 'done') {
    return 'success';
  }
  if (step.error_type || step.error_suggestion) {
    return 'error';
  }
  return 'loading';
}

function StepItem({ step }: { step: AgentStep }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const hasLongContent = step.content.length > 120;
  const status = deriveStatus(step);

  return (
    <div className="flex items-start gap-3 p-3 rounded-lg border bg-card hover:bg-muted/50 transition-colors">
      <div className="flex items-center justify-center w-8 h-8 rounded-full bg-muted flex-shrink-0">
        {stepIcons[step.step_type] ?? <Loader2 className="h-4 w-4 text-muted-foreground" />}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="font-medium text-sm">{stepLabels[step.step_type] ?? step.step_type}</span>
          <div className="ml-auto flex-shrink-0">
            {status === 'loading' && <Loader2 className="h-4 w-4 animate-spin text-blue-500" />}
            {status === 'success' && <CheckCircle2 className="h-4 w-4 text-green-500" />}
            {status === 'error' && <XCircle className="h-4 w-4 text-red-500" />}
          </div>
        </div>
        <p className="text-xs text-muted-foreground mb-2">{stepDescriptions[step.step_type] ?? 'Agent 步骤'}</p>
        <div className={`text-sm whitespace-pre-wrap break-words ${hasLongContent && !isExpanded ? 'line-clamp-3' : ''}`}>
          {step.content || '暂无内容'}
        </div>
        {hasLongContent && (
          <button onClick={() => setIsExpanded((value) => !value)} className="text-xs text-primary hover:underline mt-1">
            {isExpanded ? '收起' : '展开'}
          </button>
        )}

        {step.metadata && Object.keys(step.metadata).length > 0 && (
          <div className="mt-2 rounded border bg-muted/40 p-2 text-xs text-muted-foreground space-y-1">
            {step.metadata.error_type && <div>错误类型: {String(step.metadata.error_type)}</div>}
            {step.metadata.error_suggestion && <div>建议: {String(step.metadata.error_suggestion)}</div>}
            {step.metadata.retry_count !== undefined && (
              <div>
                重试 {String(step.metadata.retry_count)}
                {step.metadata.max_retries !== undefined ? ` / ${String(step.metadata.max_retries)}` : ''}
              </div>
            )}
            {step.metadata.message && <div>{String(step.metadata.message)}</div>}
          </div>
        )}

        {(step.error_type || step.error_suggestion) && (
          <div className="mt-2 p-2 bg-destructive/10 border border-destructive/20 rounded text-xs">
            {step.error_type && <div className="font-medium text-destructive">错误类型: {step.error_type}</div>}
            {step.error_suggestion && <p className="text-muted-foreground mt-1">建议: {step.error_suggestion}</p>}
          </div>
        )}

        {(step.retry_count !== undefined || step.max_retries !== undefined) && (
          <div className="mt-2">
            <Badge variant="outline" className="text-xs">
              重试 {step.retry_count ?? 0}/{step.max_retries ?? 3}
            </Badge>
          </div>
        )}

        {step.timestamp && <p className="text-xs text-muted-foreground mt-2">{formatTimestamp(step.timestamp)}</p>}
      </div>
    </div>
  );
}

export function ThinkingSteps({ steps }: ThinkingStepsProps) {
  const [isOpen, setIsOpen] = useState(true);

  if (steps.length === 0) {
    return null;
  }

  const successCount = steps.filter((step) => deriveStatus(step) === 'success').length;
  const errorCount = steps.filter((step) => deriveStatus(step) === 'error').length;
  const loadingCount = steps.filter((step) => deriveStatus(step) === 'loading').length;

  return (
    <Card className="mb-4">
      <Collapsible open={isOpen} onOpenChange={setIsOpen}>
        <div className="p-4 border-b">
          <CollapsibleTrigger className="flex items-center justify-between w-full hover:opacity-80 transition-opacity">
            <div className="flex items-center gap-3">
              <span className="font-medium">Agent 思考过程</span>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="text-xs">{steps.length} 步</Badge>
                {successCount > 0 && <Badge variant="outline" className="text-xs text-green-600">✓ {successCount}</Badge>}
                {errorCount > 0 && <Badge variant="outline" className="text-xs text-red-600">✗ {errorCount}</Badge>}
                {loadingCount > 0 && <Badge variant="outline" className="text-xs text-blue-600">⟳ {loadingCount}</Badge>}
              </div>
            </div>
            {isOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </CollapsibleTrigger>
        </div>
        <CollapsibleContent>
          <div className="p-4 space-y-3">
            {steps.map((step, index) => (
              <StepItem key={`${step.step_type}-${index}`} step={step} />
            ))}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  );
}
