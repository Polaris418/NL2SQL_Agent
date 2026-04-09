import {
  AlertCircle,
  Bot,
  CheckCircle2,
  Database,
  Loader2,
  Play,
  Sparkles,
  Wand2,
  XCircle,
} from 'lucide-react';
import { Card } from '../ui/card';
import { Alert, AlertDescription } from '../ui/alert';
import type { AssistantMessage as AssistantMessageType, AgentStep, ExecutionResult, ChartSuggestion } from '../../types/query';
import { ThinkingSteps } from './ThinkingSteps';
import { RAGPanel } from './RAGPanel';
import { SQLBlock } from './SQLBlock';
import { ResultTable } from './ResultTable';
import { ChartPanel } from './ChartPanel';

interface AssistantMessageProps {
  message: AssistantMessageType;
  onExecuteSQL?: (sql: string) => void;
  onPageChange?: (pageNumber: number, pageSize: number) => void;
}

type StageState = 'pending' | 'active' | 'success' | 'error';

function getSteps(message: AssistantMessageType): AgentStep[] {
  return message.steps || message.agent_steps || [];
}

function getSql(message: AssistantMessageType): string {
  return message.sql || message.generated_sql || '';
}

function getResult(message: AssistantMessageType): ExecutionResult | undefined {
  return message.results || message.execution_result || undefined;
}

function getChart(message: AssistantMessageType): ChartSuggestion | undefined {
  return message.chart_suggestion || message.chart || undefined;
}

function getRetrievedTables(message: AssistantMessageType) {
  return message.retrieved_tables || [];
}

function getTelemetry(message: AssistantMessageType) {
  return message.telemetry;
}

function deriveStageState(steps: AgentStep[], target: AgentStep['step_type'], fallbackActive = false): StageState {
  const targetSteps = steps.filter((step) => step.step_type === target);
  if (targetSteps.some((step) => step.status === 'error' || step.error_type)) {
    return 'error';
  }
  if (targetSteps.some((step) => step.status === 'success')) {
    return 'success';
  }
  if (targetSteps.some((step) => step.status === 'loading')) {
    return 'active';
  }
  return fallbackActive ? 'active' : 'pending';
}

function getLoadingStages(steps: AgentStep[]) {
  const hasSteps = steps.length > 0;
  return [
    { key: 'rewrite', label: '查询改写', icon: Wand2, state: deriveStageState(steps, 'rewrite', !hasSteps) },
    {
      key: 'schema_retrieval',
      label: '检索表结构',
      icon: Database,
      state: deriveStageState(
        steps,
        'schema_retrieval',
        hasSteps && !steps.some((step) => step.step_type === 'schema_retrieval') && steps.some((step) => step.step_type === 'rewrite'),
      ),
    },
    {
      key: 'sql_generation',
      label: '生成 SQL',
      icon: Sparkles,
      state: deriveStageState(
        steps,
        'sql_generation',
        steps.some((step) => step.step_type === 'schema_retrieval') && !steps.some((step) => step.step_type === 'sql_generation'),
      ),
    },
    {
      key: 'sql_execution',
      label: '执行查询',
      icon: Play,
      state: deriveStageState(
        steps,
        'sql_execution',
        steps.some((step) => step.step_type === 'sql_generation') && !steps.some((step) => step.step_type === 'sql_execution'),
      ),
    },
  ];
}

function StageIcon({ state }: { state: StageState }) {
  if (state === 'success') return <CheckCircle2 className="h-4 w-4 text-emerald-600" />;
  if (state === 'error') return <XCircle className="h-4 w-4 text-red-600" />;
  if (state === 'active') return <Loader2 className="h-4 w-4 animate-spin text-blue-600" />;
  return <div className="h-4 w-4 rounded-full border border-zinc-300 bg-white" />;
}

export function AssistantMessage({ message, onExecuteSQL, onPageChange }: AssistantMessageProps) {
  const isLoading = message.status === 'loading';
  const hasError = message.status === 'error';
  const isSuccess = message.status === 'success';
  const steps = getSteps(message);
  const sql = getSql(message);
  const result = getResult(message);
  const chart = getChart(message);
  const retrievedTables = getRetrievedTables(message);
  const telemetry = getTelemetry(message);
  const loadingStages = getLoadingStages(steps);

  return (
    <div className="mb-6 flex justify-start">
      <div className="flex w-full max-w-full gap-3">
        <div className="flex items-start pt-1">
          <div className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-2xl bg-zinc-100 shadow-[0_10px_24px_rgba(0,0,0,0.06)]">
            {isLoading ? <Loader2 className="h-4 w-4 animate-spin text-black" /> : <Bot className="h-4 w-4 text-black" />}
          </div>
        </div>
        <div className="min-w-0 flex-1">
          {steps.length > 0 && <ThinkingSteps steps={steps} />}

          {isLoading && !sql && (
            <Card className="mb-4 rounded-[26px] border-zinc-200 bg-white p-4 shadow-[0_10px_24px_rgba(0,0,0,0.05)]">
              <div className="mb-3 flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin text-primary" />
                <span className="text-sm font-medium text-zinc-900">Agent 正在处理</span>
              </div>
              <div className="grid gap-2 md:grid-cols-4">
                {loadingStages.map((stage) => {
                  const Icon = stage.icon;
                  const stateClass =
                    stage.state === 'success'
                      ? 'border-emerald-200 bg-emerald-50'
                      : stage.state === 'active'
                        ? 'border-blue-200 bg-blue-50'
                        : stage.state === 'error'
                          ? 'border-red-200 bg-red-50'
                          : 'border-zinc-200 bg-zinc-50';
                  const stateLabel =
                    stage.state === 'active'
                      ? '进行中'
                      : stage.state === 'success'
                        ? '已完成'
                        : stage.state === 'error'
                          ? '失败'
                          : '等待中';
                  return (
                    <div key={stage.key} className={`rounded-2xl border px-3 py-3 ${stateClass}`}>
                      <div className="mb-2 flex items-center justify-between gap-2">
                        <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-zinc-500">
                          <Icon className="h-3.5 w-3.5" />
                          <span>{stage.label}</span>
                        </div>
                        <StageIcon state={stage.state} />
                      </div>
                      <div className="text-sm text-zinc-700">{stateLabel}</div>
                    </div>
                  );
                })}
              </div>
            </Card>
          )}

          {hasError && message.error_type === 'database_mismatch' && (
            <div className="mb-4 rounded-[20px] border border-amber-300 bg-amber-50 px-4 py-3 text-sm text-amber-900">
              <div className="mb-1 flex items-center gap-2 font-medium">
                <AlertCircle className="h-4 w-4" />
                <span>数据库连接不匹配</span>
              </div>
              <p>{message.error_message || '查询执行失败，请检查数据库连接配置'}</p>
              {message.error_suggestion && <p className="mt-1 text-amber-800/80">{message.error_suggestion}</p>}
            </div>
          )}

          {hasError && message.error_type !== 'database_mismatch' && (
            <Alert variant="destructive" className="mb-4">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{message.error_message || '查询执行失败，请检查数据库连接配置'}</AlertDescription>
            </Alert>
          )}

          <RAGPanel retrievedTables={retrievedTables} telemetry={telemetry} steps={steps} />

          {sql && <SQLBlock sql={sql} onExecute={onExecuteSQL} isExecuting={isLoading} />}
          {result && <ResultTable result={result} onPageChange={onPageChange} />}
          
          {message.summary && (
            <Card className="mb-4 rounded-[22px] border-blue-200 bg-blue-50 p-4">
              <div className="mb-2 flex items-center gap-2 text-sm font-medium text-blue-900">
                <Sparkles className="h-4 w-4" />
                <span>结果总结</span>
              </div>
              <p className="text-sm leading-relaxed text-blue-900/90">{message.summary}</p>
            </Card>
          )}
          
          {result && chart && <ChartPanel result={result} suggestion={chart} />}

          {isSuccess && result && (
            <Card className="mb-4 rounded-[22px] border-zinc-200 bg-zinc-50 p-3">
              <p className="text-sm text-zinc-700">查询成功返回 {result.row_count} 条记录</p>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
