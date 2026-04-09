import { Sparkles, Lightbulb, TrendingUp, Search, Link, ArrowRight, DatabaseZap, ShieldCheck, Gauge } from 'lucide-react';
import { Card, CardContent } from '../ui/card';
import { Badge } from '../ui/badge';
import { exampleQuestions, exampleCategories } from '../../config/examples';

interface WelcomeScreenProps {
  onSelectQuery: (query: string) => void;
}

const categoryIcons: Record<string, React.ReactNode> = {
  '数据统计': <TrendingUp className="h-5 w-5" />,
  '数据分析': <Lightbulb className="h-5 w-5" />,
  '数据查询': <Search className="h-5 w-5" />,
  '关联查询': <Link className="h-5 w-5" />,
};

export function WelcomeScreen({ onSelectQuery }: WelcomeScreenProps) {
  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <div className="grid gap-6 lg:grid-cols-[1.25fr_0.75fr]">
        <Card className="overflow-hidden rounded-[32px] border-zinc-200 bg-white shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
          <CardContent className="relative p-8 md:p-10">
            <div className="relative">
              <Badge className="rounded-full bg-black px-4 py-1 text-white hover:bg-black">
                Conversational Analytics Workspace
              </Badge>
              <h1 className="mt-6 max-w-3xl text-5xl leading-[1.02] text-black md:text-6xl">
                用一句自然语言，
                <br />
                直接调动你的数据库。
              </h1>
              <p className="mt-5 max-w-2xl text-base leading-7 text-zinc-600 md:text-lg">
                从问题理解、Schema 检索、SQL 生成到执行与可视化，一次性完成。你只需要提问，不需要先写查询。
              </p>

              <div className="mt-8 flex flex-wrap gap-3">
                <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3 shadow-sm">
                  <div className="flex items-center gap-2 text-sm font-semibold text-black">
                    <DatabaseZap className="h-4 w-4 text-zinc-700" />
                    Schema-aware
                  </div>
                  <div className="mt-1 text-xs text-zinc-500">自动识别表、字段和关联上下文</div>
                </div>
                <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3 shadow-sm">
                  <div className="flex items-center gap-2 text-sm font-semibold text-black">
                    <Gauge className="h-4 w-4 text-zinc-700" />
                    Streaming
                  </div>
                  <div className="mt-1 text-xs text-zinc-500">实时展示推理步骤与执行结果</div>
                </div>
                <div className="rounded-2xl border border-zinc-200 bg-zinc-50 px-4 py-3 shadow-sm">
                  <div className="flex items-center gap-2 text-sm font-semibold text-black">
                    <ShieldCheck className="h-4 w-4 text-zinc-700" />
                    Guardrails
                  </div>
                  <div className="mt-1 text-xs text-zinc-500">只读执行、错误重试和安全约束</div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-[32px] border-zinc-200 bg-black text-white shadow-[0_12px_28px_rgba(0,0,0,0.12)]">
          <CardContent className="p-8">
            <div className="flex items-center gap-3">
              <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-white/10">
                <Sparkles className="h-6 w-6 text-white" />
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.3em] text-white/45">Starter kit</div>
                <h3 className="mt-1 text-2xl text-white">快速开始</h3>
              </div>
            </div>

            <div className="mt-6 space-y-4">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-sm font-semibold text-white">1. 选择左侧数据库连接</div>
                <p className="mt-1 text-sm leading-6 text-white/60">确认在线状态后，Agent 会自动读取 schema 上下文。</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-sm font-semibold text-white">2. 直接描述业务问题</div>
                <p className="mt-1 text-sm leading-6 text-white/60">比如“上个月各城市订单金额排行”或“复购率最高的客户”。</p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-sm font-semibold text-white">3. 查看 SQL、结果与图表</div>
                <p className="mt-1 text-sm leading-6 text-white/60">你可以继续追问，也可以手动编辑 SQL 再执行。</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2">
        {exampleCategories.map((category) => {
          const categoryQuestions = exampleQuestions.filter((q) => q.category === category);
          const icon = categoryIcons[category] || <Search className="h-5 w-5" />;

          return (
            <Card
              key={category}
              className="rounded-[28px] border-zinc-200 bg-white text-left shadow-[0_12px_28px_rgba(0,0,0,0.05)] transition-all hover:-translate-y-0.5 hover:shadow-[0_16px_30px_rgba(0,0,0,0.08)]"
            >
              <CardContent className="p-5">
                <div className="mb-4 flex items-center gap-3">
                  <div className="rounded-2xl bg-black p-3 text-white">
                    {icon}
                  </div>
                  <div>
                    <h3 className="font-semibold text-black">{category}</h3>
                    <p className="text-xs text-zinc-500">选择一个问题，立刻发起查询</p>
                  </div>
                  <Badge variant="outline" className="ml-auto rounded-full border-zinc-200 bg-zinc-50 text-zinc-600">
                    {categoryQuestions.length}
                  </Badge>
                </div>
                <div className="space-y-2">
                  {categoryQuestions.slice(0, 3).map((example, idx) => (
                    <button
                      key={idx}
                      onClick={() => onSelectQuery(example.question)}
                      className="group w-full rounded-2xl border border-zinc-200 bg-white p-4 text-left text-sm transition-all hover:border-zinc-300 hover:bg-zinc-50"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="font-medium text-black transition-colors group-hover:text-black">
                            {example.question}
                          </div>
                          <div className="mt-1 text-xs text-zinc-500">
                            {example.description}
                          </div>
                        </div>
                        <ArrowRight className="mt-0.5 h-4 w-4 text-zinc-400 transition-transform group-hover:translate-x-1 group-hover:text-black" />
                      </div>
                    </button>
                  ))}
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      <Card className="mt-6 rounded-[32px] border-zinc-200 bg-white shadow-[0_12px_28px_rgba(0,0,0,0.05)]">
        <CardContent className="grid gap-4 p-6 md:grid-cols-3">
          <div className="rounded-3xl bg-zinc-50 p-5">
            <div className="text-xs uppercase tracking-[0.28em] text-zinc-500">Think</div>
            <div className="mt-2 text-lg font-semibold text-black">自然语言理解</div>
            <p className="mt-2 text-sm leading-6 text-zinc-600">自动拆解你的业务问题，识别时间条件、聚合意图和指标需求。</p>
          </div>
          <div className="rounded-3xl bg-zinc-50 p-5">
            <div className="text-xs uppercase tracking-[0.28em] text-zinc-500">Generate</div>
            <div className="mt-2 text-lg font-semibold text-black">SQL 与结果联动</div>
            <p className="mt-2 text-sm leading-6 text-zinc-600">生成 SQL 后直接执行，并返回表格、分页和图表建议。</p>
          </div>
          <div className="rounded-3xl bg-zinc-50 p-5">
            <div className="text-xs uppercase tracking-[0.28em] text-zinc-500">Refine</div>
            <div className="mt-2 text-lg font-semibold text-black">可追问、可编辑、可回放</div>
            <p className="mt-2 text-sm leading-6 text-zinc-600">继续追问上一条结果，或手动调整 SQL，保留完整历史上下文。</p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
