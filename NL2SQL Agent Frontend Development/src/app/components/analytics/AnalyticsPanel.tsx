import { useEffect, useState } from 'react';
import { Activity, Clock, CheckCircle, TrendingUp, RefreshCw, XCircle, Zap } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui/card';
import { Button } from '../ui/button';
import {
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { analyticsApi } from '../../api/client';
import { toast } from 'sonner';

type ReportErrorItem = {
  type?: string;
  error_type?: string;
  count?: number;
  percentage?: number;
  recent_example?: string;
};

type ReportTopTableItem = {
  table?: string;
  table_name?: string;
  count?: number;
  query_count?: number;
  success_rate?: number;
};

type ReportSummary = {
  recent_success_rate?: number;
  average_llm_latency_ms?: number;
  average_db_latency_ms?: number;
  top_tables?: ReportTopTableItem[];
  error_distribution?: ReportErrorItem[];
};

type BackendAnalyticsReport = {
  summary: ReportSummary;
  errors?: ReportErrorItem[];
  top_tables?: ReportTopTableItem[];
  recent_queries?: Array<{
    query_id: string;
    question?: string;
    status?: string;
    created_at?: string;
    execution_time_ms?: number;
  }>;
  time_range?: {
    start_date: string;
    end_date: string;
  };
};

export function AnalyticsPanel() {
  const [report, setReport] = useState<BackendAnalyticsReport | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);

  useEffect(() => {
    fetchAnalytics();
  }, []);

  const fetchAnalytics = async (isRefresh = false) => {
    if (isRefresh) {
      setIsRefreshing(true);
    } else {
      setIsLoading(true);
    }

    try {
      const data = await analyticsApi.report();
      setReport(data as BackendAnalyticsReport);
      if (isRefresh) {
        toast.success('分析数据已刷新');
      }
    } catch (error: any) {
      toast.error(`加载分析数据失败: ${error.message}`);
    } finally {
      setIsLoading(false);
      setIsRefreshing(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin text-primary mx-auto mb-2" />
          <p className="text-muted-foreground">加载分析数据中...</p>
        </div>
      </div>
    );
  }

  if (!report) {
    return (
      <div className="flex items-center justify-center h-full">
        <Card>
          <CardContent className="flex flex-col items-center justify-center p-12">
            <XCircle className="h-12 w-12 text-muted-foreground mb-4" />
            <p className="text-muted-foreground mb-4">加载分析数据失败</p>
            <Button onClick={() => fetchAnalytics()}>
              <RefreshCw className="h-4 w-4 mr-2" />
              重试
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8'];
  const summary = report.summary ?? {};

  const normalizeErrors = (items: ReportErrorItem[] | undefined) =>
    (items ?? []).map((entry) => ({
      type: entry.type ?? entry.error_type ?? 'unknown',
      count: entry.count ?? 0,
      percentage: entry.percentage ?? 0,
    }));

  const normalizeTopTables = (items: ReportTopTableItem[] | undefined) =>
    (items ?? []).map((entry) => ({
      table: entry.table ?? entry.table_name ?? 'unknown',
      count: entry.count ?? entry.query_count ?? 0,
    }));

  const errorData = normalizeErrors(report.errors?.length ? report.errors : summary.error_distribution);
  const topTableData = normalizeTopTables(report.top_tables?.length ? report.top_tables : summary.top_tables);

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold mb-2">查询分析面板</h1>
          <p className="text-muted-foreground">查看系统使用情况和性能统计</p>
        </div>
        <Button onClick={() => fetchAnalytics(true)} disabled={isRefreshing}>
          <RefreshCw className={`h-4 w-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
          刷新数据
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">最近成功率</CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{(summary.recent_success_rate ?? 0).toFixed(1)}%</div>
            <p className="text-xs text-muted-foreground">最近一段时间查询成功率</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">平均 LLM 延迟</CardTitle>
            <CheckCircle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-600">
              {(summary.average_llm_latency_ms ?? 0).toFixed(0)}ms
            </div>
            <p className="text-xs text-muted-foreground">模型响应耗时</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">平均数据库延迟</CardTitle>
            <Clock className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{(summary.average_db_latency_ms ?? 0).toFixed(0)}ms</div>
            <p className="text-xs text-muted-foreground">SQL 执行耗时</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">高频表数量</CardTitle>
            <Zap className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{topTableData.length}</div>
            <p className="text-xs text-muted-foreground">当前报告中的热点数据表</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {errorData.length > 0 ? (
          <Card>
            <CardHeader>
              <CardTitle>错误类型分布</CardTitle>
              <CardDescription>各类错误的占比情况</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-[300px]">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={errorData}
                      dataKey="count"
                      nameKey="type"
                      cx="50%"
                      cy="50%"
                      outerRadius={100}
                      label={(entry: any) => `${entry.type} (${Number(entry.percentage ?? 0).toFixed(1)}%)`}
                    >
                      {errorData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        ) : (
          <Card>
            <CardHeader>
              <CardTitle>错误类型分布</CardTitle>
              <CardDescription>各类错误的占比情况</CardDescription>
            </CardHeader>
            <CardContent className="h-[300px] flex items-center justify-center">
              <p className="text-muted-foreground">暂无错误数据</p>
            </CardContent>
          </Card>
        )}

        {topTableData.length > 0 ? (
          <Card>
            <CardHeader>
              <CardTitle>高频查询表 Top 10</CardTitle>
              <CardDescription>最常被查询的数据表</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-[300px]">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={topTableData} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis type="number" />
                    <YAxis dataKey="table" type="category" width={120} />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="count" fill="#0088FE" name="查询次数" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        ) : (
          <Card>
            <CardHeader>
              <CardTitle>高频查询表 Top 10</CardTitle>
              <CardDescription>最常被查询的数据表</CardDescription>
            </CardHeader>
            <CardContent className="h-[300px] flex items-center justify-center">
              <p className="text-muted-foreground">暂无表查询数据</p>
            </CardContent>
          </Card>
        )}
      </div>

      {report.recent_queries?.length ? (
        <Card>
          <CardHeader>
            <CardTitle>最近查询记录</CardTitle>
            <CardDescription>最近 10 次查询的概览</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {report.recent_queries.map((query) => (
                <div
                  key={query.query_id}
                  className="flex items-center justify-between p-3 border rounded-lg hover:bg-muted/50 transition-colors"
                >
                  <div className="flex-1 min-w-0 mr-4">
                    <p className="text-sm font-medium truncate">{query.question || query.query_id}</p>
                    <p className="text-xs text-muted-foreground">
                      {query.created_at ? new Date(query.created_at).toLocaleString('zh-CN') : '未知时间'}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    {query.status === 'success' ? (
                      <CheckCircle className="h-4 w-4 text-green-500" />
                    ) : (
                      <XCircle className="h-4 w-4 text-red-500" />
                    )}
                    {query.execution_time_ms !== undefined && (
                      <span className="text-xs text-muted-foreground">{query.execution_time_ms}ms</span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      ) : null}

      {errorData.length === 0 && topTableData.length === 0 && (
        <Card>
          <CardContent className="flex flex-col items-center justify-center h-[200px]">
            <Activity className="h-12 w-12 text-muted-foreground mb-4 opacity-20" />
            <p className="text-muted-foreground text-center">
              暂无查询数据
              <br />
              开始执行查询后，这里将显示分析统计
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
