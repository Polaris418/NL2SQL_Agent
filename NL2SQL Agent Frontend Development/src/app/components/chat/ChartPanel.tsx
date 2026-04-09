import { useState } from 'react';
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Card } from '../ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Label } from '../ui/label';
import type { ExecutionResult, ChartSuggestion } from '../../types/query';

interface ChartPanelProps {
  result: ExecutionResult;
  suggestion?: ChartSuggestion;
}

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D', '#FFC658', '#FF6B6B'];

export function ChartPanel({ result, suggestion }: ChartPanelProps) {
  // 使用新的字段名: chart_type, x_axis, y_axis
  const [chartType, setChartType] = useState<'bar' | 'line' | 'pie'>(
    suggestion?.chart_type === 'table' ? 'bar' : (suggestion?.chart_type || 'bar')
  );
  
  // 初始化 x 和 y 轴字段
  const [xField, setXField] = useState<string>(
    suggestion?.x_axis || result.columns[0] || ''
  );
  const [yField, setYField] = useState<string>(
    suggestion?.y_axis || result.columns[1] || ''
  );

  // 转换数据为图表格式 - 处理对象数组
  const chartData = result.rows.map((row) => {
    // row 现在是对象，直接返回
    return row;
  });

  // 如果数据太多，只取前20条
  const limitedData = chartData.slice(0, 20);

  // 检查是否有数值型数据
  const hasNumericData = result.rows.some((row) =>
    Object.values(row).some((cell) => typeof cell === 'number')
  );

  // 找出所有数值型字段
  const numericFields = result.columns.filter((col) => {
    return result.rows.some((row) => typeof row[col] === 'number');
  });

  // 找出所有非数值型字段（用于 X 轴）
  const categoricalFields = result.columns.filter((col) => {
    return !numericFields.includes(col);
  });

  if (!hasNumericData || result.columns.length < 2) {
    return null;
  }

  return (
    <Card className="p-4 mb-4">
      <Tabs value={chartType} onValueChange={(v) => setChartType(v as any)}>
        <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
          <span className="text-sm font-medium">数据可视化</span>
          <TabsList>
            <TabsTrigger value="bar">柱状图</TabsTrigger>
            <TabsTrigger value="line">折线图</TabsTrigger>
            <TabsTrigger value="pie">饼图</TabsTrigger>
          </TabsList>
        </div>

        {/* 轴选择器 */}
        <div className="grid grid-cols-2 gap-4 mb-4">
          <div className="space-y-2">
            <Label className="text-xs">X 轴（分类）</Label>
            <Select value={xField} onValueChange={setXField}>
              <SelectTrigger className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {(categoricalFields.length > 0 ? categoricalFields : result.columns).map((col) => (
                  <SelectItem key={col} value={col}>
                    {col}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label className="text-xs">Y 轴（数值）</Label>
            <Select value={yField} onValueChange={setYField}>
              <SelectTrigger className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {(numericFields.length > 0 ? numericFields : result.columns).map((col) => (
                  <SelectItem key={col} value={col}>
                    {col}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <div className="h-[400px]">
          <TabsContent value="bar" className="h-full m-0">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={limitedData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey={xField} />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey={yField} fill="#0088FE" />
              </BarChart>
            </ResponsiveContainer>
          </TabsContent>

          <TabsContent value="line" className="h-full m-0">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={limitedData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey={xField} />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey={yField} stroke="#0088FE" strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </TabsContent>

          <TabsContent value="pie" className="h-full m-0">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={limitedData}
                  dataKey={yField}
                  nameKey={xField}
                  cx="50%"
                  cy="50%"
                  outerRadius={120}
                  label
                >
                  {limitedData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </TabsContent>
        </div>
      </Tabs>

      {limitedData.length < chartData.length && (
        <p className="text-xs text-muted-foreground mt-2">
          * 为了更好的显示效果，图表仅展示前 20 条数据
        </p>
      )}
    </Card>
  );
}