export interface ExampleQuestion {
  category: string;
  question: string;
  description: string;
}

export const exampleQuestions: ExampleQuestion[] = [
  {
    category: '数据统计',
    question: '统计每个部门的员工数量',
    description: '使用 COUNT 和 GROUP BY 进行分组统计',
  },
  {
    category: '数据统计',
    question: '计算过去30天的订单总金额',
    description: '使用日期过滤和 SUM 聚合函数',
  },
  {
    category: '数据统计',
    question: '查询销售额排名前10的产品',
    description: '使用 ORDER BY 和 LIMIT 进行排序和限制',
  },
  {
    category: '数据分析',
    question: '分析各地区的销售趋势',
    description: '按地区和时间维度进行销售分析',
  },
  {
    category: '数据分析',
    question: '找出复购率最高的客户',
    description: '分析客户购买频次和金额',
  },
  {
    category: '数据分析',
    question: '对比本月与上月的销售业绩',
    description: '使用时间窗口进行同环比分析',
  },
  {
    category: '数据查询',
    question: '查询最近一周的活跃用户',
    description: '根据最近活动时间筛选用户',
  },
  {
    category: '数据查询',
    question: '列出所有未完成的订单',
    description: '根据订单状态进行筛选',
  },
  {
    category: '数据查询',
    question: '显示库存不足的商品',
    description: '根据库存阈值筛选商品',
  },
  {
    category: '关联查询',
    question: '查询每个订单的详细信息包括客户和产品',
    description: '使用 JOIN 进行多表关联查询',
  },
  {
    category: '关联查询',
    question: '找出从未下单的客户',
    description: '使用 LEFT JOIN 和 NULL 过滤',
  },
  {
    category: '关联查询',
    question: '统计每个分类下的产品数量和平均价格',
    description: '跨表聚合统计',
  },
];

export const exampleCategories = Array.from(
  new Set(exampleQuestions.map((q) => q.category))
);
