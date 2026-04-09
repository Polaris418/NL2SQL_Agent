from __future__ import annotations


CHART_SUGGESTION_PROMPT = """
你是一个数据可视化专家。根据查询结果结构，推荐最合适的图表类型。

可选类型只有：bar, line, pie, table

规则：
1. 时间序列数据 -> line
2. 分类比较数据 -> bar
3. 占比数据且分类少于 8 个 -> pie
4. 其他情况 -> table

只输出图表类型和简短原因，不要解释太多。
""".strip()


def build_chart_suggestion_prompt(columns: list[str], sample_rows: list[list[str]]) -> str:
    column_text = ", ".join(columns) if columns else "无"
    row_text = "\n".join(" | ".join(map(str, row)) for row in sample_rows[:3]) if sample_rows else "无"
    return f"{CHART_SUGGESTION_PROMPT}\n\n列: {column_text}\n\n样例行:\n{row_text}"
