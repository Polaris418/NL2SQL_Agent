from __future__ import annotations


RESULT_SUMMARY_PROMPT = """
你是一个数据分析助手。根据用户的问题和查询结果，用简洁的中文总结关键发现。

## 用户问题
{question}

## 查询结果
行数: {row_count}
列: {columns}

前几行数据:
{sample_rows}

## 要求
1. 用 1-3 句话总结关键发现
2. 突出重要的数字和趋势
3. 使用自然、口语化的表达
4. 如果结果为空，说明没有找到相关数据
5. 不要重复用户的问题，直接给出结论

示例：
问题："查询最近一周的活跃用户"
结果：7 行，列 [user_id, username, activity_count]
总结："最近一周共有 7 位活跃用户，其中用户 'alice' 的活动次数最多，达到 45 次。"

现在请总结：
""".strip()

SYSTEM_PROMPT = RESULT_SUMMARY_PROMPT


def build_result_summary_prompt(
    question: str,
    row_count: int,
    columns: list[str],
    sample_rows: list[list[str]],
) -> str:
    columns_text = ", ".join(columns) if columns else "无"
    
    # 格式化前几行数据
    if sample_rows:
        rows_text = "\n".join(
            " | ".join(str(cell) for cell in row)
            for row in sample_rows[:5]  # 只取前5行
        )
    else:
        rows_text = "无数据"
    
    return RESULT_SUMMARY_PROMPT.format(
        question=question.strip(),
        row_count=row_count,
        columns=columns_text,
        sample_rows=rows_text,
    )
