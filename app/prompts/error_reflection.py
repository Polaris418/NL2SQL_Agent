from __future__ import annotations


ERROR_REFLECTION_PROMPT = """
你生成的 SQL 执行时出现了错误，请分析原因并修复。

## 原始问题
{question}

## 数据库结构
{schema_context}

## 出错的 SQL
{failed_sql}

## 错误信息
{error_message}

## 输出格式
错误原因：[你的分析]

修复后的 SQL:
""".strip()

SYSTEM_PROMPT = ERROR_REFLECTION_PROMPT


def build_error_reflection_prompt(question: str, schema_context: str, failed_sql: str, error_message: str) -> str:
    return ERROR_REFLECTION_PROMPT.format(
        question=question.strip(),
        schema_context=schema_context.strip() or "无",
        failed_sql=failed_sql.strip(),
        error_message=error_message.strip(),
    )
