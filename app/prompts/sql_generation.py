from __future__ import annotations

from .few_shot_examples import format_few_shot_examples


SQL_GENERATION_PROMPT = """
你是一个资深的 {db_type} 数据库工程师。根据以下数据库结构和用户问题，生成准确的 SQL 查询语句。

## 数据库结构
{schema_context}

## 规则
1. 只生成 SELECT 语句，禁止任何写操作
2. 使用标准 {db_type} 语法
3. 对于日期，使用数据库原生函数
4. 添加适当的 LIMIT（默认 1000 行）
5. 只输出 SQL，不要解释，不要 markdown 代码块

## 示例
{few_shot_examples}

## 用户问题
{question}

SQL:
""".strip()

SYSTEM_PROMPT = SQL_GENERATION_PROMPT


def build_sql_generation_prompt(question: str, schema_context: str, db_type: str) -> str:
    return SQL_GENERATION_PROMPT.format(
        question=question.strip(),
        schema_context=schema_context.strip() or "无",
        db_type=db_type.strip().lower(),
        few_shot_examples=format_few_shot_examples(db_type),
    )
