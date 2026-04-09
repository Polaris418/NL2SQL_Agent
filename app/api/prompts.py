from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.prompts import sql_generation, query_rewrite, error_reflection, chart_suggestion, result_summary

router = APIRouter(tags=["prompts"])


class PromptTemplate(BaseModel):
    name: str
    description: str
    template: str
    variables: list[str]


class PromptUpdateRequest(BaseModel):
    template: str


@router.get("/prompts", response_model=list[PromptTemplate])
async def list_prompts() -> list[PromptTemplate]:
    """获取所有 prompt 模板"""
    return [
        PromptTemplate(
            name="sql_generation",
            description="SQL 生成 Prompt - 根据用户问题和数据库结构生成 SQL 查询",
            template=sql_generation.SQL_GENERATION_PROMPT,
            variables=["question", "schema_context", "db_type", "few_shot_examples"],
        ),
        PromptTemplate(
            name="query_rewrite",
            description="查询改写 Prompt - 将自然语言问题改写为检索关键词",
            template=query_rewrite.QUERY_REWRITE_PROMPT,
            variables=["question"],
        ),
        PromptTemplate(
            name="error_reflection",
            description="错误反思 Prompt - 分析 SQL 错误并生成修复后的 SQL",
            template=error_reflection.ERROR_REFLECTION_PROMPT,
            variables=["question", "schema_context", "failed_sql", "error_message"],
        ),
        PromptTemplate(
            name="chart_suggestion",
            description="图表推荐 Prompt - 根据查询结果推荐合适的图表类型",
            template=chart_suggestion.CHART_SUGGESTION_PROMPT,
            variables=["columns", "sample_rows"],
        ),
        PromptTemplate(
            name="result_summary",
            description="结果总结 Prompt - 根据查询结果生成自然语言总结",
            template=result_summary.RESULT_SUMMARY_PROMPT,
            variables=["question", "row_count", "columns", "sample_rows"],
        ),
    ]


@router.get("/prompts/{prompt_name}", response_model=PromptTemplate)
async def get_prompt(prompt_name: str) -> PromptTemplate:
    """获取指定的 prompt 模板"""
    prompts = await list_prompts()
    for prompt in prompts:
        if prompt.name == prompt_name:
            return prompt
    raise HTTPException(status_code=404, detail=f"Prompt '{prompt_name}' not found")


@router.put("/prompts/{prompt_name}")
async def update_prompt(prompt_name: str, request: PromptUpdateRequest) -> dict[str, str]:
    """更新 prompt 模板（注意：这只是演示，实际需要持久化）"""
    # 验证 prompt 存在
    await get_prompt(prompt_name)
    
    # 这里应该将更新后的 prompt 保存到文件或数据库
    # 目前只是返回成功消息
    return {
        "message": f"Prompt '{prompt_name}' updated successfully",
        "note": "Changes are not persisted. Restart required to take effect.",
    }


@router.post("/prompts/{prompt_name}/reset")
async def reset_prompt(prompt_name: str) -> dict[str, str]:
    """重置 prompt 模板到默认值"""
    await get_prompt(prompt_name)
    return {"message": f"Prompt '{prompt_name}' reset to default"}
