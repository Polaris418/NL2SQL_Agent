from __future__ import annotations

from app.llm.client import LLMClient
from app.prompts.result_summary import build_result_summary_prompt
from app.schemas.query import ExecutionResult


class ResultSummarizer:
    """生成查询结果的自然语言总结"""

    def __init__(self, llm_client: LLMClient):
        self.llm_client = llm_client

    async def summarize(
        self,
        question: str,
        result: ExecutionResult | None,
    ) -> str | None:
        """
        根据用户问题和查询结果生成总结
        
        Args:
            question: 用户的原始问题
            result: SQL 查询结果
            
        Returns:
            总结文本，如果结果为空或生成失败则返回 None
        """
        if result is None or result.row_count == 0:
            return None

        # 准备数据
        columns = result.columns
        sample_rows = [
            [str(row.get(col, "")) for col in columns]
            for row in result.rows[:5]  # 只取前5行
        ]

        # 构建 prompt
        prompt = build_result_summary_prompt(
            question=question,
            row_count=result.row_count,
            columns=columns,
            sample_rows=sample_rows,
        )

        try:
            # 调用 LLM 生成总结
            summary, _ = await self.llm_client.chat(
                system_prompt="你是一个数据分析助手，擅长用简洁的中文总结查询结果。",
                user_prompt=prompt,
            )
            return summary.strip() if summary else None
        except Exception:  # noqa: BLE001
            # 如果 LLM 调用失败，返回 None
            return None
