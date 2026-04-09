from __future__ import annotations

import re

from app.llm.client import LLMClient
from app.prompts.error_reflection import SYSTEM_PROMPT
from app.schemas.connection import TableSchema
from app.agent.utils import build_user_friendly_error, sanitize_error_message


class ErrorReflector:
    def __init__(self, llm_client: LLMClient):
        self.llm_client = llm_client
        self.last_cache_hit = False

    async def reflect(
        self,
        question: str,
        failed_sql: str,
        error_message: str,
        schema_context: list[TableSchema],
    ) -> tuple[str, str, float]:
        safe_error_message = sanitize_error_message(error_message)
        prompt = (
            f"Question: {question}\nFailed SQL: {failed_sql}\nError: {safe_error_message}\n"
            f"Schema: {[table.name for table in schema_context]}"
        )
        corrected_sql, latency = await self.llm_client.chat(SYSTEM_PROMPT, prompt)
        self.last_cache_hit = getattr(self.llm_client, "last_cache_hit", False)
        corrected_sql = self._extract_sql(corrected_sql) or self._fallback(failed_sql, error_message, schema_context)
        error_type, analysis, suggestion = build_user_friendly_error(error_message)
        analysis_text = f"{analysis} {suggestion or ''}".strip()
        if error_type == "connection_error":
            corrected_sql = failed_sql
        return analysis_text, corrected_sql, latency

    def _fallback(self, failed_sql: str, error_message: str, schema_context: list[TableSchema]) -> str:
        if "no such table" in error_message.lower() and schema_context:
            replacement = schema_context[0].name
            return re.sub(r"from\s+\S+", f"FROM {replacement}", failed_sql, flags=re.IGNORECASE)
        return failed_sql

    def _extract_sql(self, text: str | None) -> str:
        if not text:
            return ""

        candidate = text.strip()
        fenced_match = re.search(r"```sql\s*(.*?)```", candidate, flags=re.IGNORECASE | re.DOTALL)
        if fenced_match:
            candidate = fenced_match.group(1).strip()
        else:
            block_match = re.search(
                r"(?:修复后的\s*SQL\s*[:：]|Corrected\s*SQL\s*[:：])\s*(.*)",
                candidate,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if block_match:
                candidate = block_match.group(1).strip()

        lines = [line.strip() for line in candidate.splitlines() if line.strip()]
        if not lines:
            return ""

        sql_lines: list[str] = []
        collecting = False
        for line in lines:
            normalized = line.lower()
            if normalized.startswith(("select ", "with ", "insert ", "update ", "delete ")):
                collecting = True
            if collecting:
                sql_lines.append(line)
        extracted = "\n".join(sql_lines).strip() if sql_lines else candidate
        extracted = extracted.strip().strip("`")
        extracted = re.sub(r"^sql\s*", "", extracted, flags=re.IGNORECASE).strip()
        if not re.match(r"^(select|with|insert|update|delete)\b", extracted, flags=re.IGNORECASE):
            return ""
        return extracted
