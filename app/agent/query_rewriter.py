from __future__ import annotations

import re

from app.llm.client import LLMClient
from app.prompts.query_rewrite import SYSTEM_PROMPT
from app.agent.utils import extract_identifier_candidates


class QueryRewriter:
    def __init__(self, llm_client: LLMClient):
        self.llm_client = llm_client
        self.cache: dict[str, str] = {}
        self.last_cache_hit = False

    async def rewrite(self, question: str) -> tuple[str, float]:
        if question in self.cache:
            self.last_cache_hit = True
            return self.cache[question], 0.0
        rewritten, latency = await self.llm_client.chat(SYSTEM_PROMPT, question)
        self.last_cache_hit = getattr(self.llm_client, "last_cache_hit", False)
        rewritten = rewritten.strip() or self._fallback(question)
        self.cache[question] = rewritten
        return rewritten, latency

    def _fallback(self, question: str) -> str:
        lines = [line.strip() for line in question.splitlines() if line.strip()]
        relevant: list[str] = []
        for line in lines:
            if line.lower().startswith("question:"):
                relevant.append(line.split(":", 1)[1].strip())
            elif line.lower().startswith("follow-up instruction:"):
                relevant.append(line.split(":", 1)[1].strip())
        source = " ".join(relevant) if relevant else question
        tokens = extract_identifier_candidates(source)
        return " ".join(tokens[:20]) or source
