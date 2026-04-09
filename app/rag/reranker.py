from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
from typing import Any, Iterable

try:
    from sentence_transformers import CrossEncoder
except ImportError:  # pragma: no cover
    CrossEncoder = None

from app.agent.contracts import tokenize_text


@dataclass(slots=True)
class RerankerConfig:
    enabled: bool = True
    model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    timeout_seconds: float = 1.0
    max_candidates: int = 20


@dataclass(slots=True)
class RerankCandidate:
    key: str
    payload: Any
    score: float = 0.0
    source_scores: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    text: str = ""


class CrossEncoderReranker:
    """Reranker with cross-encoder support and heuristic fallback."""

    def __init__(self, config: RerankerConfig | None = None):
        self.config = config or RerankerConfig()
        self._model: CrossEncoder | None = None

    def _load_model(self) -> CrossEncoder | None:
        if not self.config.enabled or CrossEncoder is None:
            return None
        if self._model is None:
            self._model = CrossEncoder(self.config.model_name)
        return self._model

    async def rerank(self, query: str, candidates: Iterable[RerankCandidate]) -> list[RerankCandidate]:
        items = list(candidates)[: self.config.max_candidates]
        if not items:
            return []
        model = self._load_model()
        if model is None:
            return sorted(items, key=lambda item: self._heuristic_score(query, item), reverse=True)
        try:
            scores = await asyncio.wait_for(
                asyncio.to_thread(self._score_with_model, model, query, items),
                timeout=self.config.timeout_seconds,
            )
            ranked = [
                RerankCandidate(
                    key=item.key,
                    payload=item.payload,
                    score=float(score),
                    source_scores=dict(item.source_scores),
                    metadata=dict(item.metadata),
                    text=item.text,
                )
                for item, score in zip(items, scores)
            ]
            return sorted(ranked, key=lambda item: item.score, reverse=True)
        except Exception:
            return sorted(items, key=lambda item: self._heuristic_score(query, item), reverse=True)

    @staticmethod
    def _score_with_model(model: Any, query: str, items: list[RerankCandidate]) -> list[float]:
        pairs = [(query, item.text or item.metadata.get("text", "")) for item in items]
        scores = model.predict(pairs)
        return [float(score) for score in scores]

    @staticmethod
    def _heuristic_score(query: str, candidate: RerankCandidate) -> float:
        query_tokens = set(tokenize_text(query))
        candidate_tokens = set(tokenize_text(candidate.text)) | set(
            tokenize_text(" ".join(str(value) for value in candidate.metadata.values()))
        )
        overlap = len(query_tokens & candidate_tokens)
        source_score = max(candidate.source_scores.values(), default=candidate.score)
        length_bonus = min(len(candidate_tokens) / 50.0, 0.2)
        return source_score + overlap * 0.2 + length_bonus
