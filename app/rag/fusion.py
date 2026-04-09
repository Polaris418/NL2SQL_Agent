from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence


@dataclass(slots=True)
class RetrievalCandidate:
    key: str
    payload: Any
    score: float = 0.0
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FusedCandidate:
    key: str
    payload: Any
    score: float
    source_scores: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    rank: int = 0


class FusionStrategy(Protocol):
    def fuse(
        self,
        sources: Mapping[str, Sequence[RetrievalCandidate]],
    ) -> list[FusedCandidate]:
        ...


def normalize_scores(scores: Sequence[float]) -> list[float]:
    if not scores:
        return []
    minimum = min(scores)
    maximum = max(scores)
    if maximum == minimum:
        return [1.0 for _ in scores]
    return [(score - minimum) / (maximum - minimum) for score in scores]


class ReciprocalRankFusion:
    """RRF with stable score aggregation across independent retrievers."""

    def __init__(self, k: int = 60):
        self.k = k

    def fuse(self, sources: Mapping[str, Sequence[RetrievalCandidate]]) -> list[FusedCandidate]:
        merged: dict[str, FusedCandidate] = {}
        for source_name, candidates in sources.items():
            for rank, candidate in enumerate(candidates, start=1):
                score = 1.0 / (self.k + rank)
                fused = merged.setdefault(
                    candidate.key,
                    FusedCandidate(
                        key=candidate.key,
                        payload=candidate.payload,
                        score=0.0,
                        source_scores={},
                        metadata=dict(candidate.metadata),
                        rank=rank,
                    ),
                )
                fused.score += score
                fused.source_scores[source_name] = fused.source_scores.get(source_name, 0.0) + score
                fused.metadata.update(candidate.metadata)
                fused.payload = candidate.payload
                fused.rank = min(fused.rank, rank) if fused.rank else rank
        return sorted(merged.values(), key=lambda item: item.score, reverse=True)


class WeightedFusion:
    """Weighted fusion with per-source normalization."""

    def __init__(self, weights: Mapping[str, float] | None = None):
        self.weights = dict(weights or {})

    def fuse(self, sources: Mapping[str, Sequence[RetrievalCandidate]]) -> list[FusedCandidate]:
        merged: dict[str, FusedCandidate] = {}
        for source_name, candidates in sources.items():
            normalized = normalize_scores([candidate.score for candidate in candidates])
            weight = self.weights.get(source_name, 1.0)
            for rank, (candidate, normalized_score) in enumerate(zip(candidates, normalized), start=1):
                weighted = normalized_score * weight
                fused = merged.setdefault(
                    candidate.key,
                    FusedCandidate(
                        key=candidate.key,
                        payload=candidate.payload,
                        score=0.0,
                        source_scores={},
                        metadata=dict(candidate.metadata),
                        rank=rank,
                    ),
                )
                fused.score += weighted
                fused.source_scores[source_name] = fused.source_scores.get(source_name, 0.0) + weighted
                fused.metadata.update(candidate.metadata)
                fused.payload = candidate.payload
                fused.rank = min(fused.rank, rank) if fused.rank else rank
        return sorted(merged.values(), key=lambda item: item.score, reverse=True)
