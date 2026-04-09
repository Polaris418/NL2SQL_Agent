"""Offline evaluation runner for production RAG upgrades."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field
from inspect import isawaitable
from pathlib import Path
from typing import Any, Protocol

from app.rag.synonym_dict import SynonymDictionary


def _normalize_table_name(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip().split(".")[-1].lower()
    candidate = (
        getattr(value, "name", None)
        or getattr(value, "table_name", None)
        or getattr(value, "table", None)
        or getattr(value, "id", None)
    )
    return str(candidate or "").strip().split(".")[-1].lower()


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


@dataclass(slots=True)
class EvaluationSample:
    question: str
    target_tables: list[str] = field(default_factory=list)
    target_fields: list[str] = field(default_factory=list)
    relationships: list[str] = field(default_factory=list)
    connection_id: str = ""
    domain: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EvaluationSample":
        target_tables = _coerce_str_list(
            payload.get("target_tables")
            or payload.get("tables")
            or payload.get("target_table")
            or payload.get("table")
        )
        return cls(
            question=str(payload.get("question", "")).strip(),
            target_tables=target_tables,
            target_fields=_coerce_str_list(payload.get("target_fields") or payload.get("fields")),
            relationships=_coerce_str_list(payload.get("relationships") or payload.get("joins")),
            connection_id=str(payload.get("connection_id") or payload.get("connection") or "").strip(),
            domain=(str(payload.get("domain")).strip() if payload.get("domain") else None),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(slots=True)
class EvaluationDataset:
    name: str
    samples: list[EvaluationSample]
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EvaluationDataset":
        raw_samples = payload.get("samples") or payload.get("items") or []
        if isinstance(raw_samples, dict):
            raw_samples = list(raw_samples.values())
        samples = [EvaluationSample.from_dict(sample) for sample in raw_samples if isinstance(sample, dict)]
        return cls(
            name=str(payload.get("name") or payload.get("dataset_name") or "rag-evaluation"),
            samples=samples,
            metadata=dict(payload.get("metadata") or {}),
        )

    @classmethod
    def from_json(cls, path: str | Path) -> "EvaluationDataset":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(payload, list):
            payload = {"samples": payload}
        if not isinstance(payload, dict):
            raise ValueError("Dataset JSON must be a list or an object")
        return cls.from_dict(payload)



@dataclass(slots=True)
class EvaluationSampleResult:
    question: str
    rewritten_question: str
    connection_id: str
    target_tables: list[str]
    retrieved_tables: list[str]
    first_hit_rank: int | None
    recall_at_k: dict[int, bool]
    table_not_found: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "question": self.question,
            "rewritten_question": self.rewritten_question,
            "connection_id": self.connection_id,
            "target_tables": list(self.target_tables),
            "retrieved_tables": list(self.retrieved_tables),
            "first_hit_rank": self.first_hit_rank,
            "recall_at_k": {str(k): v for k, v in self.recall_at_k.items()},
            "table_not_found": self.table_not_found,
        }


@dataclass(slots=True)
class EvaluationSummary:
    total_samples: int
    recall_at_k: dict[int, float]
    mrr: float
    top1_accuracy: float
    table_not_found_rate: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_samples": self.total_samples,
            "recall_at_k": {str(k): v for k, v in self.recall_at_k.items()},
            "mrr": self.mrr,
            "top1_accuracy": self.top1_accuracy,
            "table_not_found_rate": self.table_not_found_rate,
        }


@dataclass(slots=True)
class EvaluationReport:
    dataset_name: str
    summary: EvaluationSummary
    samples: list[EvaluationSampleResult]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "summary": self.summary.to_dict(),
            "samples": [sample.to_dict() for sample in self.samples],
            "metadata": dict(self.metadata),
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)


class RetrieverProtocol(Protocol):
    async def retrieve(self, query: str, connection_id: str, top_k: int = 8) -> list[Any]:
        ...


class EvaluationRunner:
    def __init__(
        self,
        retriever: RetrieverProtocol | Any,
        *,
        synonym_dictionary: SynonymDictionary | None = None,
        apply_synonyms: bool = True,
    ) -> None:
        self.retriever = retriever
        self.synonym_dictionary = synonym_dictionary or SynonymDictionary()
        self.apply_synonyms = apply_synonyms

    @staticmethod
    def load_dataset(path: str | Path) -> EvaluationDataset:
        return EvaluationDataset.from_json(path)

    async def run_evaluation(
        self,
        dataset: EvaluationDataset,
        *,
        top_k_values: tuple[int, ...] = (1, 3, 5, 10),
        default_connection_id: str | None = None,
    ) -> EvaluationReport:
        top_k_values = tuple(sorted({max(1, int(k)) for k in top_k_values}))
        max_k = max(top_k_values, default=1)
        total_samples = len(dataset.samples)
        recall_hits = {k: 0 for k in top_k_values}
        mrr_sum = 0.0
        top1_hits = 0
        table_not_found_hits = 0
        sample_results: list[EvaluationSampleResult] = []

        for sample in dataset.samples:
            connection_id = sample.connection_id or default_connection_id or ""
            rewritten_question = sample.question
            if self.apply_synonyms:
                rewritten_question = self.synonym_dictionary.rewrite(
                    rewritten_question,
                    connection_id=connection_id,
                    domain=sample.domain,
                )

            retrieved_tables = await self._retrieve_tables(rewritten_question, connection_id, max_k)
            target_tables = [_normalize_table_name(table) for table in sample.target_tables if _normalize_table_name(table)]
            target_table_set = set(target_tables)

            first_hit_rank: int | None = None
            for index, table in enumerate(retrieved_tables, start=1):
                if table in target_table_set:
                    first_hit_rank = index
                    break

            if first_hit_rank is None:
                table_not_found_hits += 1
            else:
                mrr_sum += 1.0 / first_hit_rank

            if retrieved_tables[:1] and retrieved_tables[0] in target_table_set:
                top1_hits += 1

            recall_at_k = {}
            for k in top_k_values:
                hit = any(table in target_table_set for table in retrieved_tables[:k])
                recall_at_k[k] = hit
                if hit:
                    recall_hits[k] += 1

            sample_results.append(
                EvaluationSampleResult(
                    question=sample.question,
                    rewritten_question=rewritten_question,
                    connection_id=connection_id,
                    target_tables=target_tables,
                    retrieved_tables=retrieved_tables,
                    first_hit_rank=first_hit_rank,
                    recall_at_k=recall_at_k,
                    table_not_found=first_hit_rank is None,
                )
            )

        summary = EvaluationSummary(
            total_samples=total_samples,
            recall_at_k={
                k: (recall_hits[k] / total_samples if total_samples else 0.0)
                for k in top_k_values
            },
            mrr=(mrr_sum / total_samples if total_samples else 0.0),
            top1_accuracy=(top1_hits / total_samples if total_samples else 0.0),
            table_not_found_rate=(table_not_found_hits / total_samples if total_samples else 0.0),
        )

        return EvaluationReport(
            dataset_name=dataset.name,
            summary=summary,
            samples=sample_results,
            metadata={
                "apply_synonyms": self.apply_synonyms,
                "top_k_values": list(top_k_values),
                "default_connection_id": default_connection_id,
            },
        )

    async def run_acceptance(
        self,
        dataset: EvaluationDataset,
        *,
        top_k_values: tuple[int, ...] = (1, 3, 5, 10),
        default_connection_id: str | None = None,
        min_recall_at_1: float | None = None,
        min_mrr: float | None = None,
        max_table_not_found_rate: float | None = None,
    ) -> dict[str, Any]:
        report = await self.run_evaluation(
            dataset,
            top_k_values=top_k_values,
            default_connection_id=default_connection_id,
        )
        checks = {
            "min_recall_at_1": min_recall_at_1,
            "min_mrr": min_mrr,
            "max_table_not_found_rate": max_table_not_found_rate,
        }
        passed = True
        reasons: list[str] = []
        if min_recall_at_1 is not None and report.summary.recall_at_k.get(1, 0.0) < min_recall_at_1:
            passed = False
            reasons.append(f"recall@1 {report.summary.recall_at_k.get(1, 0.0):.4f} < {min_recall_at_1:.4f}")
        if min_mrr is not None and report.summary.mrr < min_mrr:
            passed = False
            reasons.append(f"mrr {report.summary.mrr:.4f} < {min_mrr:.4f}")
        if max_table_not_found_rate is not None and report.summary.table_not_found_rate > max_table_not_found_rate:
            passed = False
            reasons.append(
                f"table_not_found_rate {report.summary.table_not_found_rate:.4f} > {max_table_not_found_rate:.4f}"
            )
        return {
            "passed": passed,
            "reasons": reasons,
            "checks": checks,
            "report": report.to_dict(),
        }

    async def run_stability_check(
        self,
        dataset: EvaluationDataset,
        *,
        rounds: int = 3,
        top_k_values: tuple[int, ...] = (1, 3, 5),
        default_connection_id: str | None = None,
    ) -> dict[str, Any]:
        rounds = max(1, int(rounds))
        reports = [
            await self.run_evaluation(
                dataset,
                top_k_values=top_k_values,
                default_connection_id=default_connection_id,
            )
            for _ in range(rounds)
        ]
        recall_1_values = [report.summary.recall_at_k.get(1, 0.0) for report in reports]
        mrr_values = [report.summary.mrr for report in reports]
        table_not_found_values = [report.summary.table_not_found_rate for report in reports]
        variability = {
            "recall_at_1_span": round(max(recall_1_values) - min(recall_1_values), 6) if recall_1_values else 0.0,
            "mrr_span": round(max(mrr_values) - min(mrr_values), 6) if mrr_values else 0.0,
            "table_not_found_rate_span": round(
                max(table_not_found_values) - min(table_not_found_values), 6
            )
            if table_not_found_values
            else 0.0,
        }
        return {
            "rounds": rounds,
            "reports": [report.to_dict() for report in reports],
            "variability": variability,
            "stable": all(value == 0.0 for value in variability.values()),
        }

    async def _retrieve_tables(self, query: str, connection_id: str, top_k: int) -> list[str]:
        retriever = getattr(self.retriever, "retrieve", None)
        if retriever is None and callable(self.retriever):
            retriever = self.retriever
        if retriever is None:
            raise TypeError("retriever must expose a retrieve method or be callable")

        result = retriever(query, connection_id, top_k=top_k)
        if isawaitable(result):
            result = await result

        tables: list[str] = []
        for item in result or []:
            table = _normalize_table_name(item)
            if table and table not in tables:
                tables.append(table)
        return tables

    async def run_and_save(
        self,
        dataset: EvaluationDataset,
        output_path: str | Path,
        *,
        top_k_values: tuple[int, ...] = (1, 3, 5, 10),
        default_connection_id: str | None = None,
    ) -> EvaluationReport:
        report = await self.run_evaluation(
            dataset,
            top_k_values=top_k_values,
            default_connection_id=default_connection_id,
        )
        Path(output_path).write_text(report.to_json(), encoding="utf-8")
        return report


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run offline evaluation for schema retrieval.")
    parser.add_argument("--dataset", required=True, help="Path to evaluation dataset JSON.")
    parser.add_argument("--output", help="Optional path to save the evaluation report JSON.")
    parser.add_argument("--synonyms", default="config/synonyms.json", help="Path to synonym dictionary JSON.")
    parser.add_argument("--connection-id", default=None, help="Fallback connection_id for samples without one.")
    parser.add_argument(
        "--top-k",
        default="1,3,5,10",
        help="Comma-separated top-k values for recall metrics.",
    )
    parser.add_argument("--mode", choices=["evaluate", "acceptance", "stability"], default="evaluate")
    parser.add_argument("--acceptance-min-recall-at-1", type=float, default=None)
    parser.add_argument("--acceptance-min-mrr", type=float, default=None)
    parser.add_argument("--acceptance-max-table-not-found-rate", type=float, default=None)
    parser.add_argument("--stability-rounds", type=int, default=3)
    return parser


async def _main_async(args: argparse.Namespace) -> int:
    dataset = EvaluationRunner.load_dataset(args.dataset)
    synonyms = SynonymDictionary.from_file(args.synonyms)
    runner = EvaluationRunner(retriever=_PassthroughRetriever(), synonym_dictionary=synonyms)
    top_k_values = tuple(int(value) for value in args.top_k.split(",") if value.strip())
    if args.mode == "acceptance":
        payload = await runner.run_acceptance(
            dataset,
            top_k_values=top_k_values,
            default_connection_id=args.connection_id,
            min_recall_at_1=args.acceptance_min_recall_at_1,
            min_mrr=args.acceptance_min_mrr,
            max_table_not_found_rate=args.acceptance_max_table_not_found_rate,
        )
        output_text = json.dumps(payload, ensure_ascii=False, indent=2)
    elif args.mode == "stability":
        payload = await runner.run_stability_check(
            dataset,
            rounds=args.stability_rounds,
            top_k_values=top_k_values,
            default_connection_id=args.connection_id,
        )
        output_text = json.dumps(payload, ensure_ascii=False, indent=2)
    else:
        report = await runner.run_evaluation(
            dataset,
            top_k_values=top_k_values,
            default_connection_id=args.connection_id,
        )
        output_text = report.to_json()
    if args.output:
        Path(args.output).write_text(output_text, encoding="utf-8")
    else:
        print(output_text)
    return 0


class _PassthroughRetriever:
    async def retrieve(self, query: str, connection_id: str, top_k: int = 8) -> list[Any]:
        return []


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)
    return asyncio.run(_main_async(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
