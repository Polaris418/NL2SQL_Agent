from __future__ import annotations

import unittest
from app.rag.debug_view import build_debug_view_from_manager, build_query_debug_view


class _FakeStatus:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def model_dump(self, mode: str = "json") -> dict[str, object]:
        return dict(self._payload)


class _FakeManager:
    def __init__(self, query_details: dict[str, dict[str, object]]) -> None:
        self._query_details = query_details

    def get_query_details(self, query_id: str):
        return self._query_details.get(query_id)

    def get_runtime_metrics(self):
        return {
            "retrieval_p50_ms": 7.5,
            "retrieval_p95_ms": 12.0,
            "cache_hit_rate": 0.75,
        }

    def get_status(self, connection_id: str):
        return _FakeStatus(
            {
                "connection_id": connection_id,
                "index_status": "ready",
                "health_status": "healthy",
                "index_mode": "hybrid",
            }
        )


class RAGDebugViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.query_log = {
            "query_id": "query-001",
            "connection_id": "conn-log",
            "original_query": "统计每个部门的员工数量",
            "rewritten_query": "部门 员工 数量",
            "expanded_query": "部门 员工 数量 组织结构",
            "applied_synonyms": [("部门", "department")],
            "selected_tables": ["employees", "departments"],
            "reranked_tables": ["departments", "employees"],
            "candidate_scores": [
                {
                    "table_name": "employees",
                    "score": 0.92,
                    "source": "vector",
                    "rank": 1,
                    "source_scores": {"vector": 0.92, "bm25": 0.55},
                    "join_path": "employees.department_id -> departments.id",
                },
                {
                    "table_name": "departments",
                    "score": 0.84,
                    "source": "bm25",
                    "rank": 2,
                    "source_scores": {"vector": 0.8, "bm25": 0.84},
                },
            ],
            "retrieval_latency_ms": 9.8,
            "embedding_latency_ms": 3.4,
            "stage_latencies": {"rewrite": 1.2, "retrieval": 9.8, "prompt_pack": 0.5},
            "prompt_schema": "schema block",
            "final_sql": "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id",
            "failure_category": "table_not_found",
            "failure_stage": "sql_generation",
            "degradation_mode": "bm25_only",
            "cache_hit": True,
            "used_fallback": False,
            "access_metadata": {"tenant_id": "tenant-a", "project_id": "project-a"},
            "input_validation": {"issues": [{"code": "ok", "message": "passed"}]},
        }
        self.query_details = {
            "query_id": "query-001",
            "connection_id": "conn-details",
            "rewritten_query": "部门 员工 数量 统计",
            "applied_synonyms": [("员工", "employee")],
            "runtime": {"ignored": True},
        }

    def test_build_query_debug_view_merges_and_normalizes_payload(self) -> None:
        view = build_query_debug_view(
            "query-001",
            query_log=self.query_log,
            query_details=self.query_details,
            runtime_metrics={"retrieval_p50_ms": 7.5, "cache_hit_rate": 0.75},
            index_state={"index_status": "ready", "health_status": "healthy"},
        )

        self.assertEqual(view.query_id, "query-001")
        self.assertEqual(view.connection_id, "conn-details")
        self.assertIn("Query: 统计每个部门的员工数量", view.summary)
        self.assertIn("Selected tables: employees, departments", view.summary)
        self.assertEqual(view.query.original_query, "统计每个部门的员工数量")
        self.assertEqual(view.query.rewritten_query, "部门 员工 数量 统计")
        self.assertEqual(view.query.applied_synonyms, [("员工", "employee")])
        self.assertEqual(view.query.selected_tables, ["employees", "departments"])
        self.assertEqual(view.query.reranked_tables, ["departments", "employees"])
        self.assertEqual(view.query.candidate_count, 2)
        self.assertEqual(view.query.selected_count, 2)
        self.assertEqual(view.candidates[0].table_name, "employees")
        self.assertEqual(view.candidates[0].rank, 1)
        self.assertAlmostEqual(view.candidates[0].score, 0.92)
        self.assertAlmostEqual(view.candidates[0].source_scores["vector"], 0.92)
        self.assertAlmostEqual(view.timings.retrieval_latency_ms, 9.8)
        self.assertAlmostEqual(view.timings.embedding_latency_ms, 3.4)
        self.assertEqual(view.timings.stage_latencies["prompt_pack"], 0.5)
        self.assertEqual(view.artifacts.prompt_schema, "schema block")
        self.assertEqual(view.artifacts.final_sql, "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id")
        self.assertEqual(view.failure_category, "table_not_found")
        self.assertEqual(view.failure_stage, "sql_generation")
        self.assertEqual(view.degradation_mode, "bm25_only")
        self.assertTrue(view.cache_hit)
        self.assertFalse(view.used_fallback)
        self.assertEqual(view.access_metadata["tenant_id"], "tenant-a")
        self.assertEqual(view.input_validation["issues"][0]["code"], "ok")
        self.assertEqual(view.runtime_metrics["retrieval_p50_ms"], 7.5)
        self.assertEqual(view.index_state["index_status"], "ready")
        self.assertEqual(view.raw_log["query_id"], "query-001")

    def test_build_debug_view_from_manager_uses_status_and_runtime(self) -> None:
        manager = _FakeManager({"query-002": self.query_log | {"query_id": "query-002", "connection_id": "conn-002"}})

        view = build_debug_view_from_manager(manager, "query-002")

        self.assertIsNotNone(view)
        self.assertEqual(view.query_id, "query-002")
        self.assertEqual(view.connection_id, "conn-002")
        self.assertEqual(view.index_state["index_status"], "ready")
        self.assertEqual(view.index_state["health_status"], "healthy")
        self.assertEqual(view.runtime_metrics["cache_hit_rate"], 0.75)

    def test_build_debug_view_from_manager_returns_none_when_missing(self) -> None:
        manager = _FakeManager({})
        self.assertIsNone(build_debug_view_from_manager(manager, "missing-query"))


if __name__ == "__main__":
    unittest.main()
