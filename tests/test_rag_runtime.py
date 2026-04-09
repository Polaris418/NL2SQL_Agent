import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from app.rag.cache import RetrievalCache
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.query_rewriter import QueryRewriter
from app.rag.sharding import SchemaShardPlanner
from app.rag.synonym_dict import SynonymDictionary
from app.rag.vector_store import InMemoryVectorStore
from app.schemas.connection import ColumnInfo, TableSchema


def _make_table(name: str, *columns: str) -> TableSchema:
    return TableSchema(
        name=name,
        columns=[ColumnInfo(name=column, type="TEXT") for column in columns],
    )


class QueryRewriterTests(unittest.TestCase):
    def test_query_rewriter_applies_synonyms_and_expands_terms(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rag_rewriter_") as tmpdir:
            path = Path(tmpdir) / "synonyms.json"
            path.write_text(
                json.dumps(
                    {
                        "global": {
                            "员工": ["employee", "employees", "staff"],
                            "部门": ["department", "departments", "team"],
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            rewriter = QueryRewriter(SynonymDictionary.from_file(path))
            result = asyncio.run(rewriter.rewrite("count employees by department", "conn-1"))
            self.assertIn("员工", result.rewritten_query)
            self.assertIn("部门", result.rewritten_query)
            self.assertTrue(result.applied_synonyms)
            self.assertTrue(result.expanded_query)

    def test_missing_synonym_file_degrades_gracefully(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rag_rewriter_missing_") as tmpdir:
            path = Path(tmpdir) / "missing.json"
            synonyms = SynonymDictionary.from_file(path)
            rewriter = QueryRewriter(synonyms)
            result = asyncio.run(rewriter.rewrite("统计部门员工数量", "conn-1"))
            self.assertEqual(result.rewritten_query, "统计部门员工数量")
            self.assertEqual(result.expanded_query, "统计部门员工数量")
            self.assertFalse(result.applied_synonyms)


class RetrievalCacheTests(unittest.TestCase):
    def test_cache_invalidates_when_schema_version_changes(self) -> None:
        cache = RetrievalCache(max_entries=4, ttl_seconds=600, enabled=True)
        payload = {"tables": ["users"]}
        cache.put("show users", "conn-1", "v1", payload)
        self.assertEqual(cache.get("show users", "conn-1", "v1"), payload)
        removed = cache.invalidate_schema_version("conn-1", "v2")
        self.assertEqual(removed, 1)
        self.assertIsNone(cache.get("show users", "conn-1", "v1"))


class RetrievalOrchestratorTests(unittest.TestCase):
    def test_orchestrator_uses_cache_and_schema_shards(self) -> None:
        tables = [_make_table(f"sales_orders_{index}", "id", "user_id", "amount") for index in range(12)]
        tables.extend(_make_table(f"user_profile_{index}", "id", "nickname", "email") for index in range(12))

        orchestrator = RetrievalOrchestrator(
            vector_store=InMemoryVectorStore(),
            shard_planner=SchemaShardPlanner(max_shard_size=10, max_query_shards=3),
            cache=RetrievalCache(max_entries=16, ttl_seconds=600, enabled=True),
        )

        async def scenario() -> None:
            await orchestrator.index_schema("conn-1", tables, database_name="demo", schema_name="public")
            first = await orchestrator.retrieve_detailed("统计用户订单金额", "conn-1", top_k=5)
            second = await orchestrator.retrieve_detailed("统计用户订单金额", "conn-1", top_k=5)
            self.assertFalse(first.telemetry.cache_hit)
            self.assertTrue(second.telemetry.cache_hit)
            self.assertGreaterEqual(first.telemetry.shard_count, 2)
            self.assertTrue(first.tables)
            self.assertIn(first.tables[0].name, {table.name for table in tables})

        asyncio.run(scenario())

    def test_orchestrator_rewrite_and_cache_invalidation(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rag_runtime_") as tmpdir:
            synonym_path = Path(tmpdir) / "synonyms.json"
            synonym_path.write_text(
                json.dumps(
                    {
                        "global": {
                            "员工": ["employee", "employees", "staff"],
                            "部门": ["department", "departments", "team"],
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            synonym_dictionary = SynonymDictionary.from_file(synonym_path)
            query_rewriter = QueryRewriter(synonym_dictionary)

            orchestrator = RetrievalOrchestrator(
                vector_store=InMemoryVectorStore(),
                shard_planner=SchemaShardPlanner(max_shard_size=10, max_query_shards=3),
                cache=RetrievalCache(max_entries=16, ttl_seconds=600, enabled=True),
                query_rewriter=query_rewriter,
                synonym_dictionary=synonym_dictionary,
                synonym_path=str(synonym_path),
            )

            users = _make_table("users", "id", "name")
            employees = _make_table("employees", "id", "department_id", "employee_count")
            departments = _make_table("departments", "id", "department_name")

            async def scenario() -> None:
                await orchestrator.index_schema("conn-1", [users, employees], database_name="demo", schema_name="public")
                first = await orchestrator.retrieve_detailed("count employees by department", "conn-1", top_k=1)
                second = await orchestrator.retrieve_detailed("count employees by department", "conn-1", top_k=1)
                self.assertIn("员工", first.telemetry.rewritten_query or "")
                self.assertIn("部门", first.telemetry.rewritten_query or "")
                self.assertTrue(first.telemetry.applied_synonyms)
                self.assertTrue(second.telemetry.cache_hit)

                await orchestrator.index_schema(
                    "conn-1",
                    [users, employees, departments],
                    database_name="demo",
                    schema_name="public",
                    force=True,
                )
                third = await orchestrator.retrieve_detailed("count employees by department", "conn-1", top_k=1)
                self.assertFalse(third.telemetry.cache_hit)
                self.assertTrue(any(table.name in {"employees", "departments"} for table in third.tables))

            asyncio.run(scenario())

    def test_orchestrator_expands_relationships_and_columns(self) -> None:
        users = TableSchema(
            name="users",
            columns=[
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="name", type="TEXT"),
                ColumnInfo(name="email", type="TEXT"),
            ],
        )
        orders = TableSchema(
            name="orders",
            columns=[
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True),
                ColumnInfo(name="amount", type="DECIMAL"),
            ],
        )

        orchestrator = RetrievalOrchestrator(
            vector_store=InMemoryVectorStore(),
            shard_planner=SchemaShardPlanner(max_shard_size=2, max_query_shards=3),
            cache=RetrievalCache(max_entries=16, ttl_seconds=600, enabled=True),
        )

        async def scenario() -> None:
            await orchestrator.index_schema("conn-rel", [users, orders], database_name="demo", schema_name="public")
            result = await orchestrator.retrieve_detailed("统计每个用户的订单金额", "conn-rel", top_k=1)
            self.assertIn("orders", [table.name for table in result.tables])
            self.assertIn("users", [table.name for table in result.tables])
            self.assertGreaterEqual(result.telemetry.relationship_count, 1)
            self.assertIn("users", result.telemetry.relationship_tables)
            self.assertIn("relationship_clues", result.metadata)
            self.assertIn("column_annotations", result.metadata)
            self.assertTrue(result.metadata["column_annotations"].get("orders"))

        asyncio.run(scenario())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
