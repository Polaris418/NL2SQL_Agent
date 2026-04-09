from __future__ import annotations

import asyncio
import json
import math
import tempfile
import time
import unittest
from pathlib import Path

from app.rag.business_knowledge import BusinessKnowledgeRepository
from app.rag.embedding import DeterministicHashEmbedding
from app.rag.fusion import ReciprocalRankFusion, RetrievalCandidate, WeightedFusion, normalize_scores
from app.rag.metadata_filter import MetadataFilter
from app.rag.profiling import ColumnProfile, ProfilingSnapshot, ProfilingStore, TableProfile
from app.rag.reranker import CrossEncoderReranker, RerankCandidate, RerankerConfig
from app.rag.schema_doc import SchemaDocumentationManager
from app.rag.vector_store import InMemoryVectorStore
from app.schemas.connection import ColumnInfo, TableSchema


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(name=name, comment=comment, description=comment, columns=columns)


def cosine_similarity(left: list[float], right: list[float]) -> float:
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left)) or 1.0
    right_norm = math.sqrt(sum(b * b for b in right)) or 1.0
    return numerator / (left_norm * right_norm)


class EmbeddingPropertyTests(unittest.IsolatedAsyncioTestCase):
    async def test_embedding_is_stable_for_the_same_text(self) -> None:
        embedder = DeterministicHashEmbedding()

        first = await embedder.embed_text("统计用户订单金额")
        second = await embedder.embed_text("统计用户订单金额")

        self.assertEqual(first, second)
        self.assertEqual(len(first), embedder.dimensions)
        self.assertAlmostEqual(cosine_similarity(first, second), 1.0, places=7)

    async def test_related_texts_score_higher_than_unrelated_texts(self) -> None:
        embedder = DeterministicHashEmbedding()

        base = await embedder.embed_text("统计用户订单金额")
        related = await embedder.embed_text("订单金额统计用户")
        unrelated = await embedder.embed_text("天气预报")

        self.assertGreater(cosine_similarity(base, related), cosine_similarity(base, unrelated))
        self.assertGreater(cosine_similarity(base, related), 0.2)


class VectorStorePropertyTests(unittest.IsolatedAsyncioTestCase):
    async def test_in_memory_vector_store_round_trip_backup_and_filtering(self) -> None:
        store = InMemoryVectorStore()
        await store.initialize()

        await store.upsert(
            ids=["conn-1:users", "conn-1:orders"],
            vectors=[[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
            metadatas=[
                {
                    "connection_id": "conn-1",
                    "database_name": "demo",
                    "schema_name": "public",
                    "db_type": "mysql",
                    "table_name": "users",
                    "table_tags": "core",
                    "business_domains": "organization",
                },
                {
                    "connection_id": "conn-1",
                    "database_name": "demo",
                    "schema_name": "public",
                    "db_type": "mysql",
                    "table_name": "orders",
                    "table_tags": "fact",
                    "business_domains": "sales",
                },
            ],
            documents=["users doc", "orders doc"],
        )

        health = await store.health_check()
        self.assertTrue(health.is_healthy)
        self.assertEqual(health.indexed_count, 2)

        filtered = await store.query(
            [1.0, 0.0, 0.0],
            top_k=5,
            filter=MetadataFilter(
                connection_id="conn-1",
                database_name="demo",
                schema_name="public",
                db_type="mysql",
                table_tags={"core"},
                business_domains={"organization"},
            ).to_where_clause(),
        )
        self.assertEqual([item.id for item in filtered], ["conn-1:users"])
        self.assertEqual(filtered[0].metadata["table_name"], "users")

        backup_path = Path(tempfile.mkdtemp(prefix="rag_vector_backup_")) / "backup.json"
        await store.backup(str(backup_path))
        payload = json.loads(backup_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["collection_name"], "default")
        self.assertIn("conn-1:users", payload["documents"])
        self.assertIn("conn-1:orders", payload["vectors"])


class SchemaDocumentationPropertyTests(unittest.TestCase):
    def test_documentation_captures_profiles_relationships_and_knowledge(self) -> None:
        profiling_store = ProfilingStore()
        profiling_store.upsert(
            ProfilingSnapshot(
                connection_id="conn-doc",
                database_name="demo",
                schema_name="public",
                tables=[
                    TableProfile(
                        table_name="orders",
                        row_count=42,
                        columns=[
                            ColumnProfile(column_name="id", data_type="INTEGER", distinct_count=42),
                            ColumnProfile(column_name="user_id", data_type="INTEGER", distinct_count=12),
                            ColumnProfile(column_name="created_at", data_type="TIMESTAMP", sample_values=["2026-01-01", "2026-01-02"], distinct_count=2, null_ratio=0.0),
                            ColumnProfile(column_name="amount", data_type="DECIMAL", sample_values=["12.50", "20.00"], distinct_count=2, null_ratio=0.0, min_value="12.50", max_value="20.00"),
                            ColumnProfile(column_name="status", data_type="TEXT", sample_values=["paid", "refunded"], distinct_count=2, null_ratio=0.0),
                        ],
                    )
                ],
            )
        )
        knowledge_repo = BusinessKnowledgeRepository()
        knowledge_repo.add(
            "orders-revenue",
            "Orders table tracks customer purchases and revenue metrics",
            connection_id="conn-doc",
            table_name="orders",
            keywords=["orders", "revenue"],
            priority=10,
        )

        manager = SchemaDocumentationManager(
            profiling_store=profiling_store,
            business_knowledge_repository=knowledge_repo,
        )
        users = build_table(
            "users",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ],
            comment="User dimension table",
        )
        orders = build_table(
            "orders",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True, comment="related user"),
                ColumnInfo(name="created_at", type="TIMESTAMP", comment="order create time"),
                ColumnInfo(name="amount", type="DECIMAL", comment="order amount metric"),
                ColumnInfo(name="status", type="TEXT", comment="paid / refunded"),
            ],
            comment="Order fact table",
        )

        doc = manager.generate_documentation(
            orders,
            related_tables=[users],
            connection_id="conn-doc",
            database_name="demo",
            schema_name="public",
            version="v1",
            sample_values=["order-1", "order-2"],
            metadata={"source": "unit-test"},
        )

        columns = {column.name: column for column in doc.columns}
        self.assertTrue(columns["id"].is_primary_key)
        self.assertTrue(columns["user_id"].is_foreign_key)
        self.assertTrue(columns["created_at"].is_time_field)
        self.assertTrue(columns["amount"].is_metric_field)
        self.assertIn("paid", columns["status"].sample_values)
        self.assertEqual(doc.metadata["profile_row_count"], 42)
        self.assertIn("Orders table tracks customer purchases and revenue metrics", doc.metadata["knowledge_snippets"])
        self.assertEqual(len(doc.join_paths), 1)
        self.assertEqual(doc.join_paths[0].target_table, "users")
        self.assertEqual(doc.join_paths[0].source_column, "user_id")
        self.assertIn("Profile: row_count=42", manager.to_context_text("orders"))
        self.assertIn("Business Knowledge:", manager.to_context_text("orders"))
        self.assertIn("Joins:", manager.to_context_text("orders"))


class FusionAndRerankerPropertyTests(unittest.TestCase):
    def test_score_normalization_and_fusion_keep_the_best_candidate_on_top(self) -> None:
        self.assertEqual(normalize_scores([5.0, 5.0, 5.0]), [1.0, 1.0, 1.0])
        self.assertEqual(normalize_scores([0.0, 5.0, 10.0]), [0.0, 0.5, 1.0])

        sources = {
            "lexical": [
                RetrievalCandidate(key="orders", payload="orders", score=10.0, source="lexical"),
                RetrievalCandidate(key="users", payload="users", score=3.0, source="lexical"),
            ],
            "vector": [
                RetrievalCandidate(key="orders", payload="orders", score=9.0, source="vector"),
                RetrievalCandidate(key="users", payload="users", score=1.0, source="vector"),
            ],
        }

        weighted = WeightedFusion(weights={"lexical": 2.0, "vector": 1.0}).fuse(sources)
        self.assertEqual([item.key for item in weighted], ["orders", "users"])
        self.assertGreater(weighted[0].score, weighted[1].score)
        self.assertEqual(weighted[0].source_scores["lexical"], 2.0)
        self.assertEqual(weighted[0].source_scores["vector"], 1.0)

        rrf = ReciprocalRankFusion(k=10).fuse(sources)
        self.assertEqual([item.key for item in rrf], ["orders", "users"])
        self.assertGreater(rrf[0].score, rrf[1].score)

    def test_reranker_falls_back_when_model_scoring_times_out(self) -> None:
        class TimedOutReranker(CrossEncoderReranker):
            def _load_model(self):  # type: ignore[override]
                return object()

            @staticmethod
            def _score_with_model(model, query: str, items: list[RerankCandidate]) -> list[float]:  # noqa: ARG004
                time.sleep(0.05)
                return [0.1 for _ in items]

        reranker = TimedOutReranker(RerankerConfig(timeout_seconds=0.001, max_candidates=4))
        ranked = asyncio.run(
            reranker.rerank(
                "用户邮箱",
                [
                    RerankCandidate(key="orders", payload={}, score=0.0, text="订单金额"),
                    RerankCandidate(key="users", payload={}, score=0.0, text="用户邮箱"),
                ],
            )
        )

        self.assertEqual(ranked[0].key, "users")
        self.assertEqual(ranked[1].key, "orders")


class MetadataFilterPropertyTests(unittest.IsolatedAsyncioTestCase):
    async def test_filter_inference_and_vector_store_isolation_remain_consistent(self) -> None:
        inferred = MetadataFilter.infer_from_query(
            "统计每个部门的员工数量",
            connection_id="conn-1",
            database_name="demo",
            schema_name="public",
        )
        self.assertIn("organization", inferred.business_domains)
        self.assertIn("connection_id", inferred.to_where_clause()["$and"][0])

        store = InMemoryVectorStore()
        await store.initialize()
        await store.upsert(
            ids=["employees", "orders"],
            vectors=[[1.0, 0.0], [0.0, 1.0]],
            metadatas=[
                {
                    "connection_id": "conn-1",
                    "database_name": "demo",
                    "schema_name": "public",
                    "db_type": "mysql",
                    "table_name": "employees",
                    "table_tags": "core",
                    "business_domains": "organization",
                },
                {
                    "connection_id": "conn-1",
                    "database_name": "demo",
                    "schema_name": "public",
                    "db_type": "mysql",
                    "table_name": "orders",
                    "table_tags": "fact",
                    "business_domains": "sales",
                },
            ],
            documents=["employees doc", "orders doc"],
        )

        results = await store.query([1.0, 0.0], top_k=5, filter=inferred.to_where_clause())
        self.assertEqual([result.id for result in results], ["employees"])
        self.assertEqual(results[0].metadata["table_name"], "employees")
        self.assertTrue(inferred.matches(results[0].metadata))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
