from __future__ import annotations

import json

import pytest

from app.agent.contracts import ColumnInfo, TableSchema
from app.rag.embedding import DeterministicHashEmbedding
from app.rag.fusion import ReciprocalRankFusion, RetrievalCandidate, WeightedFusion
from app.rag.metadata_filter import MetadataFilter
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.query_rewriter import QueryRewriter
from app.rag.schema_doc import SchemaDocumentationManager
from app.rag.synonym_dict import SynonymDictionary
from app.rag.vector_store import InMemoryVectorStore


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(
        connection_id="conn_1",
        table_name=name,
        table_comment=comment,
        columns=columns,
        description=comment,
    )


def test_schema_documentation_generation():
    users = build_table(
        "users",
        [
            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ColumnInfo(name="name", type="TEXT", comment="user name"),
            ColumnInfo(name="created_at", type="DATETIME"),
        ],
        comment="User profile table",
    )
    orders = build_table(
        "orders",
        [
            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True, foreign_table="users"),
            ColumnInfo(name="amount", type="DECIMAL", comment="order amount"),
        ],
        comment="Order fact table",
    )

    manager = SchemaDocumentationManager()
    doc = manager.generate_documentation(orders, related_tables=[users], sample_values=["12.5", "18.9"])

    assert doc.table_name == "orders"
    assert doc.business_summary
    assert doc.columns[1].is_foreign_key is True
    assert doc.join_paths
    assert "sales" in doc.domain_tags or "order" in doc.domain_tags


def test_fusion_strategies_rank_results():
    candidates_a = [
        RetrievalCandidate(key="users", payload="users", score=0.9, source="lexical"),
        RetrievalCandidate(key="orders", payload="orders", score=0.7, source="lexical"),
    ]
    candidates_b = [
        RetrievalCandidate(key="orders", payload="orders", score=0.95, source="vector"),
        RetrievalCandidate(key="users", payload="users", score=0.2, source="vector"),
    ]

    rrf = ReciprocalRankFusion()
    fused_rrf = rrf.fuse({"lexical": candidates_a, "vector": candidates_b})
    assert fused_rrf[0].key in {"users", "orders"}
    assert len(fused_rrf) == 2

    weighted = WeightedFusion({"lexical": 0.2, "vector": 0.8})
    fused_weighted = weighted.fuse({"lexical": candidates_a, "vector": candidates_b})
    assert fused_weighted[0].key == "orders"


def test_metadata_filter_matching_and_inference():
    filt = MetadataFilter.infer_from_query("统计每个部门的员工数量", connection_id="conn_1")
    assert filt.connection_id == "conn_1"
    assert "organization" in filt.business_domains or filt.business_domains == set()

    metadata = {
        "connection_id": "conn_1",
        "database_name": "sample",
        "schema_name": "public",
        "table_tags": ["fact_table"],
        "business_domains": ["organization"],
        "table_name": "employees",
    }
    assert MetadataFilter(connection_id="conn_1", business_domains={"organization"}).matches(metadata)
    assert not MetadataFilter(connection_id="conn_2").matches(metadata)


@pytest.mark.asyncio
async def test_orchestrator_indexes_and_retrieves():
    users = build_table(
        "users",
        [
            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ColumnInfo(name="name", type="TEXT", comment="user name"),
        ],
        comment="User dimension table",
    )
    employees = build_table(
        "employees",
        [
            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ColumnInfo(name="department_id", type="INTEGER"),
            ColumnInfo(name="employee_count", type="INTEGER", comment="count of employees"),
        ],
        comment="Employee fact table",
    )

    orchestrator = RetrievalOrchestrator(
        embedding_model=DeterministicHashEmbedding(),
        vector_store=InMemoryVectorStore(),
    )
    await orchestrator.index_schema("conn_1", [users, employees], database_name="demo", schema_name="public")
    result = await orchestrator.retrieve_detailed("统计部门员工数量", "conn_1", top_k=2)

    assert result.tables
    assert result.telemetry.selected_count >= 1
    assert result.schema_version
    assert any(table.table_name == "employees" for table in result.tables)


@pytest.mark.asyncio
async def test_query_rewrite_and_cache_invalidation(tmp_path):
    synonym_file = tmp_path / "synonyms.json"
    synonym_file.write_text(
        json.dumps(
            {
                "global": {
                    "员工": ["employee", "employees", "staff"],
                    "部门": ["department", "dept"],
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    synonym_dictionary = SynonymDictionary.from_file(synonym_file)
    query_rewriter = QueryRewriter(synonym_dictionary)
    missing_dictionary = SynonymDictionary.from_file(tmp_path / "missing_synonyms.json")
    assert missing_dictionary.rewrite("统计部门员工数量", connection_id="conn_1") == "统计部门员工数量"

    users = build_table(
        "users",
        [
            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ColumnInfo(name="name", type="TEXT", comment="user name"),
        ],
        comment="User dimension table",
    )
    employees = build_table(
        "employees",
        [
            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ColumnInfo(name="department_id", type="INTEGER"),
            ColumnInfo(name="employee_count", type="INTEGER", comment="count of employees"),
        ],
        comment="Employee fact table",
    )

    orchestrator = RetrievalOrchestrator(
        embedding_model=DeterministicHashEmbedding(),
        vector_store=InMemoryVectorStore(),
        query_rewriter=query_rewriter,
        synonym_dictionary=synonym_dictionary,
        synonym_path=str(synonym_file),
    )
    await orchestrator.index_schema("conn_1", [users, employees], database_name="demo", schema_name="public")

    first = await orchestrator.retrieve_detailed("统计部门员工数量", "conn_1", top_k=1)
    second = await orchestrator.retrieve_detailed("统计部门员工数量", "conn_1", top_k=1)
    assert first.telemetry.rewritten_query is not None
    assert "employee" in first.telemetry.expanded_query.lower()
    assert first.telemetry.applied_synonyms
    assert second.telemetry.cache_hit is True

    departments = build_table(
        "departments",
        [
            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
            ColumnInfo(name="department_name", type="TEXT"),
        ],
        comment="Department dimension table",
    )
    await orchestrator.index_schema("conn_1", [users, employees, departments], database_name="demo", schema_name="public", force=True)
    third = await orchestrator.retrieve_detailed("统计部门员工数量", "conn_1", top_k=1)
    assert third.telemetry.cache_hit is False
    assert any(table.table_name in {"employees", "departments"} for table in third.tables)
