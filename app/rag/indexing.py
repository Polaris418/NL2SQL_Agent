from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.agent.utils import table_name
from app.rag.schema_version import (
    SchemaVersionManager,
    compute_schema_fingerprint,
    compute_table_fingerprint,
)


@dataclass(slots=True)
class TableIndexState:
    table_name: str
    fingerprint: str
    last_indexed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class IndexTableChange:
    table_name: str
    change_type: str
    fingerprint: str
    previous_fingerprint: str | None = None
    last_indexed_at: datetime | None = None


@dataclass(slots=True)
class IndexingPlan:
    connection_id: str
    schema_fingerprint: str
    created_at: datetime
    force_rebuild: bool
    added: list[IndexTableChange] = field(default_factory=list)
    updated: list[IndexTableChange] = field(default_factory=list)
    removed: list[IndexTableChange] = field(default_factory=list)
    unchanged: list[IndexTableChange] = field(default_factory=list)
    tables_to_index: list[str] = field(default_factory=list)
    table_fingerprints: dict[str, str] = field(default_factory=dict)
    previous_table_fingerprints: dict[str, str] = field(default_factory=dict)

    @property
    def changed_tables(self) -> list[str]:
        return [item.table_name for item in (*self.added, *self.updated, *self.removed)]


@dataclass(slots=True)
class IndexingResult(IndexingPlan):
    applied_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    indexed_tables: list[str] = field(default_factory=list)
    skipped_tables: list[str] = field(default_factory=list)
    table_last_indexed_at: dict[str, datetime] = field(default_factory=dict)
    schema_version: str | None = None


class IndexingSystem:
    """Lightweight incremental schema indexing planner/state tracker."""

    def __init__(self, version_manager: SchemaVersionManager | None = None):
        self.version_manager = version_manager or SchemaVersionManager()
        self._connection_state: dict[str, dict[str, TableIndexState]] = {}
        self._current_versions: dict[str, str] = {}
        self._version_history: dict[str, list[str]] = {}

    def detect_changes(self, connection_id: str, tables: list[Any], *, force: bool = False) -> IndexingPlan:
        table_list = list(tables)
        current_fingerprints = {table_name(table): compute_table_fingerprint(table) for table in table_list}
        current_tables = {table_name(table): table for table in table_list}
        previous_state = self._connection_state.get(connection_id, {})
        previous_fingerprints = {name: state.fingerprint for name, state in previous_state.items()}
        schema_fingerprint = compute_schema_fingerprint(table_list)
        now = datetime.now(timezone.utc)

        added: list[IndexTableChange] = []
        updated: list[IndexTableChange] = []
        removed: list[IndexTableChange] = []
        unchanged: list[IndexTableChange] = []

        for name, fingerprint in current_fingerprints.items():
            if force or name not in previous_state:
                added.append(
                    IndexTableChange(
                        table_name=name,
                        change_type="added" if name not in previous_state else "updated",
                        fingerprint=fingerprint,
                        previous_fingerprint=previous_fingerprints.get(name),
                        last_indexed_at=previous_state.get(name).last_indexed_at if name in previous_state else None,
                    )
                )
            elif previous_state[name].fingerprint != fingerprint:
                updated.append(
                    IndexTableChange(
                        table_name=name,
                        change_type="updated",
                        fingerprint=fingerprint,
                        previous_fingerprint=previous_state[name].fingerprint,
                        last_indexed_at=previous_state[name].last_indexed_at,
                    )
                )
            else:
                unchanged.append(
                    IndexTableChange(
                        table_name=name,
                        change_type="unchanged",
                        fingerprint=fingerprint,
                        previous_fingerprint=previous_state[name].fingerprint,
                        last_indexed_at=previous_state[name].last_indexed_at,
                    )
                )

        for name, state in previous_state.items():
            if name not in current_tables:
                removed.append(
                    IndexTableChange(
                        table_name=name,
                        change_type="removed",
                        fingerprint=state.fingerprint,
                        previous_fingerprint=state.fingerprint,
                        last_indexed_at=state.last_indexed_at,
                    )
                )

        tables_to_index = [item.table_name for item in added + updated]
        if force and not tables_to_index:
            tables_to_index = sorted(current_fingerprints)

        return IndexingPlan(
            connection_id=connection_id,
            schema_fingerprint=schema_fingerprint,
            created_at=now,
            force_rebuild=force,
            added=added,
            updated=updated,
            removed=removed,
            unchanged=unchanged,
            tables_to_index=tables_to_index,
            table_fingerprints=current_fingerprints,
            previous_table_fingerprints=previous_fingerprints,
        )

    def incremental_update(self, connection_id: str, tables: list[Any], *, force: bool = False) -> IndexingResult:
        plan = self.detect_changes(connection_id, tables, force=force)
        table_map = {table_name(table): table for table in tables}
        current_state = dict(self._connection_state.get(connection_id, {}))
        now = datetime.now(timezone.utc)
        indexed_tables: list[str] = []
        skipped_tables: list[str] = []
        table_last_indexed_at: dict[str, datetime] = {}

        for removed in plan.removed:
            current_state.pop(removed.table_name, None)

        for table in tables:
            name = table_name(table)
            fingerprint = plan.table_fingerprints[name]
            should_index = force or name in plan.tables_to_index
            previous = current_state.get(name)
            if should_index:
                current_state[name] = TableIndexState(table_name=name, fingerprint=fingerprint, last_indexed_at=now)
                indexed_tables.append(name)
                table_last_indexed_at[name] = now
            else:
                if previous is None:
                    current_state[name] = TableIndexState(table_name=name, fingerprint=fingerprint, last_indexed_at=now)
                    table_last_indexed_at[name] = now
                else:
                    current_state[name] = TableIndexState(
                        table_name=name,
                        fingerprint=fingerprint,
                        last_indexed_at=previous.last_indexed_at,
                    )
                    table_last_indexed_at[name] = previous.last_indexed_at
                skipped_tables.append(name)

        self._connection_state[connection_id] = current_state
        schema_version = plan.schema_fingerprint
        self._current_versions[connection_id] = schema_version
        self._version_history.setdefault(connection_id, []).append(schema_version)
        self.version_manager.save_version(
            connection_id,
            list(tables),
            version=schema_version,
            metadata={
                "force_rebuild": force,
                "added_tables": [item.table_name for item in plan.added],
                "updated_tables": [item.table_name for item in plan.updated],
                "removed_tables": [item.table_name for item in plan.removed],
            },
        )

        return IndexingResult(
            connection_id=plan.connection_id,
            schema_fingerprint=plan.schema_fingerprint,
            created_at=plan.created_at,
            force_rebuild=plan.force_rebuild,
            added=plan.added,
            updated=plan.updated,
            removed=plan.removed,
            unchanged=plan.unchanged,
            tables_to_index=plan.tables_to_index,
            table_fingerprints=plan.table_fingerprints,
            previous_table_fingerprints=plan.previous_table_fingerprints,
            applied_at=now,
            indexed_tables=indexed_tables,
            skipped_tables=skipped_tables,
            table_last_indexed_at=table_last_indexed_at,
            schema_version=schema_version,
        )

    def get_table_last_indexed_at(self, connection_id: str, table: str) -> datetime | None:
        return self._connection_state.get(connection_id, {}).get(table, None).last_indexed_at if table in self._connection_state.get(connection_id, {}) else None

    def get_connection_index_state(self, connection_id: str) -> dict[str, TableIndexState]:
        return dict(self._connection_state.get(connection_id, {}))

    def get_current_schema_version(self, connection_id: str) -> str | None:
        return self._current_versions.get(connection_id)

    def get_version_history(self, connection_id: str) -> list[str]:
        return list(self._version_history.get(connection_id, []))
