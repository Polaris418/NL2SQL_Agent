from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from app.db.repositories.base import RepositoryBase
from app.db.security import PasswordCipher
from app.schemas.llm import LLMProvider


@dataclass(slots=True)
class StoredLLMProfile:
    id: str
    display_name: str
    provider: str
    model: str
    base_url: str | None
    api_key: str | None
    updated_at: str | None = None


@dataclass(slots=True)
class StoredLLMRouting:
    primary_profile_id: str | None
    fallback_profile_id: str | None


class LLMSettingsRepository(RepositoryBase):
    def __init__(self, metadata_db, cipher: PasswordCipher | None = None):
        super().__init__(metadata_db)
        self.cipher = cipher or PasswordCipher.from_env()

    def list_profiles(self) -> list[StoredLLMProfile]:
        self._migrate_legacy_default_row()
        rows = self.fetch_all("SELECT * FROM llm_settings ORDER BY updated_at DESC, id ASC")
        return [self._row_to_profile(row) for row in rows]

    def get_profile(self, profile_id: str) -> StoredLLMProfile | None:
        self._migrate_legacy_default_row()
        row = self.fetch_one("SELECT * FROM llm_settings WHERE id = ?", (profile_id,))
        return self._row_to_profile(row) if row else None

    def get(self) -> StoredLLMProfile | None:
        routing = self.get_routing()
        if routing.primary_profile_id:
            profile = self.get_profile(routing.primary_profile_id)
            if profile is not None:
                return profile
        profiles = self.list_profiles()
        return profiles[0] if profiles else None

    def save_profile(
        self,
        *,
        profile_id: str | None = None,
        display_name: str | None = None,
        provider: str | LLMProvider,
        model: str,
        base_url: str | None,
        api_key: str | None,
    ) -> StoredLLMProfile:
        provider_value = getattr(provider, "value", provider)
        encrypted_key = self.cipher.encrypt(api_key)
        target_id = profile_id or f"profile_{uuid4().hex[:10]}"
        target_name = display_name or f"{provider_value}-{model}"
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO llm_settings (id, display_name, provider, model, base_url, api_key_encrypted, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    display_name = excluded.display_name,
                    provider = excluded.provider,
                    model = excluded.model,
                    base_url = excluded.base_url,
                    api_key_encrypted = excluded.api_key_encrypted,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (target_id, target_name, provider_value, model, base_url, encrypted_key),
            )
            has_routing = conn.execute("SELECT 1 FROM llm_routing WHERE id = 'default'").fetchone()
            if has_routing is None:
                conn.execute(
                    """
                    INSERT INTO llm_routing (id, primary_profile_id, fallback_profile_id, updated_at)
                    VALUES ('default', ?, NULL, CURRENT_TIMESTAMP)
                    """,
                    (target_id,),
                )
        return StoredLLMProfile(
            id=target_id,
            display_name=target_name,
            provider=provider_value,
            model=model,
            base_url=base_url,
            api_key=api_key,
        )

    def delete_profile(self, profile_id: str) -> None:
        with self.metadata_db.connect() as conn:
            conn.execute("DELETE FROM llm_settings WHERE id = ?", (profile_id,))
            routing = conn.execute(
                "SELECT primary_profile_id, fallback_profile_id FROM llm_routing WHERE id = 'default'"
            ).fetchone()
            if routing:
                primary = routing["primary_profile_id"]
                fallback = routing["fallback_profile_id"]
                new_primary = None if primary == profile_id else primary
                new_fallback = None if fallback == profile_id else fallback
                conn.execute(
                    """
                    INSERT INTO llm_routing (id, primary_profile_id, fallback_profile_id, updated_at)
                    VALUES ('default', ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET
                        primary_profile_id = excluded.primary_profile_id,
                        fallback_profile_id = excluded.fallback_profile_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (new_primary, new_fallback),
                )

    def get_routing(self) -> StoredLLMRouting:
        self._migrate_legacy_default_row()
        row = self.fetch_one(
            "SELECT primary_profile_id, fallback_profile_id FROM llm_routing WHERE id = 'default'"
        )
        if row is None:
            return StoredLLMRouting(primary_profile_id=None, fallback_profile_id=None)
        return StoredLLMRouting(
            primary_profile_id=row.get("primary_profile_id"),
            fallback_profile_id=row.get("fallback_profile_id"),
        )

    def save_routing(self, *, primary_profile_id: str | None, fallback_profile_id: str | None) -> StoredLLMRouting:
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO llm_routing (id, primary_profile_id, fallback_profile_id, updated_at)
                VALUES ('default', ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    primary_profile_id = excluded.primary_profile_id,
                    fallback_profile_id = excluded.fallback_profile_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (primary_profile_id, fallback_profile_id),
            )
        return StoredLLMRouting(primary_profile_id=primary_profile_id, fallback_profile_id=fallback_profile_id)

    def _row_to_profile(self, row: dict | None) -> StoredLLMProfile | None:
        if row is None:
            return None
        return StoredLLMProfile(
            id=row["id"],
            display_name=row.get("display_name") or row["id"],
            provider=row["provider"],
            model=row["model"],
            base_url=row.get("base_url"),
            api_key=self.cipher.decrypt(row["api_key_encrypted"]) if row.get("api_key_encrypted") else None,
            updated_at=row.get("updated_at"),
        )

    def _migrate_legacy_default_row(self) -> None:
        legacy = self.fetch_one("SELECT * FROM llm_settings WHERE id = 'default'")
        if legacy is None:
            return
        routed = self.fetch_one("SELECT * FROM llm_routing WHERE id = 'default'")
        if routed is not None:
            return
        migrated_id = "profile_default"
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO llm_settings (id, display_name, provider, model, base_url, api_key_encrypted, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """,
                (
                    migrated_id,
                    legacy.get("display_name") or "默认 API",
                    legacy["provider"],
                    legacy["model"],
                    legacy.get("base_url"),
                    legacy.get("api_key_encrypted"),
                    legacy.get("updated_at"),
                ),
            )
            conn.execute("DELETE FROM llm_settings WHERE id = 'default'")
            conn.execute(
                """
                INSERT INTO llm_routing (id, primary_profile_id, fallback_profile_id, updated_at)
                VALUES ('default', ?, NULL, CURRENT_TIMESTAMP)
                """,
                (migrated_id,),
            )
