from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.dependencies import get_llm_client, get_metadata_db
from app.db.metadata import MetadataDB
from app.db.repositories.llm_repo import LLMSettingsRepository
from app.llm.client import LLMClient
from app.schemas.llm import (
    LLMProfileResponse,
    LLMProfileUpsert,
    LLMRoutingUpdate,
    LLMSettingsResponse,
    LLMTestResult,
)

router = APIRouter(tags=["settings"])


def _mask_api_key(api_key: str | None) -> str | None:
    if not api_key:
        return None
    if len(api_key) <= 8:
        return "*" * len(api_key)
    return f"{api_key[:4]}{'*' * max(len(api_key) - 8, 4)}{api_key[-4:]}"


def _provider_requires_api_key(provider: str) -> bool:
    return provider not in {"ollama", "custom"}


def _build_response(metadata_db: MetadataDB, llm_client: LLMClient) -> LLMSettingsResponse:
    repo = LLMSettingsRepository(metadata_db)
    routing = repo.get_routing()
    profiles = repo.list_profiles()
    return LLMSettingsResponse(
        active_profile_id=routing.primary_profile_id,
        fallback_profile_id=routing.fallback_profile_id,
        profiles=[
            LLMProfileResponse(
                id=profile.id,
                display_name=profile.display_name,
                provider=profile.provider,
                model=profile.model,
                base_url=profile.base_url,
                has_api_key=bool(profile.api_key),
                api_key_masked=_mask_api_key(profile.api_key),
                updated_at=profile.updated_at,
            )
            for profile in profiles
        ],
        providers=llm_client.provider_options(),
    )


def _sync_client_profiles(metadata_db: MetadataDB, llm_client: LLMClient) -> None:
    repo = LLMSettingsRepository(metadata_db)
    routing = repo.get_routing()
    profiles = repo.list_profiles()
    if not profiles:
        return
    llm_client.configure_profiles(
        profiles=[
            {
                "id": profile.id,
                "provider": profile.provider,
                "model": profile.model,
                "api_key": profile.api_key,
                "base_url": profile.base_url,
            }
            for profile in profiles
        ],
        primary_profile_id=routing.primary_profile_id or profiles[0].id,
        fallback_profile_id=routing.fallback_profile_id,
    )


@router.get("/settings/llm", response_model=LLMSettingsResponse)
async def get_llm_settings(
    llm_client: LLMClient = Depends(get_llm_client),
    metadata_db: MetadataDB = Depends(get_metadata_db),
) -> LLMSettingsResponse:
    return _build_response(metadata_db, llm_client)


@router.post("/settings/llm/profiles", response_model=LLMSettingsResponse)
async def upsert_llm_profile(
    payload: LLMProfileUpsert,
    llm_client: LLMClient = Depends(get_llm_client),
    metadata_db: MetadataDB = Depends(get_metadata_db),
) -> LLMSettingsResponse:
    repo = LLMSettingsRepository(metadata_db)
    existing = repo.get_profile(payload.id) if payload.id else None
    provider_value = getattr(payload.provider, "value", payload.provider)
    effective_api_key = payload.api_key if payload.api_key not in (None, "") else existing.api_key if existing else None
    if _provider_requires_api_key(provider_value) and not effective_api_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"message": "API key is required for the selected provider"})
    repo.save_profile(
        profile_id=payload.id,
        display_name=payload.display_name,
        provider=payload.provider,
        model=payload.model,
        base_url=payload.base_url,
        api_key=effective_api_key,
    )
    _sync_client_profiles(metadata_db, llm_client)
    return _build_response(metadata_db, llm_client)


@router.put("/settings/llm/routing", response_model=LLMSettingsResponse)
async def update_llm_routing(
    payload: LLMRoutingUpdate,
    llm_client: LLMClient = Depends(get_llm_client),
    metadata_db: MetadataDB = Depends(get_metadata_db),
) -> LLMSettingsResponse:
    repo = LLMSettingsRepository(metadata_db)
    primary = repo.get_profile(payload.primary_profile_id)
    if primary is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Primary profile not found"})
    if payload.fallback_profile_id:
        fallback = repo.get_profile(payload.fallback_profile_id)
        if fallback is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Fallback profile not found"})
        if payload.fallback_profile_id == payload.primary_profile_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"message": "Fallback profile must be different from primary profile"})
    repo.save_routing(primary_profile_id=payload.primary_profile_id, fallback_profile_id=payload.fallback_profile_id)
    _sync_client_profiles(metadata_db, llm_client)
    return _build_response(metadata_db, llm_client)


@router.delete("/settings/llm/profiles/{profile_id}", response_model=LLMSettingsResponse)
async def delete_llm_profile(
    profile_id: str,
    llm_client: LLMClient = Depends(get_llm_client),
    metadata_db: MetadataDB = Depends(get_metadata_db),
) -> LLMSettingsResponse:
    repo = LLMSettingsRepository(metadata_db)
    if repo.get_profile(profile_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Profile not found"})
    repo.delete_profile(profile_id)
    _sync_client_profiles(metadata_db, llm_client)
    return _build_response(metadata_db, llm_client)


@router.post("/settings/llm/test", response_model=LLMTestResult)
async def test_llm_settings(
    payload: LLMProfileUpsert,
    llm_client: LLMClient = Depends(get_llm_client),
    metadata_db: MetadataDB = Depends(get_metadata_db),
) -> LLMTestResult:
    repo = LLMSettingsRepository(metadata_db)
    existing = repo.get_profile(payload.id) if payload.id else None
    provider_value = getattr(payload.provider, "value", payload.provider)
    effective_api_key = payload.api_key if payload.api_key not in (None, "") else existing.api_key if existing else None
    if _provider_requires_api_key(provider_value) and not effective_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": "API key is required for the selected provider"},
        )
    success, latency, message = await llm_client.test_connection(
        provider=provider_value,
        model=payload.model,
        api_key=effective_api_key,
        base_url=payload.base_url,
    )
    return LLMTestResult(
        success=success,
        provider=payload.provider,
        model=payload.model,
        latency_ms=round(latency, 2),
        message=message if success else f"Connection test failed: {message}",
    )
