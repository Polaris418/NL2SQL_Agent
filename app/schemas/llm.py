from __future__ import annotations

from enum import Enum

from .base import SchemaModel
from .pydantic_compat import Field


class LLMProvider(str, Enum):
    OPENAI = "openai"
    NVIDIA = "nvidia"
    ANTHROPIC = "anthropic"
    OPENROUTER = "openrouter"
    OLLAMA = "ollama"
    CUSTOM = "custom"


class LLMProviderOption(SchemaModel):
    id: LLMProvider
    label: str
    description: str
    default_model: str
    default_base_url: str | None = None
    api_key_required: bool = True
    protocol: str = "openai"


class LLMProfileUpsert(SchemaModel):
    id: str | None = None
    display_name: str = Field(min_length=1)
    provider: LLMProvider
    model: str = Field(min_length=1)
    api_key: str | None = None
    base_url: str | None = None


class LLMRoutingUpdate(SchemaModel):
    primary_profile_id: str
    fallback_profile_id: str | None = None


class LLMProfileResponse(SchemaModel):
    id: str
    display_name: str
    provider: LLMProvider
    model: str
    base_url: str | None = None
    has_api_key: bool = False
    api_key_masked: str | None = None
    updated_at: str | None = None


class LLMSettingsResponse(SchemaModel):
    active_profile_id: str | None = None
    fallback_profile_id: str | None = None
    profiles: list[LLMProfileResponse] = Field(default_factory=list)
    providers: list[LLMProviderOption] = Field(default_factory=list)


class LLMTestResult(SchemaModel):
    success: bool
    provider: LLMProvider
    model: str
    latency_ms: float = 0.0
    message: str
