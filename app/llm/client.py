from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from collections.abc import AsyncIterator
from dataclasses import dataclass

from app.schemas.llm import LLMProvider, LLMProviderOption


@dataclass(slots=True)
class _CacheEntry:
    value: str
    expires_at: float


def _provider_catalog() -> list[LLMProviderOption]:
    return [
        LLMProviderOption(
            id=LLMProvider.NVIDIA,
            label="NVIDIA NIM",
            description="NVIDIA hosted inference API, OpenAI-compatible.",
            default_model="stepfun-ai/step-3.5-flash",
            default_base_url="https://integrate.api.nvidia.com/v1",
        ),
        LLMProviderOption(
            id=LLMProvider.OPENAI,
            label="OpenAI",
            description="OpenAI official chat completions API.",
            default_model="gpt-4.1-mini",
            default_base_url="https://api.openai.com/v1",
        ),
        LLMProviderOption(
            id=LLMProvider.ANTHROPIC,
            label="Anthropic",
            description="Anthropic Messages API.",
            default_model="claude-3-5-sonnet-latest",
            default_base_url="https://api.anthropic.com",
            protocol="anthropic",
        ),
        LLMProviderOption(
            id=LLMProvider.OPENROUTER,
            label="OpenRouter",
            description="OpenRouter unified LLM gateway, OpenAI-compatible.",
            default_model="openai/gpt-4.1-mini",
            default_base_url="https://openrouter.ai/api/v1",
        ),
        LLMProviderOption(
            id=LLMProvider.OLLAMA,
            label="Ollama",
            description="Local Ollama endpoint, OpenAI-compatible when v1 is enabled.",
            default_model="llama3.1:8b",
            default_base_url="http://localhost:11434/v1",
            api_key_required=False,
        ),
        LLMProviderOption(
            id=LLMProvider.CUSTOM,
            label="Custom",
            description="Custom OpenAI-compatible provider.",
            default_model="custom-model",
            default_base_url="https://your-endpoint/v1",
            api_key_required=False,
        ),
    ]


class LLMClient:
    def __init__(self, settings):
        self.timeout_seconds = int(getattr(settings, "llm_timeout_seconds", 30) or 30)
        self.max_retries = int(getattr(settings, "llm_max_retries", 2) or 2)
        self.cache_ttl_seconds = int(getattr(settings, "llm_cache_ttl_seconds", 3600) or 3600)
        self._cache: dict[str, _CacheEntry] = {}
        self.last_cache_hit = False
        self.last_cache_key: str | None = None
        self.last_provider_error: str | None = None
        self.catalog = _provider_catalog()
        self.primary_profile_id: str | None = None
        self.fallback_profile_id: str | None = None
        self._profiles: dict[str, dict[str, str | None]] = {}
        self.configure_from_settings(settings)

    def configure_from_settings(self, settings) -> None:
        provider = getattr(settings, "llm_provider", "openai")
        model = getattr(settings, "llm_model", "gpt-4.1-mini")
        api_key = (
            getattr(settings, "llm_api_key", None)
            or getattr(settings, "openai_api_key", None)
            or (getattr(settings, "anthropic_api_key", None) if provider == "anthropic" else None)
        )
        base_url = getattr(settings, "llm_base_url", None)
        self.configure(provider=provider, model=model, api_key=api_key, base_url=base_url)

    def configure(self, *, provider: str, model: str, api_key: str | None = None, base_url: str | None = None) -> None:
        self.provider = provider
        self.model = model
        self.api_key = api_key
        self.base_url = self._normalize_base_url(provider, base_url)
        self._cache.clear()
        self.primary_profile_id = "runtime-default"
        self.fallback_profile_id = None
        self._profiles = {
            "runtime-default": {
                "provider": provider,
                "model": model,
                "api_key": api_key,
                "base_url": self.base_url or None,
            }
        }

    def configure_profiles(
        self,
        *,
        profiles: list[dict[str, str | None]],
        primary_profile_id: str | None,
        fallback_profile_id: str | None = None,
    ) -> None:
        indexed = {str(profile["id"]): profile for profile in profiles if profile.get("id")}
        self._profiles = indexed
        self.primary_profile_id = primary_profile_id
        self.fallback_profile_id = fallback_profile_id if fallback_profile_id in indexed else None
        active = indexed.get(primary_profile_id or "")
        if active:
            self.provider = str(active.get("provider") or "openai")
            self.model = str(active.get("model") or "gpt-4.1-mini")
            self.api_key = active.get("api_key")
            self.base_url = self._normalize_base_url(self.provider, active.get("base_url"))
        self._cache.clear()

    def export_settings(self) -> dict[str, str | None]:
        return {
            "provider": self.provider,
            "model": self.model,
            "api_key": self.api_key,
            "base_url": self.base_url or None,
        }

    def provider_options(self) -> list[LLMProviderOption]:
        return list(self.catalog)

    async def chat(self, system_prompt: str, user_prompt: str) -> tuple[str, float]:
        cache_key = self._cache_key(system_prompt, user_prompt)
        self.last_cache_key = cache_key
        cached = self._cache_get(cache_key)
        if cached is not None:
            self.last_cache_hit = True
            self.last_provider_error = None
            return cached, 0.0

        self.last_cache_hit = False
        self.last_provider_error = None
        try:
            text, latency = await self._perform_provider_call(system_prompt, user_prompt)
            self._cache_set(cache_key, text)
            return text, latency
        except Exception as exc:  # noqa: BLE001
            self.last_provider_error = str(exc)
            fallback = self._fallback(system_prompt, user_prompt)
            self._cache_set(cache_key, fallback)
            return fallback, 0.0

    async def stream_chat(self, system_prompt: str, user_prompt: str) -> AsyncIterator[str]:
        text, _ = await self.chat(system_prompt, user_prompt)
        if not text:
            return

        chunk_size = 24
        for start in range(0, len(text), chunk_size):
            yield text[start : start + chunk_size]
            await asyncio.sleep(0.015)

    async def test_connection(self, *, provider: str | None = None, model: str | None = None, api_key: str | None = None, base_url: str | None = None) -> tuple[bool, float, str]:
        snapshot = self.export_settings()
        if provider and model:
            self.configure(provider=provider, model=model, api_key=api_key, base_url=base_url)
        start = time.perf_counter()
        try:
            text, _ = await self._perform_provider_call("You are a connectivity probe.", "Reply with OK only.")
            latency = (time.perf_counter() - start) * 1000
            return True, latency, text[:120] or "OK"
        except Exception as exc:  # noqa: BLE001
            latency = (time.perf_counter() - start) * 1000
            return False, latency, str(exc)
        finally:
            self.configure(
                provider=str(snapshot["provider"] or "openai"),
                model=str(snapshot["model"] or "gpt-4.1-mini"),
                api_key=snapshot["api_key"],
                base_url=snapshot["base_url"],
            )

    async def _chat_with_retry(self, func, system_prompt: str, user_prompt: str) -> tuple[str, float]:
        delay = 0.5
        last_error: Exception | None = None
        attempts = max(1, self.max_retries + 1)
        for attempt in range(attempts):
            start = time.perf_counter()
            try:
                text = await func(system_prompt, user_prompt)
                self.last_provider_error = None
                return text, (time.perf_counter() - start) * 1000
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self.last_provider_error = str(exc)
                if attempt >= attempts - 1:
                    break
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8.0)
        if last_error is not None:
            raise last_error
        raise RuntimeError("Unknown LLM provider error")

    async def _perform_provider_call(self, system_prompt: str, user_prompt: str) -> tuple[str, float]:
        profiles_to_try = self._profiles_to_try()
        last_error: Exception | None = None
        for profile in profiles_to_try:
            previous = self.export_settings()
            self.provider = str(profile.get("provider") or "openai")
            self.model = str(profile.get("model") or "gpt-4.1-mini")
            self.api_key = profile.get("api_key")
            self.base_url = self._normalize_base_url(self.provider, profile.get("base_url"))
            try:
                if self._protocol(self.provider) == "anthropic":
                    if not self.api_key:
                        raise ValueError("Anthropic API key is not configured")
                    return await self._chat_with_retry(self._anthropic_chat, system_prompt, user_prompt)
                if self.provider not in {
                    getattr(LLMProvider.OLLAMA, "value", "ollama"),
                    getattr(LLMProvider.CUSTOM, "value", "custom"),
                } and not self.api_key:
                    raise ValueError("Provider API key is not configured")
                return await self._chat_with_retry(self._openai_compatible_chat, system_prompt, user_prompt)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self.last_provider_error = str(exc)
            finally:
                self.provider = str(previous["provider"] or self.provider)
                self.model = str(previous["model"] or self.model)
                self.api_key = previous["api_key"]
                self.base_url = self._normalize_base_url(self.provider, previous["base_url"] or self.base_url)
        if last_error is not None:
            raise last_error
        raise RuntimeError("No LLM profile configured")

    def _profiles_to_try(self) -> list[dict[str, str | None]]:
        if not self._profiles:
            return [{"id": "runtime-default", **self.export_settings()}]
        ordered: list[dict[str, str | None]] = []
        seen: set[str] = set()
        for profile_id in [self.primary_profile_id, self.fallback_profile_id]:
            if profile_id and profile_id in self._profiles and profile_id not in seen:
                ordered.append(self._profiles[profile_id])
                seen.add(profile_id)
        for profile_id, profile in self._profiles.items():
            if profile_id not in seen:
                ordered.append(profile)
        return ordered

    async def _openai_compatible_chat(self, system_prompt: str, user_prompt: str) -> str:
        payload = json.dumps(
            {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            }
        ).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(
            f"{self.base_url}/chat/completions",
            data=payload,
            headers=headers,
            method="POST",
        )
        response = await asyncio.to_thread(self._read, request)
        data = json.loads(response)
        return data["choices"][0]["message"]["content"].strip()

    async def _anthropic_chat(self, system_prompt: str, user_prompt: str) -> str:
        payload = json.dumps(
            {
                "model": self.model,
                "max_tokens": 1024,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url or 'https://api.anthropic.com'}/v1/messages",
            data=payload,
            headers={
                "x-api-key": self.api_key or "",
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            method="POST",
        )
        response = await asyncio.to_thread(self._read, request)
        data = json.loads(response)
        text = "".join(block.get("text", "") for block in data.get("content", []) if block.get("type") == "text")
        return text.strip()

    def _read(self, request: urllib.request.Request) -> str:
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read().decode("utf-8")

    def _fallback(self, system_prompt: str, user_prompt: str) -> str:
        lowered = f"{system_prompt}\n{user_prompt}".lower()
        if "error" in lowered:
            return "SELECT 1 LIMIT 1000;"
        if "keyword" in lowered or "rewrite" in lowered:
            return ""
        return ""

    def _cache_key(self, system_prompt: str, user_prompt: str) -> str:
        return json.dumps(
            {
                "provider": self.provider,
                "model": self.model,
                "base_url": self.base_url,
                "system": system_prompt,
                "user": user_prompt,
            },
            sort_keys=True,
            ensure_ascii=False,
        )

    def _cache_get(self, key: str) -> str | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        if entry.expires_at < time.time():
            self._cache.pop(key, None)
            return None
        return entry.value

    def _cache_set(self, key: str, value: str) -> None:
        if not value:
            return
        self._cache[key] = _CacheEntry(value=value, expires_at=time.time() + self.cache_ttl_seconds)

    def _default_base_url(self, provider: str) -> str | None:
        for option in self.catalog:
            if getattr(option.id, "value", option.id) == provider:
                return option.default_base_url
        return None

    def _normalize_base_url(self, provider: str, base_url: str | None) -> str:
        normalized = str(base_url or self._default_base_url(provider) or "").rstrip("/")
        if not normalized:
            return ""

        suffixes = [
            "/chat/completions",
            "/v1/chat/completions",
        ]
        if self._protocol(provider) == "anthropic":
            suffixes.extend(
                [
                    "/messages",
                    "/v1/messages",
                ]
            )

        for suffix in suffixes:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)].rstrip("/")
                break

        return normalized

    def _protocol(self, provider: str) -> str:
        for option in self.catalog:
            if getattr(option.id, "value", option.id) == provider:
                return option.protocol
        return "openai"
