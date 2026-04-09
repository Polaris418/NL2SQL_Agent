"""Embedding model implementation for semantic vectorization."""

from __future__ import annotations

import asyncio
import math
import os
import re
from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Protocol

try:
    from sentence_transformers import SentenceTransformer
except ImportError:  # pragma: no cover
    SentenceTransformer = None


# Error classes
class EmbeddingError(Exception):
    """Base exception for embedding-related errors."""
    pass


class EmbeddingTimeoutError(EmbeddingError):
    """Exception raised when embedding operation times out."""
    pass


@dataclass
class EmbeddingConfig:
    """Configuration for embedding model.
    
    Attributes:
        model_name: Name of the embedding model
        provider: Provider type (e.g., 'local', 'openai', 'huggingface')
        timeout: Timeout in seconds for embedding operations
        max_retries: Maximum number of retry attempts
        dimensions: Expected vector dimensions (informational)
    """
    model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"
    provider: str = "local"
    timeout: float = 2.0
    max_retries: int = 3
    dimensions: int = 384


class EmbeddingModel(Protocol):
    """Protocol for embedding model interface.
    
    Defines the contract for semantic embedding models that convert
    text into vector representations.
    """
    
    async def embed_text(self, text: str) -> list[float]:
        """Convert text to semantic vector.
        
        Args:
            text: Input text (supports multilingual content)
            
        Returns:
            Semantic vector (at least 384 dimensions)
            
        Raises:
            EmbeddingTimeoutError: If embedding exceeds timeout
            EmbeddingError: If embedding fails
        """
        ...
    
    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Convert multiple texts to semantic vectors in batch.
        
        Args:
            texts: List of input texts
            
        Returns:
            List of semantic vectors
            
        Raises:
            EmbeddingTimeoutError: If embedding exceeds timeout
            EmbeddingError: If embedding fails
        """
        ...
    
    @property
    def dimensions(self) -> int:
        """Return the vector dimensions."""
        ...
    
    @property
    def model_name(self) -> str:
        """Return the model name."""
        ...


class SentenceTransformerEmbedding:
    """Sentence-transformers based embedding model implementation.
    
    Supports local embedding models via sentence-transformers library.
    Includes timeout control and retry mechanism for robustness.
    """
    
    def __init__(self, config: EmbeddingConfig | None = None):
        """Initialize the embedding model.
        
        Args:
            config: Embedding configuration. If None, uses default config.
            
        Raises:
            EmbeddingError: If sentence-transformers is not installed
        """
        if SentenceTransformer is None:
            raise EmbeddingError(
                "sentence-transformers is not installed. "
                "Install it with: pip install sentence-transformers"
            )
        
        self.config = config or EmbeddingConfig()
        self._model: SentenceTransformer | None = None
        self._dimensions: int | None = None
        self._cache_folder = self._resolve_cache_folder()

    @staticmethod
    def _resolve_cache_folder() -> str:
        configured = (
            os.environ.get("SENTENCE_TRANSFORMERS_HOME")
            or os.environ.get("HF_HOME")
            or os.environ.get("TRANSFORMERS_CACHE")
        )
        base_path = Path(configured) if configured else Path.cwd() / ".cache" / "huggingface"
        base_path.mkdir(parents=True, exist_ok=True)
        return str(base_path)
    
    def _ensure_model_loaded(self) -> SentenceTransformer:
        """Lazy load the model on first use.
        
        Returns:
            Loaded SentenceTransformer model
        """
        if self._model is None:
            self._model = SentenceTransformer(
                self.config.model_name,
                cache_folder=self._cache_folder,
            )
            # Get actual dimensions from model
            self._dimensions = self._model.get_sentence_embedding_dimension()
        return self._model
    
    async def embed_text(self, text: str) -> list[float]:
        """Convert text to semantic vector with timeout and retry.
        
        Args:
            text: Input text
            
        Returns:
            Semantic vector
            
        Raises:
            EmbeddingTimeoutError: If all retry attempts timeout
            EmbeddingError: If embedding fails after all retries
        """
        for attempt in range(self.config.max_retries):
            try:
                # Run embedding in executor to avoid blocking
                result = await asyncio.wait_for(
                    asyncio.to_thread(self._embed_sync, text),
                    timeout=self.config.timeout
                )
                return result
            
            except asyncio.TimeoutError:
                if attempt == self.config.max_retries - 1:
                    raise EmbeddingTimeoutError(
                        f"Embedding timeout after {self.config.max_retries} attempts "
                        f"(timeout={self.config.timeout}s)"
                    )
                # Exponential backoff
                await asyncio.sleep(0.1 * (attempt + 1))
            
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise EmbeddingError(f"Embedding failed after {self.config.max_retries} attempts: {e}")
                # Exponential backoff
                await asyncio.sleep(0.1 * (attempt + 1))
        
        # Should never reach here, but for type safety
        raise EmbeddingError("Unexpected error in embed_text")
    
    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Convert multiple texts to semantic vectors with timeout and retry.
        
        Args:
            texts: List of input texts
            
        Returns:
            List of semantic vectors
            
        Raises:
            EmbeddingTimeoutError: If all retry attempts timeout
            EmbeddingError: If embedding fails after all retries
        """
        if not texts:
            return []
        
        for attempt in range(self.config.max_retries):
            try:
                # Run batch embedding in executor
                result = await asyncio.wait_for(
                    asyncio.to_thread(self._embed_batch_sync, texts),
                    timeout=self.config.timeout * len(texts)  # Scale timeout with batch size
                )
                return result
            
            except asyncio.TimeoutError:
                if attempt == self.config.max_retries - 1:
                    raise EmbeddingTimeoutError(
                        f"Batch embedding timeout after {self.config.max_retries} attempts "
                        f"(timeout={self.config.timeout * len(texts)}s for {len(texts)} texts)"
                    )
                await asyncio.sleep(0.1 * (attempt + 1))
            
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise EmbeddingError(
                        f"Batch embedding failed after {self.config.max_retries} attempts: {e}"
                    )
                await asyncio.sleep(0.1 * (attempt + 1))
        
        raise EmbeddingError("Unexpected error in embed_batch")
    
    def _embed_sync(self, text: str) -> list[float]:
        """Synchronous embedding for single text.
        
        Args:
            text: Input text
            
        Returns:
            Semantic vector as list of floats
        """
        model = self._ensure_model_loaded()
        embedding = model.encode(text, convert_to_numpy=True)
        return embedding.tolist()
    
    def _embed_batch_sync(self, texts: list[str]) -> list[list[float]]:
        """Synchronous batch embedding.
        
        Args:
            texts: List of input texts
            
        Returns:
            List of semantic vectors
        """
        model = self._ensure_model_loaded()
        embeddings = model.encode(texts, convert_to_numpy=True)
        return embeddings.tolist()
    
    @property
    def dimensions(self) -> int:
        """Return the vector dimensions.
        
        Returns:
            Vector dimensions (e.g., 384 for MiniLM)
        """
        if self._dimensions is None:
            # Load model to get dimensions
            self._ensure_model_loaded()
        return self._dimensions or self.config.dimensions
    
    @property
    def model_name(self) -> str:
        """Return the model name.
        
        Returns:
            Model name string
        """
        return self.config.model_name


class DeterministicHashEmbedding:
    """Fallback embedding model used when a semantic model is unavailable.

    This is intentionally deterministic and multilingual-friendly enough to keep
    the RAG pipeline functional when sentence-transformers is not installed.
    It is not a replacement for a real embedding model, but it provides a
    stable production fallback and keeps the pipeline operational.
    """

    def __init__(self, config: EmbeddingConfig | None = None):
        self.config = config or EmbeddingConfig()
        self._dimensions = max(384, self.config.dimensions)

    async def embed_text(self, text: str) -> list[float]:
        return await asyncio.to_thread(self._embed_sync, text)

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        return await asyncio.to_thread(lambda: [self._embed_sync(text) for text in texts])

    def _embed_sync(self, text: str) -> list[float]:
        tokens = self._tokenize(text)
        vector = [0.0] * self._dimensions
        if not tokens:
            tokens = [text or ""]
        for index, token in enumerate(tokens):
            digest = sha256(token.encode("utf-8")).digest()
            weight = 1.0 / (1.0 + index)
            for dim in range(self._dimensions):
                vector[dim] += ((digest[dim % len(digest)] / 255.0) - 0.5) * weight
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        normalized = (text or "").lower()
        normalized = re.sub(r"[\u3000-\u303f\uff00-\uffef]", " ", normalized)
        normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", normalized)
        tokens = [token for token in normalized.split() if token]
        # Add light character n-grams to improve Chinese coverage.
        if len(tokens) == 1 and len(tokens[0]) > 2:
            token = tokens[0]
            tokens.extend([token[i : i + 2] for i in range(max(0, len(token) - 1))][:8])
        return tokens

    @property
    def dimensions(self) -> int:
        return self._dimensions

    @property
    def model_name(self) -> str:
        return f"deterministic-hash-fallback({self.config.model_name})"


def create_embedding_model(
    config: EmbeddingConfig | None = None,
    *,
    allow_fallback: bool = True,
) -> EmbeddingModel:
    """Create the best available embedding model for the current environment."""

    config = config or EmbeddingConfig()
    if SentenceTransformer is not None and config.provider.lower() in {"local", "sentence-transformers"}:
        try:
            return SentenceTransformerEmbedding(config)
        except EmbeddingError:
            if not allow_fallback:
                raise
    if allow_fallback:
        return DeterministicHashEmbedding(config)
    raise EmbeddingError("No embedding backend available")
