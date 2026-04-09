"""Vector store implementation for semantic search with lifecycle management."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Protocol

try:
    import chromadb
    from chromadb.config import Settings
except ImportError:  # pragma: no cover
    chromadb = None

logger = logging.getLogger(__name__)


def _sanitize_chroma_metadata_item(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (list, tuple)):
        cleaned = [_sanitize_chroma_metadata_item(item) for item in value]
        cleaned = [item for item in cleaned if item is not None and item != ""]
        return cleaned or None
    if isinstance(value, dict):
        if not value:
            return None
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def sanitize_chroma_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in metadata.items():
        normalized = _sanitize_chroma_metadata_item(value)
        if normalized is None:
            continue
        cleaned[key] = normalized
    return cleaned


# Error classes
class VectorStoreError(Exception):
    """Base exception for vector store errors."""
    pass


class VectorStoreUnavailableError(VectorStoreError):
    """Exception raised when vector store is unavailable."""
    pass


class VectorStoreTimeoutError(VectorStoreError):
    """Exception raised when vector store operation times out."""
    pass


@dataclass
class VectorStoreConfig:
    """Configuration for vector store.
    
    Attributes:
        persist_directory: Directory for persistent storage
        timeout: Timeout in seconds for operations
        max_retries: Maximum number of retry attempts
        retry_interval: Interval in seconds between retries
    """
    persist_directory: str = "./chroma"
    timeout: float = 3.0
    max_retries: int = 3
    retry_interval: float = 1.0


@dataclass
class VectorSearchResult:
    """Result from vector search.
    
    Attributes:
        id: Document ID
        score: Similarity score (distance)
        metadata: Document metadata
        document: Document text
    """
    id: str
    score: float
    metadata: dict[str, Any]
    document: str


@dataclass
class HealthStatus:
    """Health status of vector store.
    
    Attributes:
        is_healthy: Whether the vector store is healthy
        connection_status: Connection status description
        indexed_count: Number of indexed vectors
        last_update_time: Last update timestamp
        error_message: Error message if unhealthy
    """
    is_healthy: bool
    connection_status: str
    indexed_count: int
    last_update_time: datetime | None
    error_message: str | None = None


class VectorStore(Protocol):
    """Protocol for vector store interface.
    
    Defines the contract for vector database implementations that support
    semantic search with lifecycle management.
    """
    
    async def initialize(self) -> None:
        """Initialize the vector store.
        
        Raises:
            VectorStoreError: If initialization fails
        """
        ...
    
    async def upsert(
        self,
        ids: list[str],
        vectors: list[list[float]],
        metadatas: list[dict[str, Any]],
        documents: list[str]
    ) -> None:
        """Insert or update vectors.
        
        Args:
            ids: List of document IDs
            vectors: List of embedding vectors
            metadatas: List of metadata dictionaries
            documents: List of document texts
            
        Raises:
            VectorStoreError: If upsert fails
            VectorStoreTimeoutError: If operation times out
        """
        ...
    
    async def query(
        self,
        query_vector: list[float],
        top_k: int,
        filter: dict[str, Any] | None = None
    ) -> list[VectorSearchResult]:
        """Query for similar vectors.
        
        Args:
            query_vector: Query embedding vector
            top_k: Number of results to return
            filter: Optional metadata filter
            
        Returns:
            List of search results ordered by similarity
            
        Raises:
            VectorStoreError: If query fails
            VectorStoreTimeoutError: If operation times out
        """
        ...
    
    async def delete(self, ids: list[str]) -> None:
        """Delete vectors by IDs.
        
        Args:
            ids: List of document IDs to delete
            
        Raises:
            VectorStoreError: If deletion fails
        """
        ...
    
    async def health_check(self) -> HealthStatus:
        """Check health status of vector store.
        
        Returns:
            Health status information
        """
        ...
    
    async def backup(self, path: str) -> None:
        """Backup vector store data.
        
        Args:
            path: Backup destination path
            
        Raises:
            VectorStoreError: If backup fails
        """
        ...
    
    async def close(self) -> None:
        """Close vector store connection."""
        ...


class ChromaVectorStore:
    """Chroma-based vector store implementation with lifecycle management.
    
    Provides persistent vector storage with automatic reconnection,
    health checks, and backup capabilities.
    """
    
    def __init__(self, config: VectorStoreConfig | None = None, collection_name: str = "default"):
        """Initialize Chroma vector store.
        
        Args:
            config: Vector store configuration
            collection_name: Name of the collection to use
            
        Raises:
            VectorStoreError: If chromadb is not installed
        """
        if chromadb is None:
            raise VectorStoreError(
                "chromadb is not installed. "
                "Install it with: pip install chromadb"
            )
        
        self.config = config or VectorStoreConfig()
        self.collection_name = collection_name
        self._client: chromadb.Client | None = None
        self._collection: chromadb.Collection | None = None
        self._last_update_time: datetime | None = None
        self._is_initialized = False
    
    async def initialize(self) -> None:
        """Initialize Chroma client and collection with retry logic.
        
        Raises:
            VectorStoreUnavailableError: If initialization fails after all retries
        """
        for attempt in range(self.config.max_retries):
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._initialize_sync),
                    timeout=self.config.timeout
                )
                self._is_initialized = True
                logger.info(f"Vector store initialized successfully (collection: {self.collection_name})")
                return
            
            except asyncio.TimeoutError:
                logger.warning(
                    f"Vector store initialization timeout (attempt {attempt + 1}/{self.config.max_retries})"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_interval)
            
            except Exception as e:
                logger.error(
                    f"Vector store initialization failed (attempt {attempt + 1}/{self.config.max_retries}): {e}"
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_interval)
        
        raise VectorStoreUnavailableError(
            f"Failed to initialize vector store after {self.config.max_retries} attempts"
        )
    
    def _initialize_sync(self) -> None:
        """Synchronous initialization of Chroma client."""
        # Create persist directory if it doesn't exist
        persist_dir = Path(self.config.persist_directory)
        persist_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Chroma client with persistence
        self._client = chromadb.PersistentClient(
            path=str(persist_dir),
            settings=Settings(anonymized_telemetry=False),
        )
        
        # Get or create collection
        self._collection = self._client.get_or_create_collection(
            name=self.collection_name,
            metadata={"hnsw:space": "cosine"}  # Use cosine similarity
        )
    
    async def upsert(
        self,
        ids: list[str],
        vectors: list[list[float]],
        metadatas: list[dict[str, Any]],
        documents: list[str]
    ) -> None:
        """Insert or update vectors with timeout control.
        
        Args:
            ids: List of document IDs
            vectors: List of embedding vectors
            metadatas: List of metadata dictionaries
            documents: List of document texts
            
        Raises:
            VectorStoreUnavailableError: If vector store is not initialized
            VectorStoreTimeoutError: If operation times out
            VectorStoreError: If upsert fails
        """
        if not self._is_initialized or self._collection is None:
            raise VectorStoreUnavailableError("Vector store is not initialized")
        
        if not ids or len(ids) != len(vectors) or len(ids) != len(metadatas) or len(ids) != len(documents):
            raise VectorStoreError("All input lists must have the same non-zero length")
        metadatas = [sanitize_chroma_metadata(metadata) for metadata in metadatas]
        
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    self._collection.upsert,
                    ids=ids,
                    embeddings=vectors,
                    metadatas=metadatas,
                    documents=documents
                ),
                timeout=self.config.timeout
            )
            self._last_update_time = datetime.now()
            logger.debug(f"Upserted {len(ids)} vectors to collection {self.collection_name}")
        
        except asyncio.TimeoutError:
            raise VectorStoreTimeoutError(
                f"Upsert operation timed out after {self.config.timeout}s"
            )
        except Exception as e:
            raise VectorStoreError(f"Upsert failed: {e}")
    
    async def query(
        self,
        query_vector: list[float],
        top_k: int,
        filter: dict[str, Any] | None = None
    ) -> list[VectorSearchResult]:
        """Query for similar vectors with timeout control.
        
        Args:
            query_vector: Query embedding vector
            top_k: Number of results to return
            filter: Optional metadata filter (Chroma where clause)
            
        Returns:
            List of search results ordered by similarity
            
        Raises:
            VectorStoreUnavailableError: If vector store is not initialized
            VectorStoreTimeoutError: If operation times out
            VectorStoreError: If query fails
        """
        if not self._is_initialized or self._collection is None:
            raise VectorStoreUnavailableError("Vector store is not initialized")
        
        try:
            results = await asyncio.wait_for(
                asyncio.to_thread(
                    self._collection.query,
                    query_embeddings=[query_vector],
                    n_results=top_k,
                    where=filter
                ),
                timeout=self.config.timeout
            )
            
            # Parse Chroma results into VectorSearchResult objects
            search_results = []
            if results and results['ids'] and results['ids'][0]:
                for i, doc_id in enumerate(results['ids'][0]):
                    search_results.append(VectorSearchResult(
                        id=doc_id,
                        score=results['distances'][0][i] if results['distances'] else 0.0,
                        metadata=results['metadatas'][0][i] if results['metadatas'] else {},
                        document=results['documents'][0][i] if results['documents'] else ""
                    ))
            
            logger.debug(f"Query returned {len(search_results)} results from collection {self.collection_name}")
            return search_results
        
        except asyncio.TimeoutError:
            raise VectorStoreTimeoutError(
                f"Query operation timed out after {self.config.timeout}s"
            )
        except Exception as e:
            raise VectorStoreError(f"Query failed: {e}")
    
    async def delete(self, ids: list[str]) -> None:
        """Delete vectors by IDs.
        
        Args:
            ids: List of document IDs to delete
            
        Raises:
            VectorStoreUnavailableError: If vector store is not initialized
            VectorStoreError: If deletion fails
        """
        if not self._is_initialized or self._collection is None:
            raise VectorStoreUnavailableError("Vector store is not initialized")
        
        if not ids:
            return
        
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._collection.delete, ids=ids),
                timeout=self.config.timeout
            )
            logger.debug(f"Deleted {len(ids)} vectors from collection {self.collection_name}")
        
        except asyncio.TimeoutError:
            raise VectorStoreTimeoutError(
                f"Delete operation timed out after {self.config.timeout}s"
            )
        except Exception as e:
            raise VectorStoreError(f"Delete failed: {e}")
    
    async def delete_by_metadata(self, filter: dict[str, Any]) -> None:
        """Delete vectors by metadata filter.
        
        Args:
            filter: Metadata filter (Chroma where clause)
            
        Raises:
            VectorStoreUnavailableError: If vector store is not initialized
            VectorStoreError: If deletion fails
        """
        if not self._is_initialized or self._collection is None:
            raise VectorStoreUnavailableError("Vector store is not initialized")
        
        if not filter:
            return
        
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._collection.delete, where=filter),
                timeout=self.config.timeout
            )
            logger.debug(f"Deleted vectors matching filter from collection {self.collection_name}")
        
        except asyncio.TimeoutError:
            raise VectorStoreTimeoutError(
                f"Delete by metadata operation timed out after {self.config.timeout}s"
            )
        except Exception as e:
            raise VectorStoreError(f"Delete by metadata failed: {e}")
    
    async def get_collection_stats(self) -> dict[str, Any]:
        """Get collection statistics.
        
        Returns:
            Dictionary with collection stats (count, dimension, etc.)
        """
        if not self._is_initialized or self._collection is None:
            return {'count': 0, 'dimension': 0}
        
        try:
            count = await asyncio.wait_for(
                asyncio.to_thread(self._collection.count),
                timeout=self.config.timeout
            )
            
            # Try to get dimension from first vector
            dimension = 0
            if count > 0:
                results = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._collection.get,
                        limit=1,
                        include=['embeddings']
                    ),
                    timeout=self.config.timeout
                )
                embeddings = results.get('embeddings') if results else None
                if embeddings is not None and len(embeddings) > 0:
                    dimension = len(embeddings[0])
            
            return {
                'count': count,
                'dimension': dimension,
                'collection_name': self.collection_name
            }
        
        except Exception as e:
            logger.error(f"Failed to get collection stats: {e}")
            return {'count': 0, 'dimension': 0}
    
    async def health_check(self) -> HealthStatus:
        """Check health status of vector store.
        
        Returns:
            Health status information
        """
        if not self._is_initialized or self._collection is None:
            return HealthStatus(
                is_healthy=False,
                connection_status="not_initialized",
                indexed_count=0,
                last_update_time=None,
                error_message="Vector store is not initialized"
            )
        
        try:
            # Try to get collection count
            count = await asyncio.wait_for(
                asyncio.to_thread(self._collection.count),
                timeout=self.config.timeout
            )
            
            return HealthStatus(
                is_healthy=True,
                connection_status="connected",
                indexed_count=count,
                last_update_time=self._last_update_time
            )
        
        except asyncio.TimeoutError:
            return HealthStatus(
                is_healthy=False,
                connection_status="timeout",
                indexed_count=0,
                last_update_time=self._last_update_time,
                error_message=f"Health check timed out after {self.config.timeout}s"
            )
        except Exception as e:
            return HealthStatus(
                is_healthy=False,
                connection_status="error",
                indexed_count=0,
                last_update_time=self._last_update_time,
                error_message=str(e)
            )
    
    async def backup(self, path: str) -> None:
        """Backup vector store data by copying persist directory.
        
        Args:
            path: Backup destination path
            
        Raises:
            VectorStoreError: If backup fails
        """
        try:
            import shutil
            
            source = Path(self.config.persist_directory)
            destination = Path(path)
            
            if not source.exists():
                raise VectorStoreError(f"Source directory does not exist: {source}")
            
            # Create parent directory if needed
            destination.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy directory
            await asyncio.to_thread(
                shutil.copytree,
                source,
                destination,
                dirs_exist_ok=True
            )
            
            logger.info(f"Vector store backed up to {path}")
        
        except Exception as e:
            raise VectorStoreError(f"Backup failed: {e}")
    
    async def close(self) -> None:
        """Close vector store connection and cleanup resources."""
        if self._client is not None:
            # Chroma client doesn't have explicit close, but we can clear references
            self._collection = None
            self._client = None
            self._is_initialized = False
            logger.info("Vector store connection closed")


class InMemoryVectorStore:
    """Lightweight vector store fallback used when Chroma is unavailable."""

    def __init__(self, config: VectorStoreConfig | None = None, collection_name: str = "default"):
        self.config = config or VectorStoreConfig()
        self.collection_name = collection_name
        self._is_initialized = False
        self._vectors: dict[str, list[float]] = {}
        self._documents: dict[str, str] = {}
        self._metadatas: dict[str, dict[str, Any]] = {}
        self._last_update_time: datetime | None = None

    async def initialize(self) -> None:
        self._is_initialized = True

    async def upsert(
        self,
        ids: list[str],
        vectors: list[list[float]],
        metadatas: list[dict[str, Any]],
        documents: list[str],
    ) -> None:
        if not self._is_initialized:
            raise VectorStoreUnavailableError("Vector store is not initialized")
        if not ids or len(ids) != len(vectors) or len(ids) != len(metadatas) or len(ids) != len(documents):
            raise VectorStoreError("All input lists must have the same non-zero length")
        for idx, doc_id in enumerate(ids):
            self._vectors[doc_id] = list(vectors[idx])
            self._documents[doc_id] = documents[idx]
            self._metadatas[doc_id] = sanitize_chroma_metadata(dict(metadatas[idx]))
        self._last_update_time = datetime.now()

    async def query(
        self,
        query_vector: list[float],
        top_k: int,
        filter: dict[str, Any] | None = None,
    ) -> list[VectorSearchResult]:
        if not self._is_initialized:
            raise VectorStoreUnavailableError("Vector store is not initialized")
        scored: list[VectorSearchResult] = []
        for doc_id, vector in self._vectors.items():
            metadata = self._metadatas.get(doc_id, {})
            if filter and not self._matches_filter(metadata, filter):
                continue
            score = self._cosine_similarity(query_vector, vector)
            scored.append(
                VectorSearchResult(
                    id=doc_id,
                    score=score,
                    metadata=metadata,
                    document=self._documents.get(doc_id, ""),
                )
            )
        scored.sort(key=lambda item: item.score, reverse=True)
        return scored[:top_k]

    async def delete(self, ids: list[str]) -> None:
        if not self._is_initialized:
            raise VectorStoreUnavailableError("Vector store is not initialized")
        for doc_id in ids:
            self._vectors.pop(doc_id, None)
            self._documents.pop(doc_id, None)
            self._metadatas.pop(doc_id, None)

    async def health_check(self) -> HealthStatus:
        return HealthStatus(
            is_healthy=self._is_initialized,
            connection_status="connected" if self._is_initialized else "not_initialized",
            indexed_count=len(self._vectors),
            last_update_time=self._last_update_time,
            error_message=None if self._is_initialized else "Vector store is not initialized",
        )

    async def backup(self, path: str) -> None:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "collection_name": self.collection_name,
            "vectors": self._vectors,
            "documents": self._documents,
            "metadatas": self._metadatas,
        }
        await asyncio.to_thread(destination.write_text, json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    async def close(self) -> None:
        self._is_initialized = False

    @staticmethod
    def _cosine_similarity(left: list[float], right: list[float]) -> float:
        if not left or not right:
            return 0.0
        length = min(len(left), len(right))
        dot = sum(left[i] * right[i] for i in range(length))
        left_norm = sum(value * value for value in left[:length]) ** 0.5 or 1.0
        right_norm = sum(value * value for value in right[:length]) ** 0.5 or 1.0
        return max(0.0, min(1.0, dot / (left_norm * right_norm)))

    @classmethod
    def _matches_filter(cls, metadata: dict[str, Any], filter_clause: dict[str, Any]) -> bool:
        if not filter_clause:
            return True
        if "$and" in filter_clause:
            return all(cls._matches_filter(metadata, clause) for clause in filter_clause["$and"])
        for key, value in filter_clause.items():
            if isinstance(value, dict) and "$in" in value:
                if metadata.get(key) not in value["$in"]:
                    return False
                continue
            if metadata.get(key) != value:
                return False
        return True


def create_vector_store(
    config: VectorStoreConfig | None = None,
    *,
    collection_name: str = "default",
    prefer_chroma: bool = True,
) -> VectorStore:
    """Return the best available vector store implementation."""

    config = config or VectorStoreConfig()
    if prefer_chroma and chromadb is not None:
        return ChromaVectorStore(config=config, collection_name=collection_name)
    return InMemoryVectorStore(config=config, collection_name=collection_name)
