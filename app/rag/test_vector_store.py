"""Unit tests for vector store implementation."""

import asyncio
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.rag.vector_store import (
    ChromaVectorStore,
    HealthStatus,
    VectorSearchResult,
    VectorStoreConfig,
    VectorStoreError,
    VectorStoreTimeoutError,
    VectorStoreUnavailableError,
)


@pytest.fixture
def test_config():
    """Create test configuration with temporary directory."""
    return VectorStoreConfig(
        persist_directory="./test_chroma",
        timeout=3.0,
        max_retries=3,
        retry_interval=0.1  # Faster for tests
    )


@pytest.fixture
async def vector_store(test_config):
    """Create and initialize a test vector store."""
    store = ChromaVectorStore(config=test_config, collection_name="test_collection")
    await store.initialize()
    yield store
    await store.close()
    # Cleanup test directory
    test_dir = Path(test_config.persist_directory)
    if test_dir.exists():
        shutil.rmtree(test_dir)


class TestVectorStoreConfig:
    """Tests for VectorStoreConfig."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = VectorStoreConfig()
        assert config.persist_directory == "./chroma"
        assert config.timeout == 3.0
        assert config.max_retries == 3
        assert config.retry_interval == 1.0
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = VectorStoreConfig(
            persist_directory="./custom_chroma",
            timeout=5.0,
            max_retries=5,
            retry_interval=2.0
        )
        assert config.persist_directory == "./custom_chroma"
        assert config.timeout == 5.0
        assert config.max_retries == 5
        assert config.retry_interval == 2.0


class TestVectorSearchResult:
    """Tests for VectorSearchResult."""
    
    def test_vector_search_result_creation(self):
        """Test creating a VectorSearchResult."""
        result = VectorSearchResult(
            id="doc_1",
            score=0.85,
            metadata={"source": "test"},
            document="Test document"
        )
        assert result.id == "doc_1"
        assert result.score == 0.85
        assert result.metadata == {"source": "test"}
        assert result.document == "Test document"


class TestHealthStatus:
    """Tests for HealthStatus."""
    
    def test_healthy_status(self):
        """Test creating a healthy status."""
        from datetime import datetime
        now = datetime.now()
        status = HealthStatus(
            is_healthy=True,
            connection_status="connected",
            indexed_count=100,
            last_update_time=now
        )
        assert status.is_healthy is True
        assert status.connection_status == "connected"
        assert status.indexed_count == 100
        assert status.last_update_time == now
        assert status.error_message is None
    
    def test_unhealthy_status(self):
        """Test creating an unhealthy status."""
        status = HealthStatus(
            is_healthy=False,
            connection_status="error",
            indexed_count=0,
            last_update_time=None,
            error_message="Connection failed"
        )
        assert status.is_healthy is False
        assert status.connection_status == "error"
        assert status.indexed_count == 0
        assert status.last_update_time is None
        assert status.error_message == "Connection failed"


class TestChromaVectorStore:
    """Tests for ChromaVectorStore."""
    
    @pytest.mark.asyncio
    async def test_initialization(self, test_config):
        """Test vector store initialization."""
        store = ChromaVectorStore(config=test_config, collection_name="test_init")
        await store.initialize()
        
        assert store._is_initialized is True
        assert store._client is not None
        assert store._collection is not None
        
        await store.close()
        # Cleanup
        test_dir = Path(test_config.persist_directory)
        if test_dir.exists():
            shutil.rmtree(test_dir)
    
    @pytest.mark.asyncio
    async def test_initialization_creates_directory(self, test_config):
        """Test that initialization creates persist directory."""
        store = ChromaVectorStore(config=test_config)
        await store.initialize()
        
        persist_dir = Path(test_config.persist_directory)
        assert persist_dir.exists()
        assert persist_dir.is_dir()
        
        await store.close()
        # Cleanup
        if persist_dir.exists():
            shutil.rmtree(persist_dir)
    
    @pytest.mark.asyncio
    async def test_upsert_vectors(self, vector_store):
        """Test upserting vectors."""
        ids = ["doc_1", "doc_2"]
        vectors = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
        metadatas = [{"source": "test1"}, {"source": "test2"}]
        documents = ["Document 1", "Document 2"]
        
        await vector_store.upsert(ids, vectors, metadatas, documents)
        
        # Verify last update time was set
        assert vector_store._last_update_time is not None
    
    @pytest.mark.asyncio
    async def test_upsert_without_initialization(self, test_config):
        """Test that upsert fails without initialization."""
        store = ChromaVectorStore(config=test_config)
        
        with pytest.raises(VectorStoreUnavailableError, match="not initialized"):
            await store.upsert(
                ["doc_1"],
                [[0.1, 0.2, 0.3]],
                [{"source": "test"}],
                ["Document"]
            )
    
    @pytest.mark.asyncio
    async def test_upsert_mismatched_lengths(self, vector_store):
        """Test that upsert fails with mismatched input lengths."""
        with pytest.raises(VectorStoreError, match="same non-zero length"):
            await vector_store.upsert(
                ["doc_1", "doc_2"],
                [[0.1, 0.2, 0.3]],  # Only one vector
                [{"source": "test"}],
                ["Document"]
            )
    
    @pytest.mark.asyncio
    async def test_upsert_empty_lists(self, vector_store):
        """Test that upsert fails with empty lists."""
        with pytest.raises(VectorStoreError, match="same non-zero length"):
            await vector_store.upsert([], [], [], [])
    
    @pytest.mark.asyncio
    async def test_query_vectors(self, vector_store):
        """Test querying for similar vectors."""
        # First upsert some vectors
        ids = ["doc_1", "doc_2", "doc_3"]
        vectors = [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0]
        ]
        metadatas = [
            {"category": "A"},
            {"category": "B"},
            {"category": "A"}
        ]
        documents = ["Doc 1", "Doc 2", "Doc 3"]
        
        await vector_store.upsert(ids, vectors, metadatas, documents)
        
        # Query with a vector similar to the first one
        query_vector = [0.9, 0.1, 0.0]
        results = await vector_store.query(query_vector, top_k=2)
        
        assert len(results) <= 2
        assert all(isinstance(r, VectorSearchResult) for r in results)
        if results:
            assert results[0].id in ids
            assert results[0].metadata in metadatas
    
    @pytest.mark.asyncio
    async def test_query_with_filter(self, vector_store):
        """Test querying with metadata filter."""
        # Upsert vectors with different metadata
        ids = ["doc_1", "doc_2", "doc_3"]
        vectors = [
            [1.0, 0.0, 0.0],
            [0.9, 0.1, 0.0],
            [0.0, 1.0, 0.0]
        ]
        metadatas = [
            {"category": "A"},
            {"category": "A"},
            {"category": "B"}
        ]
        documents = ["Doc 1", "Doc 2", "Doc 3"]
        
        await vector_store.upsert(ids, vectors, metadatas, documents)
        
        # Query with filter for category A
        query_vector = [1.0, 0.0, 0.0]
        results = await vector_store.query(
            query_vector,
            top_k=3,
            filter={"category": "A"}
        )
        
        # Should only return documents with category A
        assert all(r.metadata.get("category") == "A" for r in results)
    
    @pytest.mark.asyncio
    async def test_query_without_initialization(self, test_config):
        """Test that query fails without initialization."""
        store = ChromaVectorStore(config=test_config)
        
        with pytest.raises(VectorStoreUnavailableError, match="not initialized"):
            await store.query([0.1, 0.2, 0.3], top_k=5)
    
    @pytest.mark.asyncio
    async def test_delete_vectors(self, vector_store):
        """Test deleting vectors."""
        # First upsert some vectors
        ids = ["doc_1", "doc_2", "doc_3"]
        vectors = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]]
        metadatas = [{"source": "test"}] * 3
        documents = ["Doc 1", "Doc 2", "Doc 3"]
        
        await vector_store.upsert(ids, vectors, metadatas, documents)
        
        # Delete one vector
        await vector_store.delete(["doc_2"])
        
        # Query should not return deleted document
        results = await vector_store.query([0.4, 0.5, 0.6], top_k=3)
        result_ids = [r.id for r in results]
        assert "doc_2" not in result_ids
    
    @pytest.mark.asyncio
    async def test_delete_empty_list(self, vector_store):
        """Test that deleting empty list doesn't fail."""
        await vector_store.delete([])  # Should not raise
    
    @pytest.mark.asyncio
    async def test_delete_without_initialization(self, test_config):
        """Test that delete fails without initialization."""
        store = ChromaVectorStore(config=test_config)
        
        with pytest.raises(VectorStoreUnavailableError, match="not initialized"):
            await store.delete(["doc_1"])
    
    @pytest.mark.asyncio
    async def test_health_check_healthy(self, vector_store):
        """Test health check on healthy store."""
        # Upsert some data
        await vector_store.upsert(
            ["doc_1"],
            [[0.1, 0.2, 0.3]],
            [{"source": "test"}],
            ["Document"]
        )
        
        status = await vector_store.health_check()
        
        assert status.is_healthy is True
        assert status.connection_status == "connected"
        assert status.indexed_count >= 1
        assert status.last_update_time is not None
        assert status.error_message is None
    
    @pytest.mark.asyncio
    async def test_health_check_not_initialized(self, test_config):
        """Test health check on uninitialized store."""
        store = ChromaVectorStore(config=test_config)
        
        status = await store.health_check()
        
        assert status.is_healthy is False
        assert status.connection_status == "not_initialized"
        assert status.indexed_count == 0
        assert status.last_update_time is None
        assert "not initialized" in status.error_message
    
    @pytest.mark.asyncio
    async def test_backup(self, vector_store, test_config):
        """Test backing up vector store."""
        # Upsert some data
        await vector_store.upsert(
            ["doc_1"],
            [[0.1, 0.2, 0.3]],
            [{"source": "test"}],
            ["Document"]
        )
        
        backup_path = "./test_chroma_backup"
        await vector_store.backup(backup_path)
        
        # Verify backup exists
        backup_dir = Path(backup_path)
        assert backup_dir.exists()
        assert backup_dir.is_dir()
        
        # Cleanup
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
    
    @pytest.mark.asyncio
    async def test_backup_nonexistent_source(self, test_config):
        """Test backup fails with nonexistent source."""
        config = VectorStoreConfig(persist_directory="./nonexistent")
        store = ChromaVectorStore(config=config)
        
        with pytest.raises(VectorStoreError, match="does not exist"):
            await store.backup("./backup")
    
    @pytest.mark.asyncio
    async def test_close(self, vector_store):
        """Test closing vector store."""
        assert vector_store._is_initialized is True
        
        await vector_store.close()
        
        assert vector_store._is_initialized is False
        assert vector_store._client is None
        assert vector_store._collection is None
    
    @pytest.mark.asyncio
    async def test_persistence_across_restarts(self, test_config):
        """Test that data persists across store restarts.
        
        **Validates: Requirements 2.2**
        """
        # Create store and add data
        store1 = ChromaVectorStore(config=test_config, collection_name="persist_test")
        await store1.initialize()
        
        ids = ["doc_1", "doc_2"]
        vectors = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
        metadatas = [{"source": "test1"}, {"source": "test2"}]
        documents = ["Document 1", "Document 2"]
        
        await store1.upsert(ids, vectors, metadatas, documents)
        await store1.close()
        
        # Create new store instance with same config
        store2 = ChromaVectorStore(config=test_config, collection_name="persist_test")
        await store2.initialize()
        
        # Query should return the same data
        results = await store2.query([1.0, 0.0, 0.0], top_k=2)
        
        assert len(results) >= 1
        result_ids = [r.id for r in results]
        assert "doc_1" in result_ids or "doc_2" in result_ids
        
        await store2.close()
        
        # Cleanup
        test_dir = Path(test_config.persist_directory)
        if test_dir.exists():
            shutil.rmtree(test_dir)
    
    @pytest.mark.asyncio
    async def test_reconnection_mechanism(self, test_config):
        """Test reconnection with retries.
        
        **Validates: Requirements 2.5**
        """
        # Test that initialization retries on failure
        with patch('app.rag.vector_store.chromadb.Client') as mock_client:
            # First two attempts fail, third succeeds
            mock_client.side_effect = [
                Exception("Connection failed"),
                Exception("Connection failed"),
                MagicMock()
            ]
            
            store = ChromaVectorStore(config=test_config)
            
            # Should succeed after retries
            await store.initialize()
            
            # Verify retries happened
            assert mock_client.call_count == 3
            
            await store.close()
    
    @pytest.mark.asyncio
    async def test_timeout_control(self, test_config):
        """Test timeout control on operations.
        
        **Validates: Requirements 2.6**
        """
        store = ChromaVectorStore(config=test_config)
        await store.initialize()
        
        # Mock a slow operation
        with patch.object(store._collection, 'upsert', side_effect=lambda *args, **kwargs: asyncio.sleep(10)):
            with pytest.raises(VectorStoreTimeoutError, match="timed out"):
                await store.upsert(
                    ["doc_1"],
                    [[0.1, 0.2, 0.3]],
                    [{"source": "test"}],
                    ["Document"]
                )
        
        await store.close()
        
        # Cleanup
        test_dir = Path(test_config.persist_directory)
        if test_dir.exists():
            shutil.rmtree(test_dir)


class TestErrorHandling:
    """Tests for error handling."""
    
    @pytest.mark.asyncio
    async def test_initialization_failure_after_retries(self, test_config):
        """Test that initialization fails after max retries."""
        with patch('app.rag.vector_store.chromadb.Client', side_effect=Exception("Connection failed")):
            store = ChromaVectorStore(config=test_config)
            
            with pytest.raises(VectorStoreUnavailableError, match="Failed to initialize"):
                await store.initialize()
    
    @pytest.mark.asyncio
    async def test_chromadb_not_installed(self):
        """Test error when chromadb is not installed."""
        with patch('app.rag.vector_store.chromadb', None):
            with pytest.raises(VectorStoreError, match="chromadb is not installed"):
                ChromaVectorStore()
