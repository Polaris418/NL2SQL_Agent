"""Unit tests for embedding module."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from app.rag.embedding import (
    EmbeddingConfig,
    EmbeddingError,
    EmbeddingTimeoutError,
    SentenceTransformerEmbedding,
)


@pytest.fixture
def mock_sentence_transformer():
    """Mock SentenceTransformer for testing."""
    with patch("app.rag.embedding.SentenceTransformer") as mock_st:
        mock_model = MagicMock()
        mock_model.get_sentence_embedding_dimension.return_value = 384
        mock_model.encode.return_value = MagicMock(tolist=lambda: [0.1] * 384)
        mock_st.return_value = mock_model
        yield mock_st


@pytest.fixture
def embedding_model(mock_sentence_transformer):
    """Create embedding model instance for testing."""
    config = EmbeddingConfig(
        model_name="test-model",
        provider="local",
        timeout=2.0,
        max_retries=3
    )
    return SentenceTransformerEmbedding(config)


class TestEmbeddingConfig:
    """Test EmbeddingConfig dataclass."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = EmbeddingConfig()
        assert config.model_name == "paraphrase-multilingual-MiniLM-L12-v2"
        assert config.provider == "local"
        assert config.timeout == 2.0
        assert config.max_retries == 3
        assert config.dimensions == 384
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = EmbeddingConfig(
            model_name="custom-model",
            provider="openai",
            timeout=5.0,
            max_retries=5,
            dimensions=1536
        )
        assert config.model_name == "custom-model"
        assert config.provider == "openai"
        assert config.timeout == 5.0
        assert config.max_retries == 5
        assert config.dimensions == 1536


class TestSentenceTransformerEmbedding:
    """Test SentenceTransformerEmbedding implementation."""
    
    def test_initialization_without_sentence_transformers(self):
        """Test initialization fails when sentence-transformers not installed."""
        with patch("app.rag.embedding.SentenceTransformer", None):
            with pytest.raises(EmbeddingError, match="sentence-transformers is not installed"):
                SentenceTransformerEmbedding()
    
    def test_initialization_with_default_config(self, mock_sentence_transformer):
        """Test initialization with default config."""
        model = SentenceTransformerEmbedding()
        assert model.config.model_name == "paraphrase-multilingual-MiniLM-L12-v2"
        assert model.config.timeout == 2.0
        assert model.config.max_retries == 3
    
    def test_initialization_with_custom_config(self, mock_sentence_transformer):
        """Test initialization with custom config."""
        config = EmbeddingConfig(model_name="custom-model", timeout=5.0)
        model = SentenceTransformerEmbedding(config)
        assert model.config.model_name == "custom-model"
        assert model.config.timeout == 5.0
    
    @pytest.mark.asyncio
    async def test_embed_text_success(self, embedding_model, mock_sentence_transformer):
        """Test successful text embedding."""
        result = await embedding_model.embed_text("test text")
        
        assert isinstance(result, list)
        assert len(result) == 384
        assert all(isinstance(x, float) for x in result)
        
        # Verify model was called
        mock_model = mock_sentence_transformer.return_value
        mock_model.encode.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_embed_text_timeout(self, embedding_model, mock_sentence_transformer):
        """Test embedding timeout with retries."""
        # Mock _embed_sync to simulate slow operation
        def slow_embed(text):
            import time
            time.sleep(10)  # Longer than timeout
            return [0.1] * 384
        
        with patch.object(embedding_model, '_embed_sync', side_effect=slow_embed):
            with pytest.raises(EmbeddingTimeoutError, match="Embedding timeout after 3 attempts"):
                await embedding_model.embed_text("test text")
    
    @pytest.mark.asyncio
    async def test_embed_text_error_with_retry(self, embedding_model, mock_sentence_transformer):
        """Test embedding error with retry mechanism."""
        mock_model = mock_sentence_transformer.return_value
        
        # First two calls fail, third succeeds
        call_count = 0
        def encode_with_failures(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("Temporary error")
            return MagicMock(tolist=lambda: [0.1] * 384)
        
        mock_model.encode.side_effect = encode_with_failures
        
        result = await embedding_model.embed_text("test text")
        assert len(result) == 384
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_embed_text_permanent_error(self, embedding_model, mock_sentence_transformer):
        """Test embedding with permanent error after all retries."""
        mock_model = mock_sentence_transformer.return_value
        mock_model.encode.side_effect = RuntimeError("Permanent error")
        
        with pytest.raises(EmbeddingError, match="Embedding failed after 3 attempts"):
            await embedding_model.embed_text("test text")
    
    @pytest.mark.asyncio
    async def test_embed_batch_success(self, embedding_model, mock_sentence_transformer):
        """Test successful batch embedding."""
        texts = ["text1", "text2", "text3"]
        
        mock_model = mock_sentence_transformer.return_value
        mock_model.encode.return_value = MagicMock(
            tolist=lambda: [[0.1] * 384, [0.2] * 384, [0.3] * 384]
        )
        
        result = await embedding_model.embed_batch(texts)
        
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(len(vec) == 384 for vec in result)
        
        mock_model.encode.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_embed_batch_empty_list(self, embedding_model):
        """Test batch embedding with empty list."""
        result = await embedding_model.embed_batch([])
        assert result == []
    
    @pytest.mark.asyncio
    async def test_embed_batch_timeout(self, embedding_model, mock_sentence_transformer):
        """Test batch embedding timeout."""
        texts = ["text1", "text2"]
        
        def slow_embed_batch(texts):
            import time
            time.sleep(10)  # Longer than timeout
            return [[0.1] * 384] * len(texts)
        
        with patch.object(embedding_model, '_embed_batch_sync', side_effect=slow_embed_batch):
            with pytest.raises(EmbeddingTimeoutError, match="Batch embedding timeout"):
                await embedding_model.embed_batch(texts)
    
    @pytest.mark.asyncio
    async def test_embed_batch_error(self, embedding_model, mock_sentence_transformer):
        """Test batch embedding with error."""
        texts = ["text1", "text2"]
        mock_model = mock_sentence_transformer.return_value
        mock_model.encode.side_effect = RuntimeError("Batch error")
        
        with pytest.raises(EmbeddingError, match="Batch embedding failed after 3 attempts"):
            await embedding_model.embed_batch(texts)
    
    def test_dimensions_property(self, embedding_model, mock_sentence_transformer):
        """Test dimensions property."""
        dimensions = embedding_model.dimensions
        assert dimensions == 384
        
        # Verify model was loaded
        mock_sentence_transformer.assert_called_once()
    
    def test_model_name_property(self, embedding_model):
        """Test model_name property."""
        assert embedding_model.model_name == "test-model"
    
    def test_lazy_loading(self, mock_sentence_transformer):
        """Test that model is loaded lazily on first use."""
        config = EmbeddingConfig(model_name="lazy-model")
        model = SentenceTransformerEmbedding(config)
        
        # Model should not be loaded yet
        mock_sentence_transformer.assert_not_called()
        
        # Access dimensions to trigger loading
        _ = model.dimensions
        
        # Now model should be loaded
        mock_sentence_transformer.assert_called_once_with("lazy-model")


class TestEmbeddingErrors:
    """Test error classes."""
    
    def test_embedding_error(self):
        """Test EmbeddingError exception."""
        error = EmbeddingError("test error")
        assert str(error) == "test error"
        assert isinstance(error, Exception)
    
    def test_embedding_timeout_error(self):
        """Test EmbeddingTimeoutError exception."""
        error = EmbeddingTimeoutError("timeout error")
        assert str(error) == "timeout error"
        assert isinstance(error, EmbeddingError)
        assert isinstance(error, Exception)
