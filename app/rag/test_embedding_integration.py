"""Integration tests for embedding module with real sentence-transformers.

These tests require sentence-transformers to be installed and will download
the model on first run. They verify the actual embedding functionality.
"""

import pytest

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

from app.rag.embedding import (
    EmbeddingConfig,
    SentenceTransformerEmbedding,
)


@pytest.mark.skipif(
    not SENTENCE_TRANSFORMERS_AVAILABLE,
    reason="sentence-transformers not installed"
)
class TestSentenceTransformerIntegration:
    """Integration tests with real sentence-transformers model."""
    
    @pytest.fixture
    def embedding_model(self):
        """Create real embedding model for integration testing."""
        config = EmbeddingConfig(
            model_name="paraphrase-multilingual-MiniLM-L12-v2",
            provider="local",
            timeout=10.0,  # Longer timeout for real model
            max_retries=3
        )
        return SentenceTransformerEmbedding(config)
    
    @pytest.mark.asyncio
    async def test_embed_text_real_model(self, embedding_model):
        """Test embedding with real model."""
        text = "This is a test sentence for embedding."
        result = await embedding_model.embed_text(text)
        
        # Verify result structure
        assert isinstance(result, list)
        assert len(result) == 384  # MiniLM-L12 produces 384-dim vectors
        assert all(isinstance(x, float) for x in result)
        
        # Verify vector is normalized (approximately)
        import math
        magnitude = math.sqrt(sum(x * x for x in result))
        assert 0.9 < magnitude < 1.1  # Should be close to 1.0
    
    @pytest.mark.asyncio
    async def test_embed_multilingual_text(self, embedding_model):
        """Test embedding with multilingual text (Chinese and English)."""
        texts = [
            "用户订单信息",  # Chinese
            "User order information",  # English
            "用户 user 订单 order",  # Mixed
        ]
        
        results = await embedding_model.embed_batch(texts)
        
        assert len(results) == 3
        assert all(len(vec) == 384 for vec in results)
        
        # Verify semantic similarity between Chinese and English
        def cosine_similarity(v1, v2):
            import math
            dot = sum(a * b for a, b in zip(v1, v2))
            mag1 = math.sqrt(sum(a * a for a in v1))
            mag2 = math.sqrt(sum(b * b for b in v2))
            return dot / (mag1 * mag2)
        
        # Chinese and English versions should have high similarity
        similarity = cosine_similarity(results[0], results[1])
        assert similarity > 0.5, f"Expected high similarity, got {similarity}"
    
    @pytest.mark.asyncio
    async def test_embed_batch_real_model(self, embedding_model):
        """Test batch embedding with real model."""
        texts = [
            "First sentence",
            "Second sentence",
            "Third sentence",
        ]
        
        results = await embedding_model.embed_batch(texts)
        
        assert len(results) == 3
        assert all(len(vec) == 384 for vec in results)
        assert all(isinstance(vec, list) for vec in results)
    
    @pytest.mark.asyncio
    async def test_semantic_similarity(self, embedding_model):
        """Test that semantically similar texts have high similarity."""
        similar_texts = [
            "The cat sits on the mat",
            "A cat is sitting on a mat",
        ]
        
        dissimilar_texts = [
            "The cat sits on the mat",
            "Database query optimization",
        ]
        
        # Embed similar texts
        similar_vecs = await embedding_model.embed_batch(similar_texts)
        
        # Embed dissimilar texts
        dissimilar_vecs = await embedding_model.embed_batch(dissimilar_texts)
        
        def cosine_similarity(v1, v2):
            import math
            dot = sum(a * b for a, b in zip(v1, v2))
            mag1 = math.sqrt(sum(a * a for a in v1))
            mag2 = math.sqrt(sum(b * b for b in v2))
            return dot / (mag1 * mag2)
        
        similar_score = cosine_similarity(similar_vecs[0], similar_vecs[1])
        dissimilar_score = cosine_similarity(dissimilar_vecs[0], dissimilar_vecs[1])
        
        # Similar texts should have higher similarity than dissimilar texts
        assert similar_score > dissimilar_score
        assert similar_score > 0.7, f"Expected high similarity for similar texts, got {similar_score}"
        assert dissimilar_score < 0.5, f"Expected low similarity for dissimilar texts, got {dissimilar_score}"
    
    def test_dimensions_property_real_model(self, embedding_model):
        """Test dimensions property with real model."""
        dimensions = embedding_model.dimensions
        assert dimensions == 384
    
    def test_model_name_property_real_model(self, embedding_model):
        """Test model_name property with real model."""
        assert embedding_model.model_name == "paraphrase-multilingual-MiniLM-L12-v2"
