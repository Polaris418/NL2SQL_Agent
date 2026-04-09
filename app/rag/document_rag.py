"""Document RAG support for assistant knowledge documents."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunk:
    """A parsed and indexed document chunk."""

    chunk_id: str
    document_id: str
    content: str
    metadata: Dict[str, Any]
    embedding: Optional[List[float]] = None


class DocumentRAG:
    """Document parsing, chunking, indexing, and retrieval."""

    def __init__(self, embedding_model, vector_store, chunk_size: int = 500, chunk_overlap: int = 50):
        self.embedding_model = embedding_model
        self.vector_store = vector_store
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.collection_name = "assistant_knowledge"

    def parse_markdown(self, content: str) -> str:
        """Convert markdown into plain searchable text."""

        content = re.sub(r"```[\s\S]*?```", "", content)
        content = re.sub(r"`[^`]+`", "", content)
        content = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", content)
        content = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", content)
        content = re.sub(r"^#+\s+", "", content, flags=re.MULTILINE)
        content = re.sub(r"\*\*([^*]+)\*\*", r"\1", content)
        content = re.sub(r"\*([^*]+)\*", r"\1", content)
        content = re.sub(r"__([^_]+)__", r"\1", content)
        content = re.sub(r"_([^_]+)_", r"\1", content)
        content = re.sub(r"^\s*[-*+]\s+", "", content, flags=re.MULTILINE)
        content = re.sub(r"^\s*\d+\.\s+", "", content, flags=re.MULTILINE)
        return content.strip()

    def parse_text(self, content: str) -> str:
        """Normalize plain text content."""

        return content.strip()

    def chunk_text(self, text: str) -> List[str]:
        """Split text into overlapping chunks."""

        chunks: List[str] = []
        start = 0
        text_length = len(text)

        while start < text_length:
            end = start + self.chunk_size
            if end < text_length:
                for delimiter in ["\n\n", "。", "！", "？", ".", "!", "?"]:
                    pos = text.rfind(delimiter, start, end)
                    if pos != -1:
                        end = pos + len(delimiter)
                        break

            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)

            start = end - self.chunk_overlap if end < text_length else text_length

        return chunks

    @staticmethod
    def generate_chunk_id(document_id: str, chunk_index: int) -> str:
        return f"{document_id}_chunk_{chunk_index}"

    async def index_document(
        self,
        document_id: str,
        content: str,
        file_type: str,
        metadata: Dict[str, Any],
    ) -> int:
        """Index one uploaded document into the vector store."""

        try:
            parsed_content = self.parse_markdown(content) if file_type == "markdown" else self.parse_text(content)
            chunks = self.chunk_text(parsed_content)
            if not chunks:
                logger.warning("Document %s has no chunks after parsing", document_id)
                return 0

            embeddings = await self.embedding_model.embed_batch(chunks)
            chunk_ids = [self.generate_chunk_id(document_id, index) for index in range(len(chunks))]
            chunk_metadata = [
                {
                    **metadata,
                    "document_id": document_id,
                    "chunk_index": index,
                    "chunk_count": len(chunks),
                    "file_type": file_type,
                }
                for index in range(len(chunks))
            ]

            await self.vector_store.upsert(
                ids=chunk_ids,
                vectors=embeddings,
                metadatas=chunk_metadata,
                documents=chunks,
            )

            logger.info("Indexed document %s with %s chunks", document_id, len(chunks))
            return len(chunks)
        except Exception as exc:
            logger.error("Failed to index document %s: %s", document_id, exc, exc_info=True)
            raise

    async def search(
        self,
        query: str,
        top_k: int = 5,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Search indexed document chunks."""

        try:
            query_embedding = await self.embedding_model.embed_text(query)
            results = await self.vector_store.query(
                query_vector=query_embedding,
                top_k=top_k,
                filter=filter_metadata,
            )
            return [
                {
                    "id": result.id,
                    "score": result.score,
                    "metadata": result.metadata,
                    "document": result.document,
                }
                for result in results
            ]
        except Exception as exc:
            logger.error("Failed to search documents: %s", exc, exc_info=True)
            return []

    async def delete_document(self, document_id: str) -> bool:
        """Delete all chunks for a document."""

        try:
            await self.vector_store.delete_by_metadata({"document_id": document_id})
            logger.info("Deleted document %s", document_id)
            return True
        except Exception as exc:
            logger.error("Failed to delete document %s: %s", document_id, exc, exc_info=True)
            return False

    async def get_collection_stats(self) -> Dict[str, Any]:
        """Return underlying vector collection stats."""

        try:
            return await self.vector_store.get_collection_stats()
        except Exception as exc:
            logger.error("Failed to get collection stats: %s", exc, exc_info=True)
            return {"count": 0, "dimension": 0}


class DocumentMetadataStore:
    """File-backed document metadata store."""

    def __init__(self, storage_path: str = "./data/documents"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.metadata_file = self.storage_path / "metadata.json"
        self.documents: Dict[str, Dict[str, Any]] = self._load_metadata()

    def _load_metadata(self) -> Dict[str, Dict[str, Any]]:
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r", encoding="utf-8") as file:
                    return json.load(file)
            except Exception as exc:
                logger.error("Failed to load metadata: %s", exc)
        return {}

    def _save_metadata(self) -> None:
        try:
            with open(self.metadata_file, "w", encoding="utf-8") as file:
                json.dump(self.documents, file, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.error("Failed to save metadata: %s", exc)

    def add_document(
        self,
        document_id: str,
        filename: str,
        file_type: str,
        file_size: int,
        chunk_count: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.documents[document_id] = {
            "document_id": document_id,
            "filename": filename,
            "file_type": file_type,
            "file_size": file_size,
            "chunk_count": chunk_count,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
            "metadata": metadata or {},
        }
        self._save_metadata()

    def get_document(self, document_id: str) -> Optional[Dict[str, Any]]:
        return self.documents.get(document_id)

    def list_documents(self) -> List[Dict[str, Any]]:
        return list(self.documents.values())

    def delete_document(self, document_id: str) -> bool:
        if document_id in self.documents:
            del self.documents[document_id]
            self._save_metadata()
            return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_documents": len(self.documents),
            "total_chunks": sum(doc.get("chunk_count", 0) for doc in self.documents.values()),
            "total_size": sum(doc.get("file_size", 0) for doc in self.documents.values()),
        }
