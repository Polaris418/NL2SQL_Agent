"""Document upload and knowledge base APIs."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Documents"])


class DocumentInfo(BaseModel):
    """Stored document metadata."""

    document_id: str = Field(..., description="Document ID")
    filename: str = Field(..., description="Filename")
    file_type: str = Field(..., description="File type")
    file_size: int = Field(..., description="File size in bytes")
    chunk_count: int = Field(..., description="Indexed chunk count")
    uploaded_at: str = Field(..., description="Upload timestamp")
    metadata: dict = Field(default_factory=dict, description="Custom metadata")


class DocumentUploadResponse(BaseModel):
    """Upload response payload."""

    document_id: str = Field(..., description="Document ID")
    filename: str = Field(..., description="Filename")
    chunk_count: int = Field(..., description="Indexed chunk count")
    message: str = Field(..., description="Status message")


class DocumentSearchRequest(BaseModel):
    """Document search request."""

    query: str = Field(..., description="Search query", min_length=1, max_length=500)
    top_k: int = Field(default=5, ge=1, le=20, description="Result count")


class DocumentSearchResult(BaseModel):
    """Single document search match."""

    chunk_id: str = Field(..., description="Chunk ID")
    document_id: str = Field(..., description="Document ID")
    filename: str = Field(..., description="Filename")
    content: str = Field(..., description="Chunk content")
    score: float = Field(..., description="Similarity score")
    metadata: dict = Field(default_factory=dict, description="Chunk metadata")


class DocumentSearchResponse(BaseModel):
    """Document search response."""

    query: str = Field(..., description="Search query")
    results: List[DocumentSearchResult] = Field(..., description="Search results")
    total: int = Field(..., description="Result count")


class CollectionStats(BaseModel):
    """Knowledge base collection stats."""

    total_documents: int = Field(..., description="Total documents")
    total_chunks: int = Field(..., description="Total chunks")
    total_size: int = Field(..., description="Total size in bytes")
    vector_count: int = Field(..., description="Stored vectors")
    vector_dimension: int = Field(..., description="Vector dimension")


def generate_document_id(filename: str, content: bytes) -> str:
    """Generate a stable document ID from filename and size."""

    hash_input = f"{filename}_{len(content)}".encode("utf-8")
    return hashlib.sha256(hash_input).hexdigest()[:16]


def detect_file_type(filename: str) -> str:
    """Infer supported file type from extension."""

    suffix = Path(filename).suffix.lower()
    if suffix in [".md", ".markdown"]:
        return "markdown"
    if suffix in [".txt", ".text"]:
        return "text"
    return "text"


def _get_document_services():
    """Return initialized document RAG services or raise 503."""

    from app.core.factory import get_container

    container = get_container()
    document_rag = container.document_rag
    metadata_store = container.document_metadata_store

    if not document_rag or not metadata_store:
        raise HTTPException(status_code=503, detail="文档 RAG 系统未初始化")

    return document_rag, metadata_store


@router.post("/upload", response_model=DocumentUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    title: Optional[str] = Form(None),
    category: Optional[str] = Form("基础"),
    description: Optional[str] = Form(None),
) -> DocumentUploadResponse:
    """Upload a text or markdown document into the assistant knowledge base."""

    try:
        document_rag, metadata_store = _get_document_services()

        content = await file.read()

        max_size = 10 * 1024 * 1024
        if len(content) > max_size:
            raise HTTPException(
                status_code=400,
                detail=f"文件过大，最大支持 {max_size / 1024 / 1024:.0f}MB",
            )

        filename = file.filename or "document.txt"
        file_type = detect_file_type(filename)
        document_id = generate_document_id(filename, content)

        existing = metadata_store.get_document(document_id)
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"文档已存在: {existing['filename']}",
            )

        try:
            text_content = content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                text_content = content.decode("gbk")
            except UnicodeDecodeError as exc:
                raise HTTPException(
                    status_code=400,
                    detail="无法解码文件，请确保文件是 UTF-8 或 GBK 编码",
                ) from exc

        metadata = {
            "filename": filename,
            "title": title or filename,
            "category": category or "基础",
            "description": description or "",
            "file_type": file_type,
        }

        chunk_count = await document_rag.index_document(
            document_id=document_id,
            content=text_content,
            file_type=file_type,
            metadata=metadata,
        )

        metadata_store.add_document(
            document_id=document_id,
            filename=filename,
            file_type=file_type,
            file_size=len(content),
            chunk_count=chunk_count,
            metadata=metadata,
        )

        return DocumentUploadResponse(
            document_id=document_id,
            filename=filename,
            chunk_count=chunk_count,
            message="文档上传成功",
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to upload document: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"上传失败: {exc}") from exc


@router.get("/list", response_model=List[DocumentInfo])
async def list_documents() -> List[DocumentInfo]:
    """Return all stored documents."""

    try:
        from app.core.factory import get_container

        container = get_container()
        if not container.document_metadata_store:
            return []

        documents = container.document_metadata_store.list_documents()
        return [DocumentInfo(**doc) for doc in documents]
    except Exception as exc:
        logger.error("Failed to list documents: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取文档列表失败: {exc}") from exc


@router.get("/stats", response_model=CollectionStats)
async def get_collection_stats() -> CollectionStats:
    """Return aggregate document collection stats."""

    try:
        from app.core.factory import get_container

        container = get_container()
        if not container.document_rag or not container.document_metadata_store:
            return CollectionStats(
                total_documents=0,
                total_chunks=0,
                total_size=0,
                vector_count=0,
                vector_dimension=0,
            )

        metadata_stats = container.document_metadata_store.get_stats()
        vector_stats = await container.document_rag.get_collection_stats()

        return CollectionStats(
            total_documents=metadata_stats["total_documents"],
            total_chunks=metadata_stats["total_chunks"],
            total_size=metadata_stats["total_size"],
            vector_count=vector_stats.get("count", 0),
            vector_dimension=vector_stats.get("dimension", 0),
        )
    except Exception as exc:
        logger.error("Failed to get stats: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取统计信息失败: {exc}") from exc


@router.get("/{document_id}", response_model=DocumentInfo)
async def get_document(document_id: str) -> DocumentInfo:
    """Return one stored document metadata record."""

    try:
        _, metadata_store = _get_document_services()
        document = metadata_store.get_document(document_id)
        if not document:
            raise HTTPException(status_code=404, detail="文档不存在")

        return DocumentInfo(**document)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get document: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取文档失败: {exc}") from exc


@router.delete("/{document_id}")
async def delete_document(document_id: str) -> dict:
    """Delete a document from vector store and metadata store."""

    try:
        document_rag, metadata_store = _get_document_services()
        document = metadata_store.get_document(document_id)
        if not document:
            raise HTTPException(status_code=404, detail="文档不存在")

        await document_rag.delete_document(document_id)
        metadata_store.delete_document(document_id)

        return {"message": "文档已删除", "document_id": document_id}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to delete document: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"删除文档失败: {exc}") from exc


@router.post("/search", response_model=DocumentSearchResponse)
async def search_documents(request: DocumentSearchRequest) -> DocumentSearchResponse:
    """Search uploaded assistant documents."""

    try:
        document_rag, _ = _get_document_services()
        results = await document_rag.search(query=request.query, top_k=request.top_k)

        search_results = [
            DocumentSearchResult(
                chunk_id=result["id"],
                document_id=result["metadata"].get("document_id", ""),
                filename=result["metadata"].get("filename", ""),
                content=result["document"],
                score=result["score"],
                metadata=result["metadata"],
            )
            for result in results
        ]

        return DocumentSearchResponse(
            query=request.query,
            results=search_results,
            total=len(search_results),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to search documents: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"搜索失败: {exc}") from exc
