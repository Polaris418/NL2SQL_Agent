"""AI 助手 API - 提供系统帮助和指导"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.core.dependencies import get_llm_client
from app.llm.client import LLMClient
from app.schemas.llm import LLMProvider, LLMTestResult

logger = logging.getLogger(__name__)

router = APIRouter(tags=["AI Assistant"])


class AssistantMessage(BaseModel):
    """助手消息"""
    role: str = Field(..., description="角色: user 或 assistant")
    content: str = Field(..., description="消息内容")


class AssistantChatRequest(BaseModel):
    """助手对话请求"""
    message: str = Field(..., description="用户消息", min_length=1, max_length=2000)
    history: list[AssistantMessage] = Field(default_factory=list, description="对话历史")


class AssistantChatResponse(BaseModel):
    """助手对话响应"""
    message: str = Field(..., description="助手回复")
    sources: list[str] = Field(default_factory=list, description="参考来源")


class KnowledgeItem(BaseModel):
    """知识库条目"""
    id: str = Field(..., description="知识 ID")
    title: str = Field(..., description="标题")
    content: str = Field(..., description="内容")
    category: str = Field(..., description="分类")
    updated_at: str = Field(..., description="更新时间")


class AssistantConfig(BaseModel):
    """助手配置"""
    enabled: bool = Field(default=True, description="是否启用")
    provider: str = Field(default="anthropic", description="LLM 提供商: anthropic, openai, custom")
    api_key: str = Field(default="", description="API Key")
    api_base: str = Field(default="", description="API Base URL (可选)")
    model: str = Field(default="claude-3-5-sonnet-20241022", description="使用的模型")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="温度参数")
    max_tokens: int = Field(default=1000, ge=100, le=4000, description="最大 token 数")
    system_prompt: str = Field(default="", description="系统提示词")


# 默认知识库
class AssistantConfigTestRequest(BaseModel):
    provider: LLMProvider = Field(default=LLMProvider.OPENAI, description="LLM provider")
    api_key: str = Field(default="", description="API Key")
    api_base: str = Field(default="", description="API base URL")
    model: str = Field(..., min_length=1, description="Model name")


def _provider_requires_api_key(provider: str) -> bool:
    return provider not in {"custom", "ollama"}


DEFAULT_KNOWLEDGE_BASE = {
    "overview": {
        "id": "overview",
        "title": "系统概览",
        "category": "基础",
        "content": """
# NL2SQL Agent 系统概览

这是一个智能的自然语言转 SQL 查询系统，可以帮助你用中文提问来查询数据库。

## 核心功能
1. **自然语言查询**: 用中文提问，系统自动生成 SQL
2. **智能检索**: RAG 技术自动找到相关的数据表
3. **结果可视化**: 自动推荐合适的图表类型
4. **查询历史**: 保存所有查询记录，支持查看和删除
5. **错误重试**: 自动分析错误并重试（最多 3 次）

## 支持的数据库
- MySQL
- PostgreSQL  
- SQLite
""",
        "updated_at": "2026-04-08"
    },
    "how-to-query": {
        "id": "how-to-query",
        "title": "如何查询",
        "category": "使用指南",
        "content": """
# 如何使用查询功能

## 基本步骤
1. 在主页面选择数据库连接
2. 在输入框输入你的问题（用中文）
3. 点击"查询"按钮或按 Enter
4. 等待系统生成 SQL 并执行
5. 查看结果和图表

## 提问技巧
- ✅ 好的提问: "查询最近7天的活跃用户数量"
- ✅ 好的提问: "统计各工具的使用次数，按次数排序"
- ❌ 避免: "给我看看数据"（太模糊）
- ❌ 避免: "SELECT * FROM users"（直接写 SQL）

## 高级功能
- **分页**: 结果自动分页，每页最多 1000 条
- **导出**: 可以复制 SQL 或结果数据
- **图表**: 系统自动推荐柱状图、折线图、饼图等
""",
        "updated_at": "2026-04-08"
    },
    "history": {
        "id": "history",
        "title": "查询历史",
        "category": "使用指南",
        "content": """
# 查询历史功能

## 查看历史
1. 点击左侧边栏的"查询历史"
2. 查看所有历史查询记录
3. 点击任意记录查看详情

## 删除历史
- **删除单条**: 点击记录右下角的删除按钮
- **删除多条**: 在详情页点击"删除此记录"
- **清空全部**: 点击页面右上角的"清空历史"按钮

## 历史记录包含
- 原始问题
- 生成的 SQL
- 查询结果
- 执行时间
- 错误信息（如果有）
- RAG 检索详情
""",
        "updated_at": "2026-04-08"
    },
    "connections": {
        "id": "connections",
        "title": "数据库连接",
        "category": "配置",
        "content": """
# 数据库连接管理

## 添加连接
1. 点击"连接管理"
2. 点击"添加连接"
3. 填写连接信息:
   - 名称: 连接的显示名称
   - 类型: MySQL / PostgreSQL / SQLite
   - 主机: 数据库服务器地址
   - 端口: 数据库端口
   - 用户名: 数据库用户
   - 密码: 数据库密码
   - 数据库名: 要连接的数据库

## 测试连接
添加后系统会自动测试连接，显示在线/离线状态。

## 切换连接
在主页面的连接下拉框中选择要使用的连接。
""",
        "updated_at": "2026-04-08"
    },
    "prompts": {
        "id": "prompts",
        "title": "Prompt 配置",
        "category": "高级",
        "content": """
# Prompt 配置

## 什么是 Prompt
Prompt 是给 AI 模型的指令，控制它如何生成 SQL、改写查询等。

## 可配置的 Prompt
1. **sql_generation**: SQL 生成提示词
2. **query_rewrite**: 查询改写提示词
3. **error_reflection**: 错误分析提示词
4. **chart_suggestion**: 图表推荐提示词
5. **result_summary**: 结果总结提示词

## 如何修改
1. 访问 /prompts 页面
2. 选择要修改的 Prompt
3. 编辑内容
4. 点击"保存"

⚠️ 注意: Prompt 修改不会持久化，重启后恢复默认值。
""",
        "updated_at": "2026-04-08"
    },
    "rag": {
        "id": "rag",
        "title": "RAG 检索系统",
        "category": "高级",
        "content": """
# RAG 检索系统

## 什么是 RAG
RAG (Retrieval-Augmented Generation) 是一种智能检索技术，可以从数据库的所有表中找到与你的问题最相关的表。

## 工作原理
1. **索引构建**: 系统自动为所有表建立索引
2. **查询改写**: 将你的问题转换为检索关键词
3. **混合检索**: 使用词法（BM25）+ 向量检索
4. **重排序**: 使用 AI 模型对结果重新排序
5. **关系推断**: 自动识别表之间的关系

## 检索详情
查询结果中可以看到:
- 词法检索数量
- 向量检索数量
- 最终选择的表
- 关系推断结果

## 索引管理
系统会自动维护索引，通常不需要手动操作。
""",
        "updated_at": "2026-04-08"
    },
    "troubleshooting": {
        "id": "troubleshooting",
        "title": "常见问题",
        "category": "帮助",
        "content": """
# 常见问题解答

## 查询失败怎么办？
1. 检查问题是否清晰明确
2. 确认数据库连接正常
3. 查看错误提示信息
4. 尝试换一种问法

## 找不到相关表？
- 确认数据库中确实有相关的表
- 尝试使用表名或字段名提问
- 检查 RAG 索引是否构建完成

## 生成的 SQL 不对？
- 系统会自动重试 3 次
- 可以在 Prompt 配置中调整生成策略
- 添加更多 Few-shot 示例

## 性能慢？
- 检查数据库查询性能
- 查看 RAG 检索延迟
- 考虑优化数据库索引

## 中文显示乱码？
- 确认数据库使用 UTF-8 编码
- MySQL 需要使用 utf8mb4 字符集
""",
        "updated_at": "2026-04-08"
    }
}


def load_assistant_config() -> AssistantConfig:
    """加载助手配置"""
    config_file = Path("./config/assistant_config.json")
    if config_file.exists():
        import json
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config_data = json.load(f)
                return AssistantConfig(**config_data)
        except Exception as e:
            logger.warning(f"Failed to load assistant config: {e}")
    
    # 返回默认配置
    return AssistantConfig(
        enabled=True,
        provider="anthropic",
        api_key="",
        api_base="",
        model="claude-3-5-sonnet-20241022",
        temperature=0.7,
        max_tokens=1000,
        system_prompt=""
    )


def save_assistant_config(config: AssistantConfig) -> None:
    """保存助手配置"""
    import json
    config_file = Path("./config/assistant_config.json")
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(config.model_dump(mode="json"), f, ensure_ascii=False, indent=2)


def load_knowledge_base() -> dict[str, KnowledgeItem]:
    """加载知识库"""
    # 从文件加载（如果存在）
    knowledge_file = Path("./config/assistant_knowledge.json")
    if knowledge_file.exists():
        import json
        try:
            with open(knowledge_file, "r", encoding="utf-8") as f:
                custom_kb = json.load(f)
                return {k: KnowledgeItem(**v) for k, v in custom_kb.items()}
        except Exception as e:
            logger.warning(f"Failed to load custom knowledge base: {e}")
    
    # 使用默认知识库
    return {k: KnowledgeItem(**v) for k, v in DEFAULT_KNOWLEDGE_BASE.items()}


def build_system_prompt(knowledge_base: dict[str, KnowledgeItem]) -> str:
    """构建系统提示词"""
    kb_text = "\n\n".join([
        f"## {item.title}\n{item.content}"
        for item in knowledge_base.values()
    ])
    
    return f"""你是 NL2SQL Agent 系统的智能助手，负责帮助用户了解和使用这个系统。

# 你的职责
1. 回答关于系统功能、使用方法的问题
2. 提供操作指导和最佳实践
3. 帮助用户解决常见问题
4. 用友好、专业的语气交流

# 系统知识库
{kb_text}

# 回答规则
1. 简洁明了，直接回答问题
2. 提供具体的操作步骤
3. 如果不确定，诚实告知
4. 使用友好的语气，避免过于技术化
5. 适当使用 emoji 让回答更生动
6. 如果问题超出系统范围，礼貌地说明

# 示例对话
用户: "如何查询数据？"
助手: "很简单！只需要 3 步：\n1. 选择数据库连接\n2. 在输入框输入你的问题（用中文）\n3. 点击查询按钮\n\n比如你可以问：'查询最近7天的活跃用户' 😊"

现在开始回答用户的问题吧！
"""


async def _prepare_assistant_chat(
    request: AssistantChatRequest,
) -> tuple[LLMClient, str, str, list[str]]:
    """Assemble the assistant prompt, history, and matched knowledge sources."""
    config = load_assistant_config()
    if not config.enabled:
        raise HTTPException(status_code=503, detail="AI 助手已禁用")

    from app.core.factory import get_container

    container = get_container()
    llm_client = container.llm_client

    knowledge_base = load_knowledge_base()
    system_prompt = config.system_prompt if config.system_prompt else build_system_prompt(knowledge_base)

    rag_context = ""
    rag_sources: list[str] = []

    if hasattr(container, "document_rag") and container.document_rag is not None:
        try:
            rag_results = await container.document_rag.search(query=request.message, top_k=3)
            if rag_results:
                rag_context = "\n\n".join(
                    [
                        f"[来源: {item.get('metadata', {}).get('title', item.get('metadata', {}).get('filename', 'Unknown'))}]\n{item.get('document', '')}"
                        for item in rag_results
                    ]
                )
                rag_sources = list(
                    dict.fromkeys(
                        [
                            item.get("metadata", {}).get(
                                "title",
                                item.get("metadata", {}).get("filename", "Unknown"),
                            )
                            for item in rag_results
                        ]
                    )
                )
                logger.info("RAG retrieved %s relevant chunks", len(rag_results))
        except Exception as exc:
            logger.warning("RAG search failed: %s", exc)

    if rag_context:
        system_prompt = f"""{system_prompt}

# 相关知识（从文档知识库检索）
{rag_context}

请优先使用上述检索到的知识回答用户问题。如果检索到的知识与问题相关，请引用这些知识。
"""

    conversation_text = ""
    for msg in request.history[-10:]:
        if msg.role == "user":
            conversation_text += f"用户: {msg.content}\n"
        else:
            conversation_text += f"助手: {msg.content}\n"
    conversation_text += f"用户: {request.message}\n助手: "

    message_lower = request.message.lower()
    matched_sources = [
        item.title
        for item in knowledge_base.values()
        if any(
            keyword in message_lower
            for keyword in [
                item.title.lower(),
                item.category.lower(),
                *item.content.lower().split()[:10],
            ]
        )
    ]
    return llm_client, system_prompt, conversation_text, list(dict.fromkeys(rag_sources + matched_sources))[:5]


def _sse_frame(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


@router.post("/chat", response_model=AssistantChatResponse)
async def chat_with_assistant(request: AssistantChatRequest) -> AssistantChatResponse:
    """与 AI 助手对话"""
    try:
        llm_client, system_prompt, conversation_text, sources = await _prepare_assistant_chat(request)
        response, _ = await llm_client.chat(system_prompt=system_prompt, user_prompt=conversation_text)
        return AssistantChatResponse(message=response, sources=sources)
    except Exception as e:
        logger.error(f"Assistant chat error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"助手服务错误: {str(e)}")


@router.post("/chat/stream")
async def chat_with_assistant_stream(request: AssistantChatRequest) -> StreamingResponse:
    """以 SSE 形式流式返回 AI 助手回复。"""

    async def event_stream():
        try:
            llm_client, system_prompt, conversation_text, sources = await _prepare_assistant_chat(request)
            full_message = ""
            async for chunk in llm_client.stream_chat(
                system_prompt=system_prompt,
                user_prompt=conversation_text,
            ):
                if not chunk:
                    continue
                full_message += chunk
                yield _sse_frame("chunk", {"delta": chunk})

            yield _sse_frame("done", {"message": full_message, "sources": sources})
        except HTTPException as exc:
            yield _sse_frame("error", {"message": exc.detail})
        except Exception as exc:
            logger.error("Assistant stream error: %s", exc, exc_info=True)
            yield _sse_frame("error", {"message": f"助手服务错误: {exc}"})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/knowledge", response_model=list[KnowledgeItem])
async def get_knowledge_base() -> list[KnowledgeItem]:
    """获取知识库列表"""
    knowledge_base = load_knowledge_base()
    return list(knowledge_base.values())


@router.get("/knowledge/{knowledge_id}", response_model=KnowledgeItem)
async def get_knowledge_item(knowledge_id: str) -> KnowledgeItem:
    """获取单个知识条目"""
    knowledge_base = load_knowledge_base()
    if knowledge_id not in knowledge_base:
        raise HTTPException(status_code=404, detail="知识条目不存在")
    return knowledge_base[knowledge_id]


@router.put("/knowledge/{knowledge_id}", response_model=KnowledgeItem)
async def update_knowledge_item(knowledge_id: str, item: KnowledgeItem) -> KnowledgeItem:
    """更新知识条目"""
    import json
    from datetime import datetime, timezone
    
    # 加载现有知识库
    knowledge_file = Path("./config/assistant_knowledge.json")
    knowledge_file.parent.mkdir(parents=True, exist_ok=True)
    
    if knowledge_file.exists():
        with open(knowledge_file, "r", encoding="utf-8") as f:
            knowledge_base = json.load(f)
    else:
        knowledge_base = {k: v for k, v in DEFAULT_KNOWLEDGE_BASE.items()}
    
    # 更新条目
    item.updated_at = datetime.now(timezone.utc).isoformat()
    knowledge_base[knowledge_id] = item.model_dump(mode="json")
    
    # 保存
    with open(knowledge_file, "w", encoding="utf-8") as f:
        json.dump(knowledge_base, f, ensure_ascii=False, indent=2)
    
    return item


@router.post("/knowledge", response_model=KnowledgeItem)
async def create_knowledge_item(item: KnowledgeItem) -> KnowledgeItem:
    """创建新的知识条目"""
    import json
    from datetime import datetime, timezone
    
    # 加载现有知识库
    knowledge_file = Path("./config/assistant_knowledge.json")
    knowledge_file.parent.mkdir(parents=True, exist_ok=True)
    
    if knowledge_file.exists():
        with open(knowledge_file, "r", encoding="utf-8") as f:
            knowledge_base = json.load(f)
    else:
        knowledge_base = {k: v for k, v in DEFAULT_KNOWLEDGE_BASE.items()}
    
    # 检查 ID 是否已存在
    if item.id in knowledge_base:
        raise HTTPException(status_code=400, detail="知识条目 ID 已存在")
    
    # 添加条目
    item.updated_at = datetime.now(timezone.utc).isoformat()
    knowledge_base[item.id] = item.model_dump(mode="json")
    
    # 保存
    with open(knowledge_file, "w", encoding="utf-8") as f:
        json.dump(knowledge_base, f, ensure_ascii=False, indent=2)
    
    return item


@router.delete("/knowledge/{knowledge_id}")
async def delete_knowledge_item(knowledge_id: str) -> dict[str, str]:
    """删除知识条目"""
    import json
    
    # 加载现有知识库
    knowledge_file = Path("./config/assistant_knowledge.json")
    if not knowledge_file.exists():
        raise HTTPException(status_code=404, detail="知识库文件不存在")
    
    with open(knowledge_file, "r", encoding="utf-8") as f:
        knowledge_base = json.load(f)
    
    # 删除条目
    if knowledge_id not in knowledge_base:
        raise HTTPException(status_code=404, detail="知识条目不存在")
    
    del knowledge_base[knowledge_id]
    
    # 保存
    with open(knowledge_file, "w", encoding="utf-8") as f:
        json.dump(knowledge_base, f, ensure_ascii=False, indent=2)
    
    return {"message": "知识条目已删除"}


@router.post("/config/test", response_model=LLMTestResult)
async def test_assistant_config(
    payload: AssistantConfigTestRequest,
    llm_client: LLMClient = Depends(get_llm_client),
) -> LLMTestResult:
    provider_value = getattr(payload.provider, "value", payload.provider)
    effective_api_key = payload.api_key.strip()

    if _provider_requires_api_key(provider_value) and not effective_api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": "API key is required for the selected provider"},
        )

    success, latency, message = await llm_client.test_connection(
        provider=provider_value,
        model=payload.model.strip(),
        api_key=effective_api_key or None,
        base_url=payload.api_base.strip() or None,
    )

    return LLMTestResult(
        success=success,
        provider=payload.provider,
        model=payload.model.strip(),
        latency_ms=round(latency, 2),
        message=message if success else f"Connection test failed: {message}",
    )


@router.get("/config", response_model=AssistantConfig)
async def get_assistant_config() -> AssistantConfig:
    """获取助手配置"""
    config = load_assistant_config()
    
    # 如果没有自定义 system_prompt，生成默认的
    if not config.system_prompt:
        knowledge_base = load_knowledge_base()
        config.system_prompt = build_system_prompt(knowledge_base)
    
    return config


@router.put("/config", response_model=AssistantConfig)
async def update_assistant_config(config: AssistantConfig) -> AssistantConfig:
    """更新助手配置"""
    try:
        save_assistant_config(config)
        return config
    except Exception as e:
        logger.error(f"Failed to save assistant config: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"保存配置失败: {str(e)}")
