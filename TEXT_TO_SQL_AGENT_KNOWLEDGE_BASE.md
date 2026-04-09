# Text-to-SQL Agent 项目完整知识库

## 1. 项目概览

Text-to-SQL Agent 是一个面向业务用户的自然语言查询系统。用户通过中文问题访问数据库，系统自动完成查询改写、Schema 检索、SQL 生成、执行、结果可视化建议、结果总结、查询历史记录与分析。

当前项目由两部分组成：

- 后端：FastAPI 服务，负责数据库连接管理、NL2SQL 代理链、RAG 索引与查询、AI 助手、文档知识库、历史与分析等能力
- 前端：React + Vite 单页应用，负责聊天式查询界面、连接管理、RAG 页面、历史页面、系统设置、AI 助手配置与知识库管理

项目定位不是通用 BI 平台，而是一个偏“Conversational BI / Schema-aware SQL Agent”的智能查询应用。

## 2. 核心能力

### 2.1 数据查询能力

- 支持 PostgreSQL、MySQL、SQLite 三种数据库
- 支持自然语言查询转 SQL
- 支持 Query Rewrite、Schema Retrieval、SQL Generation、SQL Execution 多阶段链路
- 支持 SQL 执行失败后的自动反思与重试，默认最多 3 次
- 支持 SQL 手工执行
- 支持分页查询与总数统计
- 支持 CSV 导出
- 支持 SSE 流式步骤输出

### 2.2 RAG 与 Schema 检索能力

- 对数据库 Schema 建立检索索引
- 支持混合检索
- 支持关系线索、列注释、上下文压缩
- 支持 Few-shot 示例注入
- 支持业务知识与租户隔离
- 支持 RAG 遥测、健康状态、索引状态与重建

### 2.3 AI 助手能力

- 提供独立于主查询链路的 AI 助手配置
- 支持 AI 助手独立配置模型提供商、API Key、Base URL、模型名、温度、最大 tokens、系统提示词
- 支持 AI 助手模型连接测试
- 提供悬浮球助手
- 悬浮球可拖动
- 展开的聊天框也可拖动
- 最小化后会回到悬浮球，而不是保留折叠条

### 2.4 文档知识库能力

- 支持上传 Markdown 和纯文本文件
- 文档知识库面向 AI 助手使用
- 支持文档索引、搜索、统计、删除
- 上传文件大小限制为 10MB

### 2.5 管理与可观测能力

- 查询历史管理
- 分析面板
- Prompt 配置页
- LLM 配置页
- AI 助手配置页
- 知识库管理页
- RAG 状态页
- 遥测与统计接口

## 3. 技术栈

### 3.1 后端

- Python 3.11+
- FastAPI
- Uvicorn
- Pydantic
- SQLAlchemy
- PyMySQL
- Psycopg
- OpenAI SDK
- Anthropic SDK
- ChromaDB
- rank-bm25
- sentence-transformers

### 3.2 前端

- React
- React Router
- Vite
- Axios
- Tailwind 风格组件体系
- Lucide React 图标
- Recharts

## 4. 系统架构

## 4.1 总体架构

```text
前端 React SPA
  -> FastAPI API
    -> DBManager / Connector
    -> NL2SQLAgent
      -> QueryRewriter
      -> SchemaRetriever
      -> SQLGenerator
      -> SQLExecutor
      -> ErrorReflector
      -> ResultSummarizer
      -> ChartSuggester
    -> MetadataDB(SQLite)
    -> RAGIndexManager
    -> AI Assistant
    -> Document RAG
```

### 4.2 服务容器

后端通过 `ServiceContainer` 统一管理核心依赖，包含：

- `settings`
- `metadata_db`
- `db_manager`
- `rag_index_manager`
- `llm_client`
- `agent`
- `document_rag`
- `document_metadata_store`

容器在 [factory.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/core/factory.py) 中构建，并通过 `lru_cache` 缓存为单例。

## 5. 核心后端模块

### 5.1 应用入口

- [main.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/main.py)
- [factory.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/core/factory.py)

`create_app()` 会注册以下核心路由：

- 健康检查
- 连接管理
- 查询
- 历史
- 分析
- RAG 遥测
- LLM 设置
- Prompt 管理
- AI 助手
- 文档知识库

### 5.2 NL2SQLAgent 主链路

主链路实现位于 [nl2sql_agent.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/agent/nl2sql_agent.py)。

处理流程如下：

1. 进入并发控制信号量
2. 获取数据库连接器与 Schema 缓存
3. 计算缓存 key 与查询审计信息
4. 命中查询缓存则直接返回
5. 调用 `QueryRewriter` 改写问题
6. 调用 `SchemaRetriever` 检索相关表与上下文
7. 调用 `SQLGenerator` 生成 SQL
8. 检查数据库/业务域不匹配
9. 调用 `SQLExecutor` 执行 SQL
10. 执行失败时调用 `ErrorReflector` 做反思与重试
11. 调用 `ChartSuggester` 生成图表建议
12. 调用 `ResultSummarizer` 生成结果总结
13. 保存查询历史
14. 成功结果写入缓存

### 5.3 数据连接管理

连接管理相关 API 位于 [connections.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/api/connections.py)。

支持：

- 创建连接
- 列出连接
- 删除连接
- 测试连接
- 同步 Schema
- 获取 Schema 缓存

连接创建或同步 Schema 后，会触发 RAG 索引重建调度。

### 5.4 查询 API

查询相关 API 位于 [query.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/api/query.py)。

支持：

- 普通查询：`POST /api/query`
- 流式查询：`POST /api/query/stream`
- 执行手工 SQL：`POST /api/query/sql`
- 导出自然语言查询结果：`POST /api/query/export`
- 导出 SQL 结果：`POST /api/query/sql/export`

流式查询通过 SSE 输出阶段步骤与最终结果。

### 5.5 AI 助手

AI 助手相关接口位于 [ai_assistant.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/api/ai_assistant.py)。

能力包括：

- 助手对话
- 助手配置读取与保存
- 助手配置连接测试
- 结构化知识条目读取、创建、更新、删除

AI 助手会：

- 加载本地结构化知识库
- 在可用时尝试使用文档 RAG 检索相关文档片段
- 使用独立的系统提示词回答系统使用相关问题

### 5.6 文档知识库

文档知识库接口位于 [documents.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/api/documents.py)。

支持：

- 上传文档
- 列出文档
- 获取统计
- 获取单个文档信息
- 删除文档
- 搜索文档

上传规则：

- 文件类型：`.md`、`.markdown`、`.txt`、`.text`
- 最大大小：10MB
- 文本解码优先 UTF-8，其次 GBK

### 5.7 LLM 设置

系统级 LLM 设置接口位于 [settings.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/api/settings.py)。

支持：

- 获取 LLM 设置
- 新增/更新 LLM profile
- 设置主路由和 fallback profile
- 删除 profile
- 测试 profile 连接

系统级 LLM 与 AI 助手配置是两套独立配置。

## 6. 核心前端页面

前端路由定义位于 [routes.tsx](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/NL2SQL%20Agent%20Frontend%20Development/src/app/routes.tsx)。

主要页面如下：

- `/` 或 `/chat`：聊天查询主页面
- `/analytics`：分析面板
- `/connections`：连接管理
- `/rag`：RAG 状态页
- `/settings`：设置首页
- `/settings/llm`：系统 LLM 配置
- `/history`：查询历史
- `/prompts`：Prompt 配置
- `/assistant-config`：AI 助手配置
- `/knowledge-base`：统一知识库管理

### 6.1 设置页

设置首页位于 [SettingsPage.tsx](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/NL2SQL%20Agent%20Frontend%20Development/src/app/components/settings/SettingsPage.tsx)。

设置页入口包括：

- API 配置
- Prompt 配置
- AI 助手配置
- 知识库管理

### 6.2 AI 助手悬浮窗

AI 助手悬浮组件位于 [AIAssistant.tsx](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/NL2SQL%20Agent%20Frontend%20Development/src/app/components/assistant/AIAssistant.tsx)。

当前交互特性：

- 收起状态显示为可拖动悬浮球
- 点击悬浮球展开聊天框
- 聊天框头部可拖动
- 最小化会还原为球
- 聊天消息支持来源显示

### 6.3 AI 助手配置页

配置页位于 [AssistantConfigPage.tsx](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/NL2SQL%20Agent%20Frontend%20Development/src/app/components/assistant/AssistantConfigPage.tsx)。

支持配置：

- 是否启用 AI 助手
- 模型提供商
- API Key
- Base URL
- 模型名称
- 温度
- 最大 tokens
- 系统提示词

支持功能：

- 保存配置
- 测试当前表单的模型连接
- 测试无需先保存
- Base URL 兼容填写基础地址和完整 `chat/completions` 端点

### 6.4 知识库管理页

统一知识库页面位于：

- [UnifiedKnowledgeBasePage.tsx](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/NL2SQL%20Agent%20Frontend%20Development/src/app/components/assistant/UnifiedKnowledgeBasePage.tsx)

它管理两类知识：

- 结构化知识条目
- 文档知识库

## 7. API 清单

### 7.1 健康检查

- `GET /health`

### 7.2 连接管理

- `POST /api/connections`
- `GET /api/connections`
- `DELETE /api/connections/{connection_id}`
- `POST /api/connections/{connection_id}/sync`
- `POST /api/connections/{connection_id}/test`
- `GET /api/connections/{connection_id}/schema`

### 7.3 查询

- `POST /api/query`
- `POST /api/query/stream`
- `POST /api/query/sql`
- `POST /api/query/export`
- `POST /api/query/sql/export`

### 7.4 历史与分析

- `GET /api/history`
- `GET /api/history/{id}`
- `GET /api/history/{id}/context`
- `DELETE /api/history/{id}`
- `DELETE /api/history`
- `GET /api/analytics/summary`
- `GET /api/analytics/errors`
- `GET /api/analytics/top-tables`
- `GET /api/analytics/report`

### 7.5 RAG

- `GET /api/rag/index/status`
- `GET /api/rag/index/status/{connection_id}`
- `GET /api/rag/index/health/{connection_id}`
- `POST /api/rag/index/{connection_id}/rebuild`
- `GET /api/rag/telemetry/dashboard`

### 7.6 系统 LLM 设置

- `GET /api/settings/llm`
- `POST /api/settings/llm/profiles`
- `PUT /api/settings/llm/routing`
- `DELETE /api/settings/llm/profiles/{profile_id}`
- `POST /api/settings/llm/test`

### 7.7 Prompt

- `GET /api/prompts`
- `GET /api/prompts/{name}`
- `PUT /api/prompts/{name}`
- `POST /api/prompts/{name}/reset`

### 7.8 AI 助手

- `POST /api/assistant/chat`
- `GET /api/assistant/config`
- `PUT /api/assistant/config`
- `POST /api/assistant/config/test`
- `GET /api/assistant/knowledge`
- `GET /api/assistant/knowledge/{knowledge_id}`
- `PUT /api/assistant/knowledge/{knowledge_id}`
- `POST /api/assistant/knowledge`
- `DELETE /api/assistant/knowledge/{knowledge_id}`

### 7.9 文档知识库

- `POST /api/documents/upload`
- `GET /api/documents/list`
- `GET /api/documents/stats`
- `GET /api/documents/{document_id}`
- `DELETE /api/documents/{document_id}`
- `POST /api/documents/search`

## 8. 目录结构说明

```text
Text-to-SQL Agent/
├─ app/
│  ├─ agent/                # NL2SQL 代理链
│  ├─ api/                  # FastAPI 路由
│  ├─ core/                 # 应用工厂、配置、依赖、RAG 管理器
│  ├─ db/                   # 连接器、元数据库、仓储
│  ├─ llm/                  # LLM 客户端
│  ├─ prompts/              # Prompt 模板与 few-shot
│  ├─ rag/                  # RAG、向量存储、嵌入、遥测
│  ├─ schemas/              # Pydantic Schema
│  └─ main.py               # 启动入口
├─ backend/
│  ├─ scripts/              # 启动与示例脚本
│  └─ tests/fixtures/       # 示例数据库
├─ config/                  # AI 助手配置、同义词等
├─ data/                    # 文档元数据等
├─ docs/                    # 文档
├─ tests/                   # 后端测试
├─ chroma/                  # 主 RAG 向量目录
├─ chroma_documents/        # 文档知识库向量目录
├─ metadata.sqlite3         # 元数据库
└─ NL2SQL Agent Frontend Development/
   └─ src/app/              # 前端应用代码
```

## 9. 关键配置项

示例环境变量见 [.env.example](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/.env.example)。

核心配置包括：

- `NL2SQL_MASTER_KEY`
- `NL2SQL_METADATA_DB`
- `APP_HOST`
- `APP_PORT`
- `API_PREFIX`
- `DEBUG`
- `LOG_LEVEL`
- `CORS_ORIGINS`
- `DEFAULT_QUERY_LIMIT`
- `MAX_CONCURRENT_QUERIES`
- `QUERY_TIMEOUT_SECONDS`
- `LLM_PROVIDER`
- `LLM_MODEL`
- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `CHROMA_PERSIST_DIRECTORY`
- `POSTGRESQL_DRIVER`
- `MYSQL_DRIVER`
- `SQLITE_DRIVER`

## 10. 数据与文件落盘位置

- 元数据库：`./metadata.sqlite3`
- 主 Schema/RAG 向量目录：`./chroma`
- 文档知识库向量目录：`./chroma_documents`
- 文档元数据目录：`./data/documents`
- AI 助手配置文件：`./config/assistant_config.json`
- 结构化知识库文件：`./config/assistant_knowledge.json`
- 遥测文件：`./rag_telemetry.jsonl`
- RAG 版本文件：`./rag_schema_versions.json`

## 11. 启动与运行方式

### 11.1 本地启动后端

```bash
uvicorn app.main:app --reload
```

Windows 项目内脚本：

- [start_backend_win.bat](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/backend/scripts/start_backend_win.bat)

### 11.2 启动前端

前端目录：

- [NL2SQL Agent Frontend Development](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/NL2SQL%20Agent%20Frontend%20Development)

开发命令：

```bash
npm run dev
```

构建命令：

```bash
npm run build
```

Windows 项目内脚本：

- [start_frontend_spa_win.bat](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/backend/scripts/start_frontend_spa_win.bat)

### 11.3 Docker

可通过 [docker-compose.yml](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/docker-compose.yml) 启动后端容器。

暴露端口：

- `8000:8000`

挂载内容：

- `metadata.sqlite3`
- `backend/tests/fixtures`

## 12. 测试覆盖

测试位于 [tests](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/tests)。

当前测试主题包含：

- 代理链
- API 契约
- 连接管理
- SQL 执行
- 历史与模型 roundtrip
- Prompt packing 回归
- RAG 接受性测试
- RAG 并发、超时、稳定性、降级、索引、关系检索、同义词、遥测、多租户等

说明：

- 该项目的 RAG 相关测试覆盖较重
- 相比之下，README 中曾说明项目在早期阶段测试没有完全展开，但目前测试目录已经包含大量 RAG 测试文件

## 13. 脚本与辅助工具

后端脚本位于 [backend/scripts](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/backend/scripts)：

- `create_sample_db.py`：创建示例数据库
- `seed_polaris_demo_data.py`：填充演示数据
- `start_backend_win.bat`：Windows 启动后端
- `start_frontend_spa_win.bat`：Windows 启动前端

## 14. 当前已知实现细节与注意事项

### 14.1 AI 助手与系统 LLM 是两套配置

- 系统 LLM 用于 NL2SQL 主链路
- AI 助手配置页用于悬浮球助手
- 两者不要混用

### 14.2 AI 助手模型测试已支持 OpenAI 兼容完整端点

例如以下写法都能处理：

- `https://api.openai.com/v1`
- `https://open.bigmodel.cn/api/paas/v4`
- `https://open.bigmodel.cn/api/paas/v4/chat/completions`

### 14.3 文档知识库初始化存在潜在问题

在 [factory.py](C:/Users/Administrator/Desktop/Text-to-SQL%20Agent/app/core/factory.py) 中，文档 RAG 初始化会尝试导入：

- `DeterministicHashEmbedding`

如果该类不存在或导入失败，日志会告警，文档 RAG 可能不可用。实际运行中曾出现过：

- `Failed to initialize document RAG system: cannot import name 'DeterministicHashEmbedding'`

如果要稳定使用文档知识库，需要重点验证这部分初始化链路。

### 14.4 AI 助手配置文件可能被脏数据污染

`config/assistant_config.json` 直接落盘保存。如果用户把错误信息误贴到 API Key 输入框，文件会原样保存，导致后续连接测试异常，例如：

- `431 Request Header Fields Too Large`

### 14.5 前端打包体积偏大

当前构建可成功，但 Vite 会提示某些 chunk 超过 500kB，需要后续考虑拆包优化。

## 15. 推荐上传到文档知识库时的元信息

建议在文档上传页填写：

- 标题：`Text-to-SQL Agent 完整项目知识库`
- 分类：`基础` 或 `系统`
- 描述：`包含项目概览、架构、页面、接口、配置、运行方式、数据落盘位置、测试覆盖与已知事项`

## 16. 推荐问答示例

以下问题适合 AI 助手基于本知识库回答：

- 这个项目支持哪些数据库？
- NL2SQL 查询链路是怎么工作的？
- AI 助手配置和系统 LLM 配置有什么区别？
- 文档知识库支持上传什么文件？
- 如何启动后端和前端？
- RAG 状态页和知识库管理页分别做什么？
- 查询接口和流式接口有什么区别？
- 数据都保存在哪些文件和目录里？
- 项目里有哪些已知注意事项？

## 17. 结论

Text-to-SQL Agent 已经具备一个完整的“数据库连接管理 + NL2SQL 代理链 + RAG + 历史分析 + AI 助手 + 文档知识库”的应用雏形，前后端都已搭建完成，接口与页面较为齐全。项目当前最有价值的部分在于：

- 面向真实数据库的自然语言查询链路
- 与 Schema 紧密耦合的 RAG 检索与上下文压缩
- AI 助手与知识库的集成
- 面向业务场景的管理页面和辅助能力

如果作为知识库文档上传，本文档可以作为项目的总索引和统一说明入口。
