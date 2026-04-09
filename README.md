# NL2SQL Agent

NL2SQL Agent 是一个面向数据分析场景的全栈 AI 系统。它将自然语言问题转换为 SQL，结合 Schema RAG、文档知识库、流式交互和可观测性能力，提供一套完整的智能查询与辅助问答体验。

## 功能概览

- 自然语言转 SQL，覆盖问题改写、Schema 检索、SQL 生成、执行、结果总结完整链路
- 支持 `MySQL`、`PostgreSQL`、`SQLite` 多数据库连接管理
- 内置 `Schema RAG`，在生成 SQL 前检索相关表、字段和关系线索
- 内置 `Document RAG`，支持上传 Markdown / TXT 文档作为知识库
- 提供悬浮 AI 助手，支持流式输出、拖拽、最小化
- 提供 Prompt 配置、查询历史、分析面板、RAG 状态管理、模型连接测试等后台能力

## 核心模块

### 对话查询

- 输入自然语言问题
- 自动问题改写
- 检索相关 Schema 上下文
- 生成 SQL 并执行
- 返回结果表格、结果总结和图表建议

### 连接管理

- 新增、删除、测试数据库连接
- 同步 Schema 缓存
- 维护连接在线状态

### Schema RAG

- 预构建数据库 Schema 索引
- 展示索引状态、健康状态、表数和向量数
- 为 SQL 生成提供检索增强上下文

### 文档知识库

- 上传 Markdown / TXT 文档
- 文档切块、向量化和检索
- 为 AI 助手问答提供知识支撑

### AI 助手

- 独立悬浮窗交互
- 流式回答
- 模型配置与连接测试
- 结合文档知识库进行辅助问答

### 系统配置与观测

- Prompt 模板配置
- 查询历史记录
- 分析面板
- RAG 遥测与状态页

## 界面预览

### 对话式 SQL 工作台

![对话式 SQL 工作台](image/image.png)

### SQL 生成与结果展示

![SQL 生成与结果展示](image.png)

### 数据库连接管理

![数据库连接管理](image-1.png)

### RAG 状态管理

![RAG 状态管理](image-3.png)

### 文档知识库

![文档知识库](image-2.png)

### 模型 API 配置与连接测试

![模型 API 配置与连接测试](image-4.png)

## 技术栈

### 前端

- React 18
- TypeScript
- Vite
- Tailwind CSS
- Zustand
- Axios

### 后端

- FastAPI
- Pydantic
- SQLAlchemy
- SQLite 元数据库
- Chroma 向量存储

### AI / 检索

- OpenAI 兼容模型接入
- 自定义模型提供商配置
- Schema RAG
- Document RAG
- Prompt 模板系统

## 项目结构

```text
.
├─ app/                                   # FastAPI 主应用
│  ├─ agent/                              # NL2SQL Agent 工作流
│  ├─ api/                                # HTTP API
│  ├─ core/                               # 配置、依赖注入、应用装配
│  ├─ db/                                 # 数据库连接器与元数据层
│  ├─ llm/                                # 模型客户端
│  ├─ prompts/                            # Prompt 模板
│  ├─ rag/                                # RAG、向量检索、遥测
│  └─ schemas/                            # 数据模型
├─ backend/                               # Dockerfile 与启动脚本
├─ config/                                # 跟踪的静态配置
├─ image/                                 # README 展示截图
├─ NL2SQL Agent Frontend Development/     # React 前端
├─ pyproject.toml                         # Python 依赖
├─ docker-compose.yml
└─ TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md    # 项目知识库文档
```

## 本地启动

### 启动后端

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
copy .env.example .env
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

后端默认地址：

```text
http://127.0.0.1:8000
```

### 启动前端

```bash
cd "NL2SQL Agent Frontend Development"
npm install
npm run dev
```

前端默认地址：

```text
http://127.0.0.1:5173
```

## 主要接口

### 查询与 Agent

- `POST /api/query`
- `POST /api/query/stream`
- `POST /api/query/sql`

### 数据库连接与 Schema

- `GET /api/connections`
- `POST /api/connections`
- `POST /api/connections/{connection_id}/test`
- `POST /api/connections/{connection_id}/sync`
- `GET /api/connections/{connection_id}/schema`

### RAG 与状态监控

- `GET /api/rag/index/status`
- `GET /api/rag/index/health/{connection_id}`
- `POST /api/rag/index/{connection_id}/rebuild`
- `GET /api/rag/telemetry/dashboard`

### AI 助手与知识库

- `POST /api/assistant/chat`
- `POST /api/assistant/chat/stream`
- `GET /api/assistant/config`
- `POST /api/assistant/config/test`
- `POST /api/documents/upload`
- `POST /api/documents/search`
- `GET /api/documents/stats`

## 知识库文档

仓库保留了一个完整 Markdown 文档，可直接用于项目知识库或外部文档 RAG：

- `TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md`
