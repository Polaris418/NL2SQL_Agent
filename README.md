# NL2SQL Agent

一个面向真实业务场景的 AI Agent 项目：把自然语言问题转成 SQL，完成数据库查询、结果解释、可视化建议、检索增强和文档问答，而不是只做一个“问一句返回一段 SQL”的玩具 Demo。

这个项目的目标很明确：

- 展示我对 `LLM 应用工程`、`AI Agent 工作流`、`RAG`、`全栈产品化` 的完整实现能力
- 用一个可运行、可部署、可演示的系统，覆盖 AI Agent 开发岗面试里最常被追问的关键能力点

## 项目定位

NL2SQL Agent 是一个“面向数据分析场景的 AI 助手系统”。

用户可以：

- 配置数据库连接
- 通过自然语言发起查询
- 让系统自动检索相关表结构与字段
- 生成并执行 SQL
- 查看结果、图表建议和查询历史
- 通过文档知识库和悬浮 AI 助手获取系统使用帮助

它不是单点能力演示，而是一套完整的 AI Agent 应用。

## 这个项目为什么适合面试 AI Agent 开发岗

面试 AI Agent / LLM Application Engineer 岗位时，面试官通常不会只看“会不会调接口”，而会看你是否具备下面这些能力。这个项目对应关系很直接。

### 1. Agent 工作流设计能力

这个系统实现了完整的 NL2SQL Agent 链路，而不是一次性 prompt 调用：

1. 用户问题输入
2. 问题改写与意图整理
3. Schema RAG 检索相关表、字段、关系线索
4. 组装上下文与 Prompt
5. 生成 SQL
6. 执行 SQL
7. 返回结果、步骤信息、图表建议
8. 持久化历史、指标与调试信息

这能体现：

- 多阶段推理链路设计
- 检索与生成解耦
- Agent 状态落盘
- 出错后的回退与可观测性设计

### 2. RAG 系统工程能力

项目包含两类检索增强：

- `Schema RAG`：面向数据库表结构检索，为 SQL 生成提供上下文
- `Document RAG`：面向 Markdown/TXT 文档，为 AI 助手提供知识库问答能力

这能体现：

- 向量检索与词法检索混合召回
- 数据库结构信息建索引
- 文档分块、向量化、检索、命中返回
- 知识库与业务 Agent 的组合式设计

### 3. LLM 接入与多提供商配置能力

系统支持独立的 AI 助手模型配置，包含：

- 提供商切换
- 自定义 Base URL
- 模型名配置
- API Key 管理
- 模型连接测试

这能体现：

- OpenAI 兼容协议适配
- Anthropic / Custom Provider 接入思路
- 配置与运行时隔离
- 面向产品的模型切换能力

### 4. 流式输出与交互体验能力

项目里有两类流式交互：

- 查询链路的流式输出
- 悬浮 AI 助手的流式输出

同时做了：

- 悬浮窗展开/最小化
- 拖拽交互
- 流式消息渲染
- 独立 AI 助手配置页

这能体现：

- 面向用户体验的 LLM 前端实现能力
- SSE / 流式响应处理
- 状态同步和交互细节处理能力

### 5. 后端 API 和工程化能力

后端不是简单脚本，而是可运行 API 服务，覆盖：

- 连接管理
- 查询执行
- 历史记录
- 分析统计
- RAG 索引状态
- 文档知识库
- AI 助手配置与测试

这能体现：

- FastAPI 服务拆分
- 模块化目录设计
- 数据模型与接口模型分离
- 可部署的后端工程结构

### 6. 产品化与可演示能力

这个项目有明确的产品页面，而不是命令行实验：

- 对话查询页
- 连接管理页
- RAG 增强页
- 分析面板
- 查询历史页
- AI 助手配置页
- 知识库管理页

这能体现：

- 把 AI 能力做成真正可用产品
- 面向招聘方可直接在线演示
- 能讲清楚“从用户价值到系统设计”的完整闭环

## 核心功能清单

### 自然语言转 SQL

- 输入中文或英文问题
- 自动做查询意图改写
- 结合 Schema RAG 检索相关表
- 生成 SQL 并执行
- 返回结构化结果
- 输出图表建议

### 数据库连接管理

- 支持 `MySQL`、`PostgreSQL`、`SQLite`
- 新增、删除、测试连接
- 同步 Schema 缓存
- 维护在线状态

### Schema RAG

- 根据表结构构建检索索引
- 查询前召回相关表和字段
- 展示索引状态、健康状态和基础指标
- 支持重建索引

### 文档知识库

- 上传 Markdown / TXT 文档
- 文档切块与向量化
- 文档检索
- 供 AI 助手在问答时引用

### 悬浮 AI 助手

- 页面右下角悬浮入口
- 展开后可聊天
- 支持流式输出
- 支持拖动
- 支持最小化回悬浮球

### AI 助手模型配置

- 独立于系统主 LLM 配置
- 支持自定义模型提供商
- 支持 Base URL / Key / Model / Temperature / Max Tokens
- 支持“测试模型连接”

### 查询历史与分析

- 保存查询历史
- 查看查询步骤
- 统计查询表现
- 展示分析指标

## 系统架构

### 前端

- `React 18`
- `TypeScript`
- `Vite`
- `Tailwind CSS`
- `Zustand`
- `Axios`

职责：

- 展示业务页面
- 管理前端状态
- 调用后端 API
- 处理流式输出
- 提供 AI 助手与配置交互

### 后端

- `FastAPI`
- `Pydantic`
- `SQLAlchemy`
- `SQLite 元数据库`
- `Chroma 向量库`

职责：

- 连接数据库
- 执行 NL2SQL Agent 链路
- 管理 RAG 检索
- 处理历史记录和统计
- 提供文档知识库 API
- 提供 AI 助手聊天与配置 API

### LLM / 检索层

- OpenAI 兼容模型接入
- Anthropic 兼容接入
- 本地 / 兼容嵌入模型
- Chroma 文档向量检索
- Schema 检索与 Prompt 组装

## 仓库结构

```text
.
├─ app/                                   # FastAPI 主应用
│  ├─ agent/                              # NL2SQL Agent 工作流
│  ├─ api/                                # HTTP API
│  ├─ core/                               # 应用装配、依赖注入、配置
│  ├─ db/                                 # 连接器、仓储、元数据
│  ├─ llm/                                # 模型客户端
│  ├─ prompts/                            # Prompt 模板
│  ├─ rag/                                # RAG、向量检索、遥测
│  └─ schemas/                            # Pydantic 数据模型
├─ backend/                               # Dockerfile 和启动脚本
├─ config/                                # 跟踪的静态配置文件
├─ NL2SQL Agent Frontend Development/     # React 前端
├─ pyproject.toml                         # Python 依赖
├─ docker-compose.yml                     # 本地编排
└─ TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md    # 项目完整知识库文档
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

### RAG 与可观测性

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

## 面试时建议重点演示的功能

如果我用这个项目面试 AI Agent 开发岗，我会按下面顺序演示：

### 1. 连接一个数据库并同步 Schema

说明系统不是纯模型演示，而是真实数据接入能力。

### 2. 用自然语言发起一次查询

展示从用户问题到 SQL 生成、执行结果返回的完整链路。

### 3. 展示 Schema RAG

说明系统不是“盲生 SQL”，而是会先检索相关表结构和字段上下文，再生成 SQL。

### 4. 展示查询历史和分析指标

说明系统具备可观测性和产品化思路，便于排查与复盘。

### 5. 展示 AI 助手配置与模型连接测试

说明系统具备多模型接入、可配置化和运行时校验能力。

### 6. 展示文档知识库 + 悬浮 AI 助手

说明项目除了 NL2SQL，还实现了一个可结合文档知识的 AI 助手模块，具备更强的产品完整度。

## 部署建议

如果用于求职展示，推荐：

- 前端部署到 `Vercel`
- 后端部署到 `Railway`、`Render` 或 `VPS`

需要持久化的数据包括：

- 元数据库文件
- Chroma 向量数据
- 文档知识库数据
- 运行时配置文件

如果只部署成纯静态前端，无法完整演示这个项目的核心能力。

## 知识库文档

仓库保留了一个可直接上传到文档知识库系统的完整 Markdown 文档：

- `TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md`

这份文档适合：

- 上传到项目内置知识库
- 上传到外部 RAG 文档库
- 给 AI 助手作为项目背景知识

## 截图建议

你后面给我截图时，建议至少准备这 5 张，我再帮你补进 README：

1. 对话查询页：展示自然语言问题、生成 SQL、结果表格
2. 连接管理页：展示已接入数据库和连接状态
3. RAG 增强页：展示索引状态、检索信息或健康状态
4. AI 助手配置页：展示模型连接测试成功
5. 知识库管理页：展示文档上传成功后的统计信息

## 总结

这个项目最重要的价值，不是“做了一个 Text-to-SQL”，而是把一个 AI Agent 系统从模型调用、检索增强、后端服务、前端交互、知识库、流式输出、配置管理，到部署展示，做成了一套完整产品。

如果面试岗位是：

- AI Agent 开发
- LLM 应用工程
- AI 产品工程
- RAG 工程
- 全栈 AI 工程

这个项目都能直接作为主项目来讲。
