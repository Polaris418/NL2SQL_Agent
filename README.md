# NL2SQL Agent

NL2SQL Agent 是一个面向数据分析场景的全栈自然语言转 SQL 系统。它支持用户用中文或英文提出数据问题，系统会结合数据库 Schema 检索、Prompt 模板、模型配置、SQL 执行和结果解释，完成从自然语言问题到可执行 SQL 的完整链路。

在线演示：

- 演示地址：[https://nl2sql.polaristools.online](https://nl2sql.polaristools.online)
- 健康检查：[https://nl2sql.polaristools.online/health](https://nl2sql.polaristools.online/health)

## 项目定位

这个项目不是一个简单的 Text-to-SQL Demo，而是一套可运行、可配置、可观测的 AI 数据查询产品原型，重点覆盖：

- 自然语言问题改写、Schema 检索、SQL 生成、执行、纠错和结果总结。
- MySQL / MariaDB、PostgreSQL、SQLite 多数据库连接管理。
- 基于数据库结构的 Schema RAG，支持中文表名、中文字段注释和中文问题。
- 文档知识库检索，可作为 AI 助手的额外知识来源。
- OpenAI-compatible、Anthropic 等外部模型接入。
- LLM 配置、AI 助手配置、Prompt 配置和 RAG 状态管理。
- 查询历史、分析看板、RAG telemetry、索引健康度和诊断能力。
- 前后端分离部署，支持 Linux + systemd + Nginx 生产环境。

## 核心功能

### 对话式 SQL 工作台

- 输入自然语言问题并生成 SQL。
- 展示问题改写、Schema 检索、SQL 生成、错误修复等查询过程。
- 返回结果表格、执行耗时、图表建议和结果摘要。
- 支持流式查询输出、查询历史回放和手动 SQL 执行。

示例问题：

- 统计每个部门的员工数量。
- 计算过去 30 天的订单总金额。
- 查询销售额排名前 10 的商品。
- 查询最近一周的活跃用户。
- 找出从未下单的客户。

### 数据库连接管理

- 支持 MySQL / MariaDB、PostgreSQL、SQLite。
- 支持新增、测试、删除连接。
- 支持同步数据库 Schema。
- 每个连接维护独立的 Schema、索引状态和健康信息。

### Schema RAG

- 在生成 SQL 前检索相关表、字段、注释和关系。
- 支持每个数据库连接独立构建索引。
- 支持查看索引状态、索引健康度、构建任务、Schema 版本和 telemetry。
- 支持 Chroma 向量存储，也提供轻量 fallback，便于低资源环境运行。

### 文档知识库

- 支持上传 Markdown / TXT 文档。
- 自动切块、向量化和检索。
- 可作为 AI 助手或业务问答的上下文来源。
- 在向量服务不可用时支持降级策略。

### 模型和 Prompt 配置

- 支持 OpenAI-compatible 接口和 Anthropic 接口。
- 支持配置 Base URL、模型名、API Key 和路由策略。
- 支持连接测试。
- 支持 Prompt 模板和 few-shot 示例配置。

### AI 助手

- 提供悬浮式 AI 助手。
- 支持流式回答。
- 支持独立于主 NL2SQL 链路的模型配置。
- 支持结合知识库内容回答问题。

## 技术栈

### 前端

- React 18
- TypeScript
- Vite
- Tailwind CSS
- Zustand
- Axios
- Radix UI / shadcn 风格组件
- Recharts
- Monaco Editor

### 后端

- FastAPI
- Pydantic
- SQLAlchemy
- SQLite 元数据数据库
- Chroma / 内存向量存储 fallback
- OpenAI SDK
- Anthropic SDK

### 检索与增强

- Schema RAG
- Document RAG
- BM25 / 向量混合检索
- Prompt 模板
- Few-shot 示例
- RAG telemetry 和 debug view

## 项目结构

```text
.
├── app/                                  # FastAPI 主应用
│   ├── agent/                            # NL2SQL 主链路
│   ├── api/                              # HTTP API
│   ├── core/                             # 配置、工厂、依赖注入
│   ├── db/                               # 数据库连接器、元数据存储
│   ├── llm/                              # LLM 客户端
│   ├── prompts/                          # Prompt 模板
│   ├── rag/                              # RAG、向量检索、索引健康度
│   └── schemas/                          # Pydantic 数据模型
├── backend/
│   └── scripts/                          # 示例数据和 Windows 启动脚本
├── config/                               # 配置文件
├── data/demo/                            # 演示数据库 SQL
├── deploy/linux/                         # Linux 部署配置
├── image/                                # README 截图资源
├── NL2SQL Agent Frontend Development/    # React 前端
├── requirements.txt                      # Python 依赖
├── pyproject.toml                        # Python 项目配置
├── docker-compose.yml
└── TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md   # 项目知识库文档
```

## 本地启动

### 1. 准备后端环境

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

启动后端：

```powershell
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

后端默认地址：

```text
http://127.0.0.1:8000
```

### 2. 启动前端

```powershell
cd "NL2SQL Agent Frontend Development"
npm install
npm run dev
```

前端默认地址：

```text
http://127.0.0.1:5173
```

## 环境变量

复制 `.env.example` 为 `.env` 后按需修改。常见配置包括：

- `DATABASE_URL`：元数据数据库地址。
- `OPENAI_API_KEY`：OpenAI-compatible 模型密钥。
- `OPENAI_BASE_URL`：OpenAI-compatible Base URL。
- `OPENAI_MODEL`：默认模型名。
- `ANTHROPIC_API_KEY`：Anthropic 模型密钥。
- `RAG_LIGHTWEIGHT_MODE`：轻量 RAG 模式。

具体字段以 `.env.example` 为准。

## 演示数据库

仓库提供了一份适合快速体验的 MariaDB / MySQL 演示数据库脚本：

- [data/demo/polaris_quick_query_demo.sql](data/demo/polaris_quick_query_demo.sql)

导入方式：

```bash
mysql -uroot -p < data/demo/polaris_quick_query_demo.sql
```

导入后会创建示例数据库 `polaris_demo`，可用于测试以下问题：

- 统计每个部门的员工数量。
- 计算过去 30 天的订单总金额。
- 查询销售额排名前 10 的商品。
- 分析各地区的销售趋势。
- 找出复购率最高的客户。
- 对比本月与上月的销售业绩。
- 查询最近一周的活跃用户。
- 列出所有未完成的订单。
- 显示库存不足的商品。
- 查询每个订单的详细信息，包括客户和产品。

## 主要接口

### 查询与执行

- `POST /api/query`
- `POST /api/query/stream`
- `POST /api/query/sql`
- `POST /api/query/export`
- `POST /api/query/sql/export`

### 数据库连接

- `GET /api/connections`
- `POST /api/connections`
- `POST /api/connections/{connection_id}/test`
- `POST /api/connections/{connection_id}/sync`
- `GET /api/connections/{connection_id}/schema`

### RAG 与索引状态

- `GET /api/rag/index/status`
- `GET /api/rag/index/status/{connection_id}`
- `POST /api/rag/index/{connection_id}/rebuild`
- `GET /api/rag/index/jobs`
- `GET /api/rag/index/health/{connection_id}`
- `GET /api/rag/telemetry/dashboard`
- `GET /api/rag/telemetry/events`
- `GET /api/rag/telemetry/summary`

### LLM 设置

- `GET /api/settings/llm`
- `POST /api/settings/llm/profiles`
- `PUT /api/settings/llm/routing`
- `DELETE /api/settings/llm/profiles/{profile_id}`
- `POST /api/settings/llm/test`

### AI 助手与知识库

- `GET /api/assistant/config`
- `PUT /api/assistant/config`
- `POST /api/assistant/config/test`
- `POST /api/assistant/chat/stream`
- `POST /api/documents/upload`
- `GET /api/documents/stats`
- `POST /api/documents/search`

## 页面截图

### 对话式 SQL 工作台

![对话式 SQL 工作台](image/image.png)

### SQL 生成与结果展示

![SQL 生成与结果展示](image/image%20copy%202.png)

### 数据库连接管理

![数据库连接管理](image/image%20copy%204.png)

### RAG 状态管理

![RAG 状态管理](image/image%20copy%205.png)

### 文档知识库

![文档知识库](image/image%20copy%2010.png)

### AI 助手配置

![AI 助手配置](image/image%20copy%2012.png)

## Linux 部署

仓库提供了非 Docker 的 Linux 直接部署配置：

- [deploy/linux/DEPLOY.md](deploy/linux/DEPLOY.md)
- [deploy/linux/nl2sql-agent.service](deploy/linux/nl2sql-agent.service)
- [deploy/linux/nl2sql-agent.nginx.conf](deploy/linux/nl2sql-agent.nginx.conf)

典型生产环境：

- Debian / Ubuntu
- Python 3.11
- systemd
- Nginx
- HTTPS 证书
- 域名反向代理

## 开发检查

后端测试：

```powershell
pytest
```

前端构建：

```powershell
cd "NL2SQL Agent Frontend Development"
npm run build
```

代码格式检查可使用：

```powershell
ruff check app
```

## 相关文档

- [TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md](TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md)
- [deploy/linux/DEPLOY.md](deploy/linux/DEPLOY.md)
