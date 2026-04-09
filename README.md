# NL2SQL Agent

NL2SQL Agent 是一个面向数据分析场景的全栈 AI 系统。它把自然语言问题转成 SQL，并结合 Schema RAG、文档知识库、流式对话、模型配置、查询历史和观测能力，提供一套完整的 AI 数据查询产品。

## 在线演示

- 演示地址：[https://nl2sql.polaristools.online](https://nl2sql.polaristools.online)
- 健康检查：[https://nl2sql.polaristools.online/health](https://nl2sql.polaristools.online/health)

## 项目定位

这个项目不是单纯的 Text-to-SQL Demo，而是一套可以实际演示以下能力的系统：

- 自然语言到 SQL 的完整链路
- 多数据库连接管理与 Schema 同步
- 基于数据库结构的 Schema RAG 检索增强
- 基于 Markdown / TXT 文档的知识库检索
- AI 助手独立配置、连接测试与流式输出
- 历史记录、分析报表、RAG 状态页与诊断能力
- 前后端分离部署、Nginx 反代、生产环境运行

## 核心功能

### 1. 对话式 SQL 工作台

- 用户输入自然语言问题
- 系统自动重写问题、检索相关表结构、生成 SQL、执行查询
- 返回结果表格、执行信息、图表建议和结果总结
- 支持流式查询输出

典型问题：

- 统计每个部门的员工数量
- 计算过去 30 天的订单总金额
- 查询销售额排名前 10 的产品
- 查询最近一周的活跃用户
- 找出从未下单的客户

### 2. 数据库连接管理

- 支持 MySQL / MariaDB、PostgreSQL、SQLite
- 支持新增、测试、删除连接
- 支持同步数据库 Schema
- 每个连接都可维护独立状态和缓存

### 3. Schema RAG

- 在生成 SQL 前先检索相关表、字段、关系线索
- 维护每个连接独立的索引状态、健康状态和预构建任务
- 支持查看索引状态、重建索引、查看运行指标
- 生产环境默认支持轻量模式，避免小内存机器被本地模型拖垮

### 4. 文档知识库

- 支持上传 Markdown / TXT 文档
- 自动切块、向量化、检索
- 可作为 AI 助手的额外知识来源
- 在资源不足场景下支持 fallback，保证功能可用

### 5. AI 助手

- 悬浮窗交互
- 支持拖动、最小化成球
- 支持流式回答
- 支持模型连接测试
- 支持独立于主系统的模型配置

### 6. 运维与观测

- 查询历史记录
- 分析报表
- RAG telemetry 和索引状态页
- 健康检查接口
- 适配单机 Linux 生产部署

## 系统架构

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
- Chroma / 内存向量存储 fallback
- sentence-transformers / 轻量 fallback embedding

### AI 与检索

- OpenAI 兼容模型接入
- Anthropic 接入
- Schema RAG
- Document RAG
- Prompt 模板与 Few-shot 示例

## 主要页面

### 对话查询

- 自然语言提问
- 流式输出 SQL 查询过程
- 结果表格与分页
- 历史问题回放

### 连接管理

- 新建数据库连接
- 测试连接
- 同步 Schema
- 管理当前连接

### RAG 增强

- 查看每连接预构建状态
- 查看索引健康状态
- 查看 telemetry 概览
- 手动重建索引

### 知识库管理

- 上传文档
- 查看统计信息
- 查看文本块和向量状态

### AI 助手配置

- 独立模型配置
- API Base URL 配置
- 模型连接测试
- AI 助手开关

## 演示截图

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

### AI 助手配置与模型测试
![AI 助手配置与模型测试](image/image%20copy%2012.png)

## 示例演示数据库

仓库内提供了一份适合“快速查询卡片”演示的 MariaDB / MySQL 示例库脚本：

- [data/demo/polaris_quick_query_demo.sql](/Users/Administrator/Desktop/Text-to-SQL%20Agent/data/demo/polaris_quick_query_demo.sql)

导入后会创建数据库：

- `polaris_demo`

导入方式：

```bash
mysql -uroot -p < data/demo/polaris_quick_query_demo.sql
```

它覆盖以下典型问题：

- 统计每个部门的员工数量
- 计算过去 30 天的订单总金额
- 查询销售额排名前 10 的产品
- 分析各地区的销售趋势
- 找出复购率最高的客户
- 对比本月与上月的销售业绩
- 查询最近一周的活跃用户
- 列出所有未完成的订单
- 显示库存不足的商品
- 查询每个订单的详细信息包括客户和产品
- 找出从未下单的客户
- 统计每个分类下的产品数量和平均价格

## 项目结构

```text
.
├─ app/                                   # FastAPI 主应用
│  ├─ agent/                              # NL2SQL 主链路
│  ├─ api/                                # HTTP API
│  ├─ core/                               # 应用工厂、配置、依赖注入
│  ├─ db/                                 # 数据库连接器、元数据存储
│  ├─ llm/                                # 模型客户端
│  ├─ prompts/                            # Prompt 模板
│  ├─ rag/                                # RAG、向量检索、遥测
│  └─ schemas/                            # Pydantic 数据模型
├─ config/                                # 配置文件目录
├─ data/                                  # 数据目录
├─ deploy/linux/                          # Linux 直部署配置
├─ image/                                 # README 截图资源
├─ NL2SQL Agent Frontend Development/     # React 前端
├─ requirements.txt                       # Python 依赖
├─ pyproject.toml                         # Python 项目配置
├─ docker-compose.yml
└─ TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md    # 项目知识库文档
```

## 本地启动

### 1. 启动后端

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

默认地址：

```text
http://127.0.0.1:8000
```

### 2. 启动前端

```bash
cd "NL2SQL Agent Frontend Development"
npm install
npm run dev
```

默认地址：

```text
http://127.0.0.1:5173
```

## Linux 服务器部署

仓库内提供了非 Docker 的 Linux 直部署配置：

- [deploy/linux/DEPLOY.md](/Users/Administrator/Desktop/Text-to-SQL%20Agent/deploy/linux/DEPLOY.md)
- [deploy/linux/nl2sql-agent.service](/Users/Administrator/Desktop/Text-to-SQL%20Agent/deploy/linux/nl2sql-agent.service)
- [deploy/linux/nl2sql-agent.nginx.conf](/Users/Administrator/Desktop/Text-to-SQL%20Agent/deploy/linux/nl2sql-agent.nginx.conf)

当前线上环境即采用：

- Debian
- systemd
- Nginx
- 自有域名反代
- HTTPS 证书

## 主要接口

### 查询与执行

- `POST /api/query`
- `POST /api/query/stream`
- `POST /api/query/sql`
- `POST /api/query/export`

### 数据库连接

- `GET /api/connections`
- `POST /api/connections`
- `POST /api/connections/{connection_id}/test`
- `POST /api/connections/{connection_id}/sync`
- `GET /api/connections/{connection_id}/schema`

### RAG 与状态

- `GET /api/rag/index/status`
- `GET /api/rag/index/health/{connection_id}`
- `POST /api/rag/index/{connection_id}/rebuild`
- `GET /api/rag/telemetry/dashboard`

### AI 助手与知识库

- `GET /api/assistant/config`
- `PUT /api/assistant/config`
- `POST /api/assistant/config/test`
- `POST /api/assistant/chat/stream`
- `POST /api/documents/upload`
- `GET /api/documents/stats`
- `POST /api/documents/search`

## 当前项目文档

仓库保留了一份适合导入知识库的项目总文档：

- [TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md](/Users/Administrator/Desktop/Text-to-SQL%20Agent/TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md)
