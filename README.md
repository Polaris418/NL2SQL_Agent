# NL2SQL Agent

NL2SQL Agent is a full-stack AI application that turns natural language questions into SQL, executes queries against real databases, and explains the result with an agent-style workflow. The project is designed as a portfolio-grade demo for AI Agent / LLM application engineering, not just a toy text-to-SQL script.

It includes:

- A FastAPI backend with an orchestrated NL2SQL pipeline
- A React + Vite frontend for conversational BI and configuration management
- Multi-database support for MySQL, PostgreSQL, and SQLite
- RAG-based schema retrieval and document knowledge retrieval
- Query history, analytics, prompt management, and AI assistant modules
- Streaming output for both SQL generation workflows and the floating AI assistant

## Why This Project Exists

Most text-to-SQL demos stop at "ask a question, get SQL". This project goes further and tries to resemble a real AI product:

- It manages database connections
- It builds and monitors schema indexes
- It retrieves relevant schema context before generation
- It supports retries and error reflection
- It exposes telemetry and history for debugging
- It includes an assistant that can answer usage questions with document-grounded context

That makes it suitable for:

- AI Agent / LLM application demos
- full-stack AI product portfolios
- NL2SQL / RAG system interviews
- internal data copilot prototypes

## Core Features

### 1. NL2SQL agent pipeline

The backend coordinates a multi-step workflow:

1. Rewrite the user question into retrieval-friendly intent
2. Retrieve relevant schema context with hybrid RAG
3. Generate SQL with the selected LLM
4. Validate and execute the SQL safely
5. Summarize and visualize the result
6. Persist query history and telemetry

### 2. Full-stack product UI

The frontend includes:

- conversational query workspace
- connection management
- RAG index management and telemetry dashboard
- analytics and query history pages
- prompt configuration
- AI assistant configuration
- document knowledge base management
- floating AI assistant with streaming output

### 3. RAG and knowledge features

The project contains two distinct retrieval layers:

- **Schema RAG**: retrieves relevant tables, columns, and relationship clues for SQL generation
- **Document RAG**: lets the AI assistant answer from uploaded Markdown / TXT knowledge documents

### 4. Deployment-friendly structure

The repository is organized so the frontend and backend can be deployed separately:

- frontend: Vercel / static hosting
- backend: Railway / Render / VPS / Docker

## Tech Stack

### Backend

- Python 3.11+
- FastAPI
- Pydantic
- SQLite metadata store
- Chroma vector store
- sentence-transformers

### Frontend

- React 18
- TypeScript
- Vite
- Tailwind CSS
- Axios

### AI / Retrieval

- OpenAI-compatible and Anthropic-compatible LLM integration
- hybrid schema retrieval
- document embeddings + vector search
- prompt-driven agent steps

## Repository Structure

```text
.
├── app/                                   # FastAPI application
│   ├── agent/                             # NL2SQL agent workflow
│   ├── api/                               # HTTP APIs
│   ├── core/                              # app wiring and config
│   ├── db/                                # database connectors and repositories
│   ├── llm/                               # LLM client layer
│   ├── prompts/                           # prompt templates
│   ├── rag/                               # retrieval, indexing, embeddings, telemetry
│   └── schemas/                           # API / domain schemas
├── backend/                               # backend Dockerfile and helper scripts
├── config/                                # tracked config assets (for example synonyms)
├── tests/                                 # backend tests
├── NL2SQL Agent Frontend Development/     # React frontend
├── pyproject.toml                         # backend dependencies
├── docker-compose.yml
└── TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md    # project knowledge base document
```

## Quick Start

### 1. Backend

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
copy .env.example .env
uvicorn app.main:app --reload
```

Backend default URL:

```text
http://127.0.0.1:8000
```

### 2. Frontend

```bash
cd "NL2SQL Agent Frontend Development"
npm install
npm run dev
```

Frontend default URL:

```text
http://127.0.0.1:5173
```

## Main API Endpoints

### Query and agent

- `POST /api/query`
- `POST /api/query/stream`
- `POST /api/query/sql`

### Connections and schema

- `GET /api/connections`
- `POST /api/connections`
- `POST /api/connections/{connection_id}/sync`
- `GET /api/connections/{connection_id}/schema`

### History and analytics

- `GET /api/history`
- `GET /api/analytics/summary`
- `GET /api/rag/telemetry/dashboard`

### AI assistant and knowledge

- `POST /api/assistant/chat`
- `POST /api/assistant/chat/stream`
- `GET /api/assistant/knowledge`
- `POST /api/documents/upload`
- `POST /api/documents/search`

## Demo Scenarios

This project demos well in interviews because you can show multiple layers of AI product engineering:

### Scenario 1: Text-to-SQL

- connect a real database
- ask a natural language question
- inspect generated SQL
- run the query and view chart suggestions

### Scenario 2: Retrieval-aware generation

- show index status and health
- ask a schema-specific question
- explain how relevant tables were retrieved before SQL generation

### Scenario 3: AI assistant with document grounding

- upload a Markdown knowledge document
- ask the floating assistant a product question
- show streaming answer + retrieved sources

### Scenario 4: Product-level engineering

- test LLM connection configuration
- inspect history, telemetry, and analytics
- show configuration pages and deployment readiness

## Deployment Notes

Recommended portfolio deployment:

- **Frontend**: Vercel
- **Backend**: Railway, Render, or a VPS

If you deploy the backend, make sure you persist runtime data directories and files instead of treating the service as fully stateless. In production you should persist:

- metadata database
- Chroma vector data
- uploaded document metadata
- runtime config that should survive restarts

## Knowledge Base Document

The file [`TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md`](./TEXT_TO_SQL_AGENT_KNOWLEDGE_BASE.md) is included as the single long-form project knowledge document for document-RAG ingestion or external knowledge base upload.

## What Makes This Interview-Relevant

This repository demonstrates practical AI engineering skills across:

- agent workflow design
- LLM integration
- RAG system integration
- backend API design
- frontend product implementation
- streaming UX
- deployment awareness
- debugging and telemetry

If you are hiring for AI agent application development, this project is intended to show the gap between a prompt wrapper and a real productized AI system.
