# DealLens AI M&A Screener

A comprehensive M&A deal screening platform with AI-powered analysis, featuring a Bloomberg Terminal-inspired interface. Production-ready with robust monitoring, security, and reliability features.

## 🏗️ Architecture

This is a monorepo containing:

- **apps/web** - Next.js frontend with Bloomberg Terminal-style UI (deployed on Vercel)
- **apps/api** - FastAPI backend with ML/AI capabilities (deployed on Railway)
- **apps/worker** - Background task processing with Celery (deployed on Railway)
- **packages/db** - Shared database schema and utilities

```
┌─────────────────┐     ┌────────────────┐     ┌─────────────────┐
│  Frontend       │     │  Backend API   │     │  Worker         │
│  (Next.js)      │────▶│  (FastAPI)     │◀────│  (Celery)       │
│  [Vercel]       │     │  [Railway]     │     │  [Railway]      │
└─────────────────┘     └────────────────┘     └─────────────────┘
                               │  ▲                     │
                               │  │                     │
                               ▼  │                     ▼
                        ┌─────────────────┐    ┌─────────────────┐
                        │  PostgreSQL DB  │    │  Redis Cache    │
                        │  [Railway]      │    │  [Railway]      │
                        └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Node.js 18.20.4 (specified in `.nvmrc`)
- Python 3.11
- Docker & Docker Compose
- pnpm 10.14.0

### Setup

1. **Clone and install dependencies**
```bash
git clone <your-repo>
cd DealLens-AI-MA-Screener
pnpm install
```

2. **Environment setup**
```bash
# Copy environment files for each service
cp apps/api/.env.example apps/api/.env
cp apps/worker/.env.example apps/worker/.env
cp apps/web/.env.example apps/web/.env.local
# Edit with your configuration
```

3. **Start infrastructure**
```bash
# Start PostgreSQL and Redis
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
docker run -d --name redis -p 6379:6379 redis:7
# Or use docker-compose
pnpm run docker:up
```

4. **Database setup**
```bash
pnpm run db:generate
pnpm run db:push
```

5. **Start development servers**
```bash
# Start all services in parallel
pnpm run dev

# Or start each service individually
# Terminal 1: API
cd apps/api && uvicorn main:app --reload

# Terminal 2: Worker
cd apps/worker && celery -A celery_app.app worker -B --loglevel=info

# Terminal 3: Web
pnpm --filter @deallens/web dev
```

## 📦 Services

- **Frontend (Next.js)**: http://localhost:3000
- **API (FastAPI)**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **API Health**: http://localhost:8000/healthz
- **API Readiness**: http://localhost:8000/readyz
- **PostgreSQL**: localhost:5432
- **Redis**: localhost:6379
- **Metrics (Optional)**: http://localhost:8000/metrics (when enabled)

## 🛠️ Development

### Available Scripts

- `pnpm run dev` - Start all services in development mode
- `pnpm run build` - Build all applications
- `pnpm run lint` - Lint all code
- `pnpm run test` - Run tests
- `pnpm run db:generate` - Generate Prisma client
- `pnpm run db:push` - Push schema changes to database
- `pnpm run docker:up` - Start Docker containers

### Project Structure

```
├── apps/
│   ├── web/          # Next.js frontend
│   ├── api/          # FastAPI backend
│   └── worker/       # Celery worker
├── packages/
│   └── db/           # Database schema & utilities
├── docker-compose.yml
└── turbo.json
```

## 🎨 Design System

The UI is inspired by Bloomberg Terminal with:
- Dark theme with financial data emphasis
- Real-time data displays with API health monitoring
- Professional trading interface aesthetics
- Responsive design for desktop and tablet
- Terminal-style command interface

## 📊 Features

- M&A Deal Screening & Analysis
- Company Financial Metrics
- Market Data Visualization
- AI-Powered Deal Recommendations
- Real-time Data Processing
- Advanced Filtering & Search

## 🔒 Production Features

- **Health Monitoring**: API and service health endpoints
- **Reliability**: Idempotent tasks, retry mechanisms, circuit breakers
- **Security**: JWT validation, rate limiting, CORS protection
- **Observability**: Structured JSON logging, request tracing, metrics
- **Cost Controls**: API usage tracking, budget enforcement
- **CI/CD**: GitHub Actions workflows for quality control
- **Documentation**: Comprehensive guides and setup instructions

## 🔧 Tech Stack

- **Frontend**: Next.js 15, React 18, TypeScript, Tailwind CSS
- **Backend**: FastAPI, Python 3.11, Pydantic
- **Database**: PostgreSQL 15, Prisma
- **Cache/Queue**: Redis 7, Celery
- **Infrastructure**: Docker, Railway, Vercel, pnpm workspaces, Turborepo
- **Observability**: JSON logging, Prometheus metrics (optional)
- **Security**: JWT, rate limiting, environment validation
- **AI/ML**: OpenAI integration, NLP on news & filings

## 📝 Documentation

- [CHECKLIST.md](./CHECKLIST.md) - Comprehensive production checklist
- [DEMO.md](./DEMO.md) - Step-by-step demo verification guide

---

DealLens – An AI-powered M&A deal screener that automates investment banking workflows. Production-ready with enterprise-grade monitoring, security, and reliability features.
