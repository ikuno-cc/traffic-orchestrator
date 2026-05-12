# Traffic Orch

Traffic Orch is a traffic and request orchestration platform used to route jobs to services such as n8n, ComfyUI, and custom endpoints.

## What It Contains

- `app/`: FastAPI backend (service management, dispatch API, stats)
- `workers/`: Supabase-backed worker logic for async dispatch execution
- `frontend/`: React/Vite dashboard for operators
- `docker/`: container definitions and compose file
- `scrapling_service/`: auxiliary scraping service container assets

## Runtime Architecture

- UI -> API -> Supabase queue table -> service-specific workers -> external target service
- Optional webhook callbacks after job completion

## Quick Start (Docker)

```bash
cd "Traffic Orch/docker"
docker compose up -d --build
```

Access:
- UI: `http://localhost:3000`
- API docs: `http://localhost:8000/docs`

## Core Workflows

1. Register services (`/services`)
2. Dispatch request payloads (`/dispatch`)
3. Track status/history (`/requests`, `/stats`)
4. Pause/resume services during incidents

## Local Development

Backend requirements are in `requirements.txt`.

Frontend:

```bash
cd "Traffic Orch/frontend"
npm install
npm run dev
```

## Key Files

- `app/main.py`
- `workers/tasks.py`
- `frontend/src/pages/Dispatch.jsx`
- `docker/docker-compose.yml`

## Troubleshooting

- If dispatches remain queued: verify Supabase connectivity and worker logs.
- If UI cannot call API: verify API URL settings and Docker network reachability.
- If external calls fail: validate registered service URL/headers/timeout.
