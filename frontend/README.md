# Frontend Setup

## Development

```bash
cd frontend
npm install
npm start
```

This starts the React development server on `http://localhost:3000`.

## Production Build

```bash
npm run build
```

The optimized build will be created in the `build/` directory.

## Docker Build

The `Dockerfile.gui` automatically builds the React app and serves it with Nginx:

```bash
docker build -f Dockerfile.gui -t traffic-orchestrator-gui .
docker run -p 80:80 traffic-orchestrator-gui
```

## Environment Variables

Create a `.env` file in the `frontend/` directory:

```env
REACT_APP_API_URL=/api
SKIP_PREFLIGHT_CHECK=true
```

## Tech Stack

- **React 18** - UI framework
- **Axios** - HTTP client
- **Custom CSS** - Cyberpunk-inspired dark theme

## Key Components

- **Dashboard** - System stats and overview
- **Services** - CRUD operations for service configuration
- **Requests** - Filter and monitor request history
- **Dispatch** - Test endpoint with JSON payload editor
- **Topbar** - Real-time clock and system status
- **Sidebar** - Navigation with live badge counts
- **Toast** - Non-blocking notifications

## Styling

All styles are in CSS without external UI libraries. The design uses a custom dark theme with:
- Cyberpunk color palette
- Smooth animations
- Responsive grid layouts
- Professional typography (Syne + JetBrains Mono)

## API Integration

The frontend communicates with the FastAPI backend via `/api` prefix. In development, the proxy in `docker-compose.yml` handles routing. In production, Nginx rewrites requests to the API container.

See `src/api.js` for all available API endpoints.
