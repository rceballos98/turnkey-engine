# Turnkey Engine

FastAPI service that generates AI-powered property reports for NYC addresses. A background worker fetches public data (PLUTO, ACRIS, DOB, sales comps via Socrata), transforms it, generates AI narratives with Claude, renders an HTML report, and converts it to PDF.

## Architecture

```
                         ┌─────────────────────────────┐
  Browser ──► Stripe ──► │  POST /webhook/stripe        │
                         │                              │
  Client ──────────────► │  turnkey-engine-api           │ ──► Postgres
                         │  (FastAPI / Gunicorn)         │       ▲
                         └──────────────────────────────┘       │
                                                                │
                         ┌──────────────────────────────┐       │
                         │  turnkey-engine-worker         │ ──────┘
                         │  (polling loop, 5s interval)   │
                         └──────────────────────────────┘
                           Fetchers → Transform → AI → Render → PDF
```

- **API** — FastAPI behind Gunicorn. Handles auth, creates reports, enqueues jobs, processes Stripe webhooks.
- **Worker** — Background process that polls `job_queue`, claims jobs via `SELECT ... FOR UPDATE SKIP LOCKED`, runs the report pipeline, and writes results back.
- **Postgres** — Render managed Postgres. Schema managed by Alembic migrations (currently at revision 003).

### Report Pipeline

The worker runs a 5-phase async pipeline for each job:

1. **Fetch** — Pulls data from PLUTO, ACRIS, DOB, NYC sales comps, and other Socrata datasets
2. **Transform** — Normalizes raw data into a structured format
3. **AI Narratives** — Claude generates human-readable analysis sections
4. **Render** — Injects transformed data + narratives into an HTML template
5. **PDF** — Converts HTML to PDF via Browserless (falls back to `.html` if unavailable)

### Authentication

Two independent auth paths:

| Path | Header | Use Case |
|------|--------|----------|
| Bearer token | `Authorization: Bearer <INTERNAL_API_KEY>` | Internal/testing — full access, no payment needed |
| Payment token | `X-Payment-Token: <stripe_session_id>` | Public — after Stripe checkout |

Bearer token auth always works, even when Stripe is not configured. The system is fully functional without Stripe credentials.

## Local Development

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Sync dependencies (creates .venv automatically)
uv sync

# 3. Start Postgres
docker run -d --name turnkey-pg \
  -e POSTGRES_DB=turnkey -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 postgres:16

# 4. Configure env
cp .env.example .env
# Edit .env with your API keys

# 5. Run migrations
uv run alembic upgrade head

# 6. Start API
uv run uvicorn app.main:app --reload

# 7. Start worker (separate terminal)
uv run python -m app.worker
```

## Testing

Integration tests run against the real local Postgres (the `turnkey-pg` container) — no mocking except for the pipeline itself.

```bash
uv sync          # includes dev dependencies by default
uv run pytest tests/ -v
```

Tests cover: health check, auth (missing key, invalid key, payment token, invalid payment token), report CRUD, full lifecycle (create → enqueue → claim → process → retrieve), status endpoint, and checkout when Stripe is not configured.

## API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | None | Health check |
| `POST` | `/checkout` | None | Create Stripe Checkout Session |
| `POST` | `/webhook/stripe` | Stripe signature | Handle Stripe webhook events |
| `GET` | `/reports/status?session_id=` | None | Look up report by Stripe session ID |
| `POST` | `/reports` | Bearer or Payment | Create a report and enqueue for processing |
| `GET` | `/reports/{id}` | Bearer or Payment | Get report by ID |
| `GET` | `/reports/{id}/pdf` | Bearer or Payment | Download generated PDF/HTML |

### Examples

```bash
# Create a report (internal)
curl -X POST https://turnkey-engine-api.onrender.com/reports \
  -H "Authorization: Bearer $INTERNAL_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"address": "301 E 79th St, Unit 3B, New York, NY 10075"}'

# Check report status
curl https://turnkey-engine-api.onrender.com/reports/<REPORT_ID> \
  -H "Authorization: Bearer $INTERNAL_API_KEY"

# Start a checkout (requires STRIPE_SECRET_KEY)
curl -X POST https://turnkey-engine-api.onrender.com/checkout \
  -H "Content-Type: application/json" \
  -d '{"address": "301 E 79th St, Unit 3B, New York, NY 10075"}'
# Returns {checkout_url, session_id, address_hash}

# After payment, check report status by session
curl "https://turnkey-engine-api.onrender.com/reports/status?session_id=cs_..."
```

## Deployment (Render)

Deployed via [Render Blueprint](https://docs.render.com/infrastructure-as-code) using `render.yaml`. Auto-deploys on push to `main`.

| Service | Type | Plan |
|---------|------|------|
| `turnkey-engine-api` | Web Service | Starter |
| `turnkey-engine-worker` | Background Worker | Starter |
| Postgres | Managed Database | Free |

Render runs the build command (`uv sync --frozen --no-dev`) and pre-deploy command (`alembic upgrade head`) automatically on each deploy. The worker uses the same build but starts `python -m app.worker` instead of Gunicorn.

### Environment Variables

| Variable | Required | Where to set | Notes |
|----------|----------|--------------|-------|
| `DATABASE_URL` | Yes | API + Worker | Render Postgres internal URL |
| `INTERNAL_API_KEY` | Yes | API + Worker | Auto-generated by Render |
| `ANTHROPIC_API_KEY` | Yes | API + Worker | [Anthropic Console](https://console.anthropic.com/) |
| `FIRECRAWL_API_KEY` | Yes | API + Worker | [Firecrawl](https://firecrawl.dev/) |
| `BROWSERLESS_URL` | Yes | Worker | Browserless instance URL |
| `BROWSERLESS_TOKEN` | Yes | Worker | Browserless API token |
| `BASE_URL` | Yes | API | Your `.onrender.com` URL |
| `PDF_DIR` | No | Worker | Defaults to `/data/pdfs` |
| `STRIPE_SECRET_KEY` | No | API | Stripe dashboard → API keys |
| `STRIPE_WEBHOOK_SECRET` | No | API + Worker | Stripe dashboard → Webhooks |
| `REPORT_PRICE_CENTS` | No | API | Defaults to `2500` ($25) |

### First-time setup

1. Push repo to GitHub
2. Create a **Blueprint** in Render, point it at the repo
3. Create a **Postgres** instance in Render (free tier, same region — Oregon)
4. Set `DATABASE_URL` on both API and worker to the **Internal Database URL**
5. Set the remaining env vars (Anthropic, Firecrawl, Browserless, BASE_URL)
6. Render runs `alembic upgrade head` as a pre-deploy step to create tables
7. `INTERNAL_API_KEY` is auto-generated on first deploy

### Live URLs

- API: `https://turnkey-engine-api.onrender.com`
- Health: `https://turnkey-engine-api.onrender.com/health`
- Docs: `https://turnkey-engine-api.onrender.com/docs`

## Stripe Integration

The Stripe payment flow allows public users to pay $25 and receive a property report. The flow is:

1. Client sends `POST /checkout` with an address
2. Server creates a Stripe Checkout Session and returns the `checkout_url`
3. User pays on Stripe's hosted checkout page
4. Stripe fires a `checkout.session.completed` webhook to `POST /webhook/stripe`
5. Webhook handler creates a Payment record, a Report, and enqueues a job
6. User polls `GET /reports/status?session_id=cs_...` to check progress
7. Once complete, user can fetch the report with `X-Payment-Token: cs_...`

**Current status:** The Stripe integration code is in place and passing tests, but has not been tested end-to-end with real Stripe credentials. We need to:

1. Create a Stripe account and get API keys
2. Set `STRIPE_SECRET_KEY` and `STRIPE_WEBHOOK_SECRET` on Render
3. Configure the webhook endpoint in Stripe dashboard (`https://turnkey-engine-api.onrender.com/webhook/stripe`)
4. Test with Stripe's test mode cards

Without Stripe credentials, `POST /checkout` returns 503. All other endpoints work normally with Bearer token auth.

## Alembic Migrations

```bash
# Run all migrations
uv run alembic upgrade head

# Check current revision
uv run alembic current

# Create a new migration
uv run alembic revision -m "description"
```

| Revision | Description |
|----------|-------------|
| 001 | Initial tables (reports, job_queue) |
| 002 | Add pipeline fields (address, raw_data, result_json, pdf_path) |
| 003 | Add payments table, payment_id FK on reports |
