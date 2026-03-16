# Architecture

## Overview

TurnKey Engine is the backend service for generating NYC property due diligence reports. It fetches data from 30+ public sources, transforms it, generates AI narratives via Claude, renders an HTML report, and converts it to PDF.

Reports can be requested through multiple channels (voice, SMS, email, web) — each channel is a thin adapter that calls the engine's API.

## System Design

```
Channels (Supabase Edge Functions)        Backend (Render)
──────────────────────────────────        ──────────────────────────────

Vapi (voice/SMS)  ──┐                    ┌─────────────────────────┐
Email (Resend)    ──┤                    │  turnkey-engine-api     │
Web Chat          ──┼──────────────────► │                         │
Web App           ──┤                    │  POST /checkout         │
Direct API        ──┘                    │  POST /reports          │
                                         │  GET  /reports/{id}     │
                                         │  POST /webhook/stripe   │
                                         └────────────┬────────────┘
                                                      │
                                         ┌────────────▼────────────┐
                                         │  turnkey-engine-worker  │
                                         │                         │
                                         │  1. Fetch 30+ sources   │
                                         │  2. Transform raw data  │
                                         │  3. AI narratives       │
                                         │  4. Render HTML → PDF   │
                                         │  5. Deliver (email/SMS) │
                                         └─────────────────────────┘

Storage: Render Postgres + S3 (PDF files)
```

## Responsibilities

### turnkey-engine (Render) — the brain

- **Report generation pipeline**: fetch → transform → AI → render → PDF
- **Payment**: Stripe Checkout session creation + webhook handling
- **Storage**: report metadata in Postgres, PDFs in S3/Supabase Storage
- **Delivery**: send completed reports via email (Resend) or SMS link
- **API**: the single interface everything calls

### Supabase Edge Functions — thin channel adapters

These stay lightweight. They translate "user said X on channel Y" into API calls.

- **handle-voice-request**: Vapi webhook → extracts address → `POST /checkout`
- **handle-inbound-email**: Resend webhook → extracts address via Claude Haiku → `POST /checkout`
- **chat-proxy**: forwards web chat to Vapi API, keeps API key server-side

The Vapi assistant, ElevenLabs voice config, and Resend webhook URLs stay pointed at Supabase. No reason to move them at MVP.

## Report Pipeline

```
POST /reports { address, unit? }
  │
  ├─ Create report row (status=queued)
  ├─ Enqueue job
  │
  ▼ (worker picks up job)
  │
  ├─ Phase 1: Foundation (sequential)
  │   └─ Address → BBL/BIN via PLUTO, listing via Firecrawl
  │
  ├─ Phase 2: Parallel batch (Promise.allSettled equivalent)
  │   └─ DOB, HPD, FDNY, ACRIS, sales, 311, NYPD, FEMA, etc.
  │
  ├─ Phase 3: Chained enrichment
  │   └─ ACRIS details, HPD contacts, DOF financials
  │
  ├─ Transform: raw API responses → normalized data (100+ fields)
  │
  ├─ AI Narratives: Claude Sonnet → 12 narrative sections
  │
  ├─ Render: Jinja2 HTML template (146 placeholders)
  │
  ├─ PDF: Browserless.io (headless Chrome)
  │
  └─ Update report status=completed, store PDF
```

## Payment Flow (MVP)

No accounts. No login. Always issue a Stripe Payment Link.

```
1. User requests report (any channel)

2. POST /checkout { email, address }
   → Creates report row (status=pending_payment)
   → Creates Stripe Checkout Session (report_id in metadata)
   → Returns payment URL

3. Channel adapter sends link to user
   (Vapi reads URL / email includes link / web redirects)

4. User pays → Stripe webhook → POST /webhook/stripe
   → Verifies payment signature
   → Updates report status → queued
   → Enqueues job for worker

5. Worker generates report

6. Delivery based on channel:
   - Email → Resend with PDF attachment
   - Voice/SMS → SMS with download link
   - Web → frontend polls GET /reports/{id}
```

## Data Sources

30+ NYC public APIs organized by category:

| Category | Sources | Key Data |
|----------|---------|----------|
| Property | PLUTO | BBL, year built, zoning, assessed value |
| Violations | DOB, HPD, FDNY, ECB | Type, status, dates, penalties |
| Sales | DOF, ACRIS | Price history, ownership, mortgages |
| Listings | StreetEasy (Firecrawl) | Comps, price/sqft, DOM |
| Building Systems | DOB BIS | Elevators, boilers, inspections |
| Neighborhood | 311, NYPD, FEMA | Complaints, crime, flood zones |
| Environmental | NYS DEC | Contaminated sites |

Fetching is resilient: rate-limited per provider, retries with backoff, hard 180s timeout. Non-critical source failures don't block the pipeline.

## API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | No | Health check |
| `POST` | `/checkout` | No | Create payment session, returns Stripe URL |
| `POST` | `/reports` | Bearer | Create report (internal/paid) |
| `GET` | `/reports/{id}` | Bearer | Get report status + result |
| `POST` | `/webhook/stripe` | Stripe sig | Payment confirmation |

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API | FastAPI + Gunicorn |
| Worker | Python polling loop |
| Database | Postgres (Render) |
| Migrations | Alembic |
| AI | Claude Sonnet (narratives), Claude Haiku (address extraction) |
| PDF | Browserless.io |
| Payments | Stripe Checkout |
| Email | Resend |
| Voice/SMS | Vapi + Twilio |
| Channel Adapters | Supabase Edge Functions (Deno) |

## Build Phases

### Phase 1 — Report engine (end-to-end)
- Port fetch orchestrator + data source contracts from hello-world-site
- Port transform layer
- Wire Claude for AI narratives
- Port HTML template + Jinja2 rendering
- Browserless.io PDF generation
- `POST /reports` with API key → real report

### Phase 2 — Payments
- Stripe Checkout integration (`POST /checkout`)
- Webhook handler (verify + enqueue)
- Report status: `pending_payment → queued → processing → completed`

### Phase 3 — Channel adapters
- Update Supabase Edge Functions to call turnkey-engine API
- Vapi tools → `POST /checkout` on turnkey-engine
- Email handler → `POST /checkout` on turnkey-engine
- Include `callback_channel` + `callback_address` for delivery routing

### Future
- User accounts + saved payment methods
- Report caching (dedup by BBL + unit)
- Webhook notifications on report completion
- Rate limiting per API key
