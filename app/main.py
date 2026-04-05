from fastapi import FastAPI

from app.routes import health, reports, checkout, webhooks

app = FastAPI(title="Turnkey Engine API")

app.include_router(health.router, tags=["health"])
app.include_router(checkout.router, tags=["checkout"])
app.include_router(webhooks.router, tags=["webhooks"])
app.include_router(reports.router, tags=["reports"])
