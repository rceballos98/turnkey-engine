from fastapi import FastAPI

from app.routes import health, reports

app = FastAPI(title="Turnkey Engine API")

app.include_router(health.router, tags=["health"])
app.include_router(reports.router, tags=["reports"])
