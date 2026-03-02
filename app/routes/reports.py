from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from app.auth import verify_api_key
from app.database import get_db
from app.models import Report, JobQueue

router = APIRouter()


class ReportRequest(BaseModel):
    query: str


class ReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: str
    query: str
    result: str | None
    error: str | None


@router.post("/reports", response_model=ReportResponse, status_code=status.HTTP_201_CREATED)
def create_report(
    request: ReportRequest,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    report = Report(query=request.query, status="queued")
    db.add(report)
    db.flush()

    job = JobQueue(report_id=report.id, status="pending")
    db.add(job)
    db.commit()
    db.refresh(report)

    return report


@router.get("/reports/{report_id}", response_model=ReportResponse)
def get_report(
    report_id: UUID,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    report = db.query(Report).filter(Report.id == report_id).first()
    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Report not found",
        )
    return report
