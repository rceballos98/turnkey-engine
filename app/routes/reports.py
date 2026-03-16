from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from app.auth import verify_api_key
from app.database import get_db
from app.models import Report, JobQueue

router = APIRouter()


class ReportRequest(BaseModel):
    address: str


class ReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: str
    address: str
    raw_data: dict | None = None
    result_json: dict | None = None
    pdf_path: str | None = None
    error: str | None = None


@router.post("/reports", response_model=ReportResponse, status_code=status.HTTP_201_CREATED)
def create_report(
    request: ReportRequest,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    report = Report(address=request.address, status="queued")
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


@router.get("/reports/{report_id}/pdf")
def get_report_pdf(
    report_id: UUID,
    db: Session = Depends(get_db),
    _: None = Depends(verify_api_key),
):
    report = db.query(Report).filter(Report.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    if not report.pdf_path:
        raise HTTPException(status_code=404, detail="Report not yet generated")

    report_file = Path(report.pdf_path)
    if not report_file.exists():
        raise HTTPException(status_code=404, detail="Report file not found on disk")

    is_html = report_file.suffix == ".html"
    media_type = "text/html" if is_html else "application/pdf"
    filename = f"report-{report_id}{report_file.suffix}"

    return FileResponse(
        path=str(report_file),
        media_type=media_type,
        filename=filename,
    )
