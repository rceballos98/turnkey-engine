"""Full report pipeline: fetch -> transform -> AI -> render -> PDF."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from app.pipeline.orchestrator import orchestrate_fetch
from app.pipeline.transform.raw import transform_raw_data
from app.pipeline.render.ai_narratives import generate_ai_narratives
from app.pipeline.render.report_renderer import render_report
from app.pipeline.render.html_to_pdf import html_to_pdf
from app.pipeline.fetch_engine import close_client, reset_rate_limiters

logger = logging.getLogger(__name__)

TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "report.html"
PDF_DIR = Path(os.environ.get("PDF_DIR", str(Path(__file__).parent.parent.parent / "data" / "pdfs")))


async def run_report_pipeline(address: str, report_id: str) -> dict:
    """
    Run the full report pipeline.

    Returns dict with keys: raw_data, transformed, pdf_path
    """
    reset_rate_limiters()

    try:
        # Phase 1: Fetch all data
        logger.info("[PIPELINE] Starting fetch for: %s", address)
        raw_data = await orchestrate_fetch(address)

        # Phase 2: Transform
        logger.info("[PIPELINE] Transforming raw data...")
        transformed = transform_raw_data(raw_data, address)

        # Phase 3: AI Narratives
        logger.info("[PIPELINE] Generating AI narratives...")
        narratives, ai_failed = await generate_ai_narratives(transformed)
        if ai_failed:
            logger.warning("[PIPELINE] AI narratives failed — using fallback")

        # Phase 4: Render HTML
        logger.info("[PIPELINE] Rendering HTML report...")
        template = TEMPLATE_PATH.read_text(encoding="utf-8")
        html = render_report(transformed, narratives, template)

        # Phase 5: Generate PDF
        logger.info("[PIPELINE] Generating PDF...")
        file_bytes, ext = await html_to_pdf(html)

        # Save report file
        pdf_dir = PDF_DIR
        pdf_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = str(pdf_dir / f"{report_id}{ext}")
        with open(pdf_path, "wb") as f:
            f.write(file_bytes)
        logger.info("[PIPELINE] Report saved: %s (%d bytes)", pdf_path, len(file_bytes))

        # Serialize transformed data (strip large arrays for JSON storage)
        transformed_json = _prepare_for_storage(transformed)

        return {
            "raw_data": raw_data,
            "transformed": transformed_json,
            "pdf_path": pdf_path,
        }
    finally:
        await close_client()


def _prepare_for_storage(transformed: dict) -> dict:
    """Prepare transformed data for JSON storage, trimming large arrays."""
    result = {}
    for key, value in transformed.items():
        if isinstance(value, list) and len(value) > 50:
            result[key] = value[:50]
        else:
            result[key] = value
    return result
