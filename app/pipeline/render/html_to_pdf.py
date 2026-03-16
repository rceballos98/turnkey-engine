"""Convert rendered HTML to PDF bytes via Browserless.io headless Chrome."""

from __future__ import annotations

import json
import logging

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


async def html_to_pdf(html: str) -> tuple[bytes, str]:
    """Convert HTML to PDF. Returns (file_bytes, extension) where extension is '.pdf' or '.html'."""
    browserless_url = settings.browserless_url
    browserless_token = settings.browserless_token

    if browserless_url and browserless_token:
        endpoint = f"{browserless_url.rstrip('/')}/pdf?token={browserless_token}"
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
            resp = await client.post(
                endpoint,
                headers={"Content-Type": "application/json"},
                content=json.dumps({
                    "html": html,
                    "options": {
                        "printBackground": True,
                        "format": "Letter",
                        "margin": {"top": "0", "right": "0", "bottom": "0", "left": "0"},
                    },
                }),
            )

            if not resp.is_success:
                body = resp.text[:200]
                raise RuntimeError(f"[HTML-TO-PDF] Browserless returned {resp.status_code}: {body}")

            logger.info("[HTML-TO-PDF] PDF generated via Browserless: %d bytes", len(resp.content))
            return resp.content, ".pdf"

    # Fallback: save as HTML
    logger.warning("[HTML-TO-PDF] No Browserless service configured. Storing rendered HTML as fallback.")
    return html.encode("utf-8"), ".html"
