"""Resilient HTTP fetch engine. NEVER throws — always returns a result."""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from app.pipeline.types import FetchOptions, ResilientFetchResult, SourceResult

logger = logging.getLogger(__name__)

# ── Rate Limiter (semaphore per provider group) ──

RATE_LIMITS: dict[str, int] = {
    "socrata": 10,
    "firecrawl": 3,
    "dob-bis": 2,
}

_semaphores: dict[str, asyncio.Semaphore] = {}


def _get_semaphore(key: str) -> asyncio.Semaphore | None:
    limit = RATE_LIMITS.get(key)
    if not limit:
        return None
    if key not in _semaphores:
        _semaphores[key] = asyncio.Semaphore(limit)
    return _semaphores[key]


def reset_rate_limiters():
    """Reset semaphores between pipeline runs."""
    _semaphores.clear()


# ── Shared HTTP client ──

_client: httpx.AsyncClient | None = None


async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
    return _client


async def close_client():
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None


# ── resilient_fetch ──

async def resilient_fetch(opts: FetchOptions) -> ResilientFetchResult:
    """
    Resilient HTTP fetch with retry, backoff, timeout, and rate limiting.
    NEVER THROWS. Always returns a ResilientFetchResult.
    """
    total_attempts = opts.max_retries + 1
    start = time.monotonic()
    last_error = ""
    last_http_status = 0
    retry_attempts = 0

    sem = _get_semaphore(opts.rate_limit_key) if opts.rate_limit_key else None
    client = await get_client()

    for attempt in range(1, total_attempts + 1):
        try:
            if sem:
                await sem.acquire()
            try:
                timeout = httpx.Timeout(opts.timeout_ms / 1000.0, connect=10.0)
                if opts.method == "POST":
                    response = await client.post(
                        opts.url,
                        headers=opts.headers,
                        content=opts.body,
                        timeout=timeout,
                    )
                else:
                    response = await client.get(
                        opts.url,
                        headers=opts.headers,
                        timeout=timeout,
                    )

                last_http_status = response.status_code

                if response.is_success:
                    try:
                        data = response.json()
                    except Exception:
                        data = response.text
                    return ResilientFetchResult(
                        ok=True,
                        data=data,
                        http_status=response.status_code,
                        retry_attempts=attempt - 1,
                        duration_ms=int((time.monotonic() - start) * 1000),
                    )

                if response.status_code in opts.retry_on and attempt < total_attempts:
                    retry_attempts = attempt
                    last_error = f"HTTP {response.status_code}"

                    delay = (opts.retry_delay_ms / 1000.0) * (2 ** (attempt - 1))
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                parsed = int(retry_after)
                                if parsed > 0:
                                    delay = float(parsed)
                            except ValueError:
                                pass

                    logger.info(
                        "[FETCH] %s | attempt %d/%d | %d | retrying in %.1fs",
                        opts.label, attempt, total_attempts, response.status_code, delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                # Non-retryable error
                error_body = response.text[:200]
                return ResilientFetchResult(
                    ok=False,
                    data=None,
                    http_status=response.status_code,
                    retry_attempts=attempt - 1,
                    duration_ms=int((time.monotonic() - start) * 1000),
                    error=f"HTTP {response.status_code}: {error_body}",
                )
            finally:
                if sem:
                    sem.release()

        except Exception as err:
            last_error = str(err)
            retry_attempts = attempt - 1
            if attempt < total_attempts:
                delay = (opts.retry_delay_ms / 1000.0) * (2 ** (attempt - 1))
                logger.info(
                    "[FETCH] %s | attempt %d/%d | %s | retrying in %.1fs",
                    opts.label, attempt, total_attempts, last_error[:100], delay,
                )
                await asyncio.sleep(delay)
                continue

    return ResilientFetchResult(
        ok=False,
        data=None,
        http_status=last_http_status,
        retry_attempts=retry_attempts,
        duration_ms=int((time.monotonic() - start) * 1000),
        error=last_error or "All retry attempts exhausted",
    )


def to_source_result(name: str, fetch_result: ResilientFetchResult) -> SourceResult:
    if not fetch_result.ok:
        return SourceResult(
            name=name,
            status="failed",
            data=[] if isinstance(fetch_result.data, list) else None,
            record_count=0,
            duration_ms=fetch_result.duration_ms,
            error=fetch_result.error,
            retry_attempts=fetch_result.retry_attempts,
            http_status=fetch_result.http_status,
        )

    data = fetch_result.data
    record_count = len(data) if isinstance(data, list) else (1 if data else 0)

    return SourceResult(
        name=name,
        status="ok",
        data=data,
        record_count=record_count,
        duration_ms=fetch_result.duration_ms,
        retry_attempts=fetch_result.retry_attempts,
        http_status=fetch_result.http_status,
    )
