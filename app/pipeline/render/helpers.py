"""Formatting helpers for the report renderer."""

from __future__ import annotations


def fmt_p(n: int | float | None) -> str:
    if n is None or n == 0:
        return "N/A"
    return f"${n:,.0f}"


def fmt_num(n: int | float | None) -> str:
    if n is None:
        return "N/A"
    return f"{n:,.0f}"


def fmt_date(s: str | None) -> str | None:
    if not s:
        return None
    return s[:10]


def fmt_short(n: int | float | None) -> str:
    if n is None or n == 0:
        return "N/A"
    if n >= 1_000_000:
        v = n / 1_000_000
        return f"${v:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"${round(n / 1_000)}K"
    return f"${n:,.0f}"


def fmt_ppsf(n: int | float | None) -> str:
    if n is None or n == 0:
        return "N/A"
    return f"${n:,.0f}/sqft"


def fmt_pct(n: int | float | None) -> str:
    if n is None:
        return "N/A"
    sign = "+" if n > 0 else ""
    return f"{sign}{round(n)}%"


def fmt_monthly(n: int | float | None) -> str:
    if n is None or n == 0:
        return "N/A"
    return f"${n:,.0f}/mo"


def viol_color(n: int) -> str:
    return "red" if n > 0 else "green"


def escape_html(s: str | None) -> str:
    if not s:
        return ""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
