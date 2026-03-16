"""Non-Socrata data source contracts: FEMA, DOB BIS, Firecrawl, Zoning, Claude AI."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DataSourceContract:
    name: str
    provider: str
    endpoint: str
    timeout_ms: int
    max_retries: int
    retry_on: list[int] = field(default_factory=list)
    rate_limit_key: str = ""
    critical: bool = False
    custom: bool = True


NON_SOCRATA_CONTRACTS: list[DataSourceContract] = [
    DataSourceContract(
        name="dobBISProfile", provider="dob-bis",
        endpoint="https://a810-bisweb.nyc.gov/bisweb/PropertyProfileOverviewServlet",
        timeout_ms=20_000, max_retries=3, retry_on=[429, 500, 502, 503], rate_limit_key="dob-bis",
    ),
    DataSourceContract(
        name="dobBISBoilers", provider="dob-bis",
        endpoint="https://a810-bisweb.nyc.gov/bisweb/BoilerSearchServlet",
        timeout_ms=20_000, max_retries=3, retry_on=[429, 500, 502, 503], rate_limit_key="dob-bis",
    ),
    DataSourceContract(
        name="dobBISElevator", provider="dob-bis",
        endpoint="https://a810-bisweb.nyc.gov/bisweb/ElevatorSearchServlet",
        timeout_ms=20_000, max_retries=3, retry_on=[429, 500, 502, 503], rate_limit_key="dob-bis",
    ),
    DataSourceContract(
        name="firecrawlSearch", provider="firecrawl",
        endpoint="https://api.firecrawl.dev/v1/search",
        timeout_ms=30_000, max_retries=1, retry_on=[429], rate_limit_key="firecrawl",
    ),
    DataSourceContract(
        name="firecrawlScrape", provider="firecrawl",
        endpoint="https://api.firecrawl.dev/v1/scrape",
        timeout_ms=30_000, max_retries=1, retry_on=[429], rate_limit_key="firecrawl",
    ),
    DataSourceContract(
        name="floodZone", provider="fema",
        endpoint="https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query",
        timeout_ms=15_000, max_retries=1, retry_on=[500, 502, 503],
    ),
    DataSourceContract(
        name="zoningDistricts", provider="nyc-zoning",
        endpoint="https://zoning-api.nycplanningdigital.com/api/tax-lots",
        timeout_ms=10_000, max_retries=1, retry_on=[500, 502, 503],
    ),
    DataSourceContract(
        name="claudeHaiku", provider="claude",
        endpoint="https://api.anthropic.com/v1/messages",
        timeout_ms=45_000, max_retries=1, retry_on=[500, 529],
    ),
    DataSourceContract(
        name="claudeSonnet", provider="claude",
        endpoint="https://api.anthropic.com/v1/messages",
        timeout_ms=60_000, max_retries=1, retry_on=[500, 529],
    ),
]


def get_non_socrata_contract(name: str) -> DataSourceContract | None:
    for c in NON_SOCRATA_CONTRACTS:
        if c.name == name:
            return c
    return None
