"""Socrata Contract Registry — all Socrata API endpoints, query patterns, and field dependencies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

QueryType = Literal[
    "bbl", "bin", "boro_block_lot", "parid", "geo",
    "dashed_bbl", "address", "registration_id", "document_id",
]

SOCRATA_DEFAULTS = {
    "timeout_ms": 15_000,
    "max_retries": 2,
    "retry_on": [429, 500, 502, 503],
    "rate_limit_key": "socrata",
}


@dataclass
class SocrataContract:
    name: str
    dataset_id: str
    endpoint: str
    query_type: QueryType
    where_template: str
    sample_query: str
    used_fields: list[str] = field(default_factory=list)
    order_by: str | None = None
    limit: int | None = None
    pad_block: int | None = None
    pad_lot: int | None = None
    custom: bool = False
    sparse_fields: list[str] | None = None
    timeout_ms: int = 15_000
    max_retries: int = 2
    retry_on: list[int] = field(default_factory=lambda: [429, 500, 502, 503])
    rate_limit_key: str = "socrata"
    critical: bool = False


SOCRATA_CONTRACTS: list[SocrataContract] = [
    SocrataContract(
        name="pluto", dataset_id="64uk-42ks",
        endpoint="https://data.cityofnewyork.us/resource/64uk-42ks.json",
        query_type="address",
        where_template="address LIKE '{houseNumber} {street}%' AND borough='{borough}'",
        sample_query="$where=bbl='1001347502'&$limit=1",
        used_fields=["address", "borough", "zipcode", "bbl", "block", "lot", "latitude", "longitude",
                     "yearbuilt", "numfloors", "unitsres", "unitstotal", "bldgclass", "landuse",
                     "lotarea", "assesstot", "bldgarea", "ownername", "borocode", "zonedist1",
                     "residfar", "builtfar", "yearalter1", "yearalter2"],
        custom=True, critical=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dobViolations", dataset_id="3h2n-5cm9",
        endpoint="https://data.cityofnewyork.us/resource/3h2n-5cm9.json",
        query_type="boro_block_lot",
        where_template="boro='{boro}' AND block='{block}' AND lot='{lot}'",
        sample_query="$where=boro='1' AND block='00134' AND lot='07502'&$limit=1",
        used_fields=["boro", "block", "lot", "issue_date", "isn_dob_bis_viol",
                     "violation_category", "violation_number", "violation_type", "violation_type_code"],
        sparse_fields=["description", "disposition_date", "disposition_comments"],
        order_by="issue_date DESC", limit=200, pad_block=5, pad_lot=5, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dobJobs", dataset_id="ic3t-wcy2",
        endpoint="https://data.cityofnewyork.us/resource/ic3t-wcy2.json",
        query_type="bin",
        where_template="bin__='{bin}'",
        sample_query="$where=bin__='1001389'&$limit=1",
        used_fields=["bin__", "house__", "street_name", "job_type", "job_status_descrp",
                     "job_status", "pre__filing_date", "latest_action_date"],
        sparse_fields=["job_description"],
        order_by="pre__filing_date DESC", limit=100, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dobEcb", dataset_id="6bgk-3dad",
        endpoint="https://data.cityofnewyork.us/resource/6bgk-3dad.json",
        query_type="bin",
        where_template="bin='{bin}'",
        sample_query="$where=bin='1001389'&$limit=1",
        used_fields=["bin", "ecb_violation_number", "ecb_violation_status",
                     "violation_type", "violation_description", "issue_date",
                     "served_date", "balance_due", "penality_imposed"],
        order_by="issue_date DESC", limit=200, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dobComplaints", dataset_id="eabe-havv",
        endpoint="https://data.cityofnewyork.us/resource/eabe-havv.json",
        query_type="bin",
        where_template="bin='{bin}'",
        sample_query="$where=bin='1001389'&$limit=1",
        used_fields=["bin", "date_entered", "complaint_category", "status"],
        order_by="date_entered DESC", limit=200, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dobNowPermits", dataset_id="rbx6-tga4",
        endpoint="https://data.cityofnewyork.us/resource/rbx6-tga4.json",
        query_type="bbl",
        where_template="bbl='{bbl}'",
        sample_query="$where=bbl='1001347502'&$limit=1",
        used_fields=["bbl", "job_filing_number", "filing_reason", "work_type", "permit_status"],
        sparse_fields=["issued_date"],
        order_by="issued_date DESC", limit=100, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="hpdViolations", dataset_id="wvxf-dwi5",
        endpoint="https://data.cityofnewyork.us/resource/wvxf-dwi5.json",
        query_type="boro_block_lot",
        where_template="boroid='{boro}' AND block='{block}' AND lot='{lot}'",
        sample_query="$where=boroid='1' AND block='00134' AND lot='7502'&$limit=1",
        used_fields=["boroid", "block", "lot", "inspectiondate", "currentstatus"],
        order_by="inspectiondate DESC", limit=200, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="hpdComplaints", dataset_id="ygpa-z7cr",
        endpoint="https://data.cityofnewyork.us/resource/ygpa-z7cr.json",
        query_type="bbl",
        where_template="bbl='{bbl}'",
        sample_query="$where=bbl='1001347502'&$limit=1",
        used_fields=["bbl", "received_date", "major_category", "minor_category"],
        order_by="received_date DESC", limit=200, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="hpdLitigation", dataset_id="59kj-x8nc",
        endpoint="https://data.cityofnewyork.us/resource/59kj-x8nc.json",
        query_type="bbl",
        where_template="bbl='{bbl}'",
        sample_query="$where=bbl='1001347502'&$limit=1",
        used_fields=["bbl", "caseopendate", "casestatus"],
        order_by="caseopendate DESC", limit=100, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="hpdRegistration", dataset_id="tesw-yqqr",
        endpoint="https://data.cityofnewyork.us/resource/tesw-yqqr.json",
        query_type="bin",
        where_template="bin='{bin}'",
        sample_query="$where=bin='1001389'&$limit=1",
        used_fields=["bin", "registrationid", "registrationenddate", "buildingid"],
        limit=1, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="hpdRegContacts", dataset_id="feu5-w2e2",
        endpoint="https://data.cityofnewyork.us/resource/feu5-w2e2.json",
        query_type="registration_id",
        where_template="registrationid='{registrationId}'",
        sample_query="$limit=1",
        used_fields=["registrationid", "type", "corporationname"],
        sparse_fields=["firstname", "lastname"],
        limit=50, custom=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="elevatorData", dataset_id="e5aq-a4j2",
        endpoint="https://data.cityofnewyork.us/resource/e5aq-a4j2.json",
        query_type="bbl",
        where_template="bbl='{bbl}'",
        sample_query="$limit=1",
        sparse_fields=["bbl", "block", "lot", "borough", "device_number",
                       "device_status", "device_type", "status_date"],
        limit=200, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="boilerData", dataset_id="52dp-yji6",
        endpoint="https://data.cityofnewyork.us/resource/52dp-yji6.json",
        query_type="bin",
        where_template="bin_number='{bin}'",
        sample_query="$limit=1",
        used_fields=["bin_number"],
        limit=50, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="nycSales", dataset_id="w2pb-icbu",
        endpoint="https://data.cityofnewyork.us/resource/w2pb-icbu.json",
        query_type="boro_block_lot",
        where_template="borough='{boro}' AND block='{block}' AND sale_price > '100000'",
        sample_query="$where=borough='1' AND block='00134' AND sale_price > '100000'&$limit=1",
        used_fields=["borough", "block", "lot", "zip_code", "sale_price", "sale_date",
                     "address", "building_class_category", "building_class_at_time_of",
                     "neighborhood", "bin"],
        order_by="sale_date DESC", limit=100, custom=True, pad_block=5, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="acrisLegals", dataset_id="8h5j-fqxa",
        endpoint="https://data.cityofnewyork.us/resource/8h5j-fqxa.json",
        query_type="boro_block_lot",
        where_template="borough='{boro}' AND block='{block}' AND lot='{lot}'",
        sample_query="$where=borough='1' AND block='134'&$limit=1",
        used_fields=["borough", "block", "lot", "document_id",
                     "good_through_date", "street_number", "street_name",
                     "property_type", "unit"],
        order_by="good_through_date DESC", limit=200, custom=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="acrisMaster", dataset_id="bnx9-e6tj",
        endpoint="https://data.cityofnewyork.us/resource/bnx9-e6tj.json",
        query_type="document_id",
        where_template="document_id in({documentIds})",
        sample_query="$where=document_id='2024010500478001'&$limit=1",
        used_fields=["document_id", "doc_type", "recorded_datetime", "document_amt"],
        limit=200, custom=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="acrisParties", dataset_id="636b-3b5g",
        endpoint="https://data.cityofnewyork.us/resource/636b-3b5g.json",
        query_type="document_id",
        where_template="document_id in({documentIds})",
        sample_query="$where=document_id='2024010500478001'&$limit=1",
        used_fields=["document_id", "party_type", "name"],
        limit=500, custom=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dofExemptions", dataset_id="muvi-b6kx",
        endpoint="https://data.cityofnewyork.us/resource/muvi-b6kx.json",
        query_type="parid",
        where_template="parid='{parid}'",
        sample_query="$limit=1",
        used_fields=["parid"],
        sparse_fields=["exmp_code", "exname"],
        limit=100, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dofAbatements", dataset_id="rgyu-ii48",
        endpoint="https://data.cityofnewyork.us/resource/rgyu-ii48.json",
        query_type="parid",
        where_template="parid='{parid}'",
        sample_query="$limit=1",
        used_fields=["parid"],
        limit=100, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dofCondoIncome", dataset_id="9ck6-2jew",
        endpoint="https://data.cityofnewyork.us/resource/9ck6-2jew.json",
        query_type="dashed_bbl",
        where_template="boro_block_lot='{dashedBbl}'",
        sample_query="$limit=1",
        used_fields=["boro_block_lot", "report_year", "address", "neighborhood",
                     "building_classification", "total_units", "year_built", "gross_sqft",
                     "estimated_gross_income", "gross_income_per_sqft",
                     "estimated_expense", "expense_per_sqft",
                     "net_operating_income", "full_market_value", "market_value_per_sqft"],
        order_by="report_year DESC", limit=5, custom=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="dofCoopIncome", dataset_id="myei-c3fa",
        endpoint="https://data.cityofnewyork.us/resource/myei-c3fa.json",
        query_type="dashed_bbl",
        where_template="boro_block_lot='{dashedBbl}'",
        sample_query="$limit=1",
        used_fields=["boro_block_lot", "report_year", "address", "neighborhood",
                     "building_classification", "total_units", "year_built", "gross_sqft",
                     "estimated_gross_income", "gross_income_per_sqft",
                     "estimated_expense", "expense_per_sqft",
                     "net_operating_income", "full_market_value", "market_value_per_sqft"],
        order_by="report_year DESC", limit=5, custom=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="taxLienSales", dataset_id="9rz4-mjek",
        endpoint="https://data.cityofnewyork.us/resource/9rz4-mjek.json",
        query_type="boro_block_lot",
        where_template="borough='{boro}' AND block='{block}' AND lot='{lot}'",
        sample_query="$limit=1",
        used_fields=["borough", "block", "lot"],
        limit=50, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="certOccupancy", dataset_id="bs8b-p36w",
        endpoint="https://data.cityofnewyork.us/resource/bs8b-p36w.json",
        query_type="bbl",
        where_template="bbl='{bbl}'",
        sample_query="$limit=1",
        used_fields=["bbl", "c_o_issue_date"],
        order_by="c_o_issue_date DESC", limit=50, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="sr311", dataset_id="erm2-nwe9",
        endpoint="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
        query_type="bbl",
        where_template="bbl='{bbl}'",
        sample_query="$where=bbl='1001347502'&$limit=1",
        used_fields=["created_date", "complaint_type"],
        sparse_fields=["bbl", "location", "latitude", "longitude", "closed_date"],
        order_by="created_date DESC", limit=200, custom=True, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="nypdCrime", dataset_id="5uac-w243",
        endpoint="https://data.cityofnewyork.us/resource/5uac-w243.json",
        query_type="geo",
        where_template="within_circle(lat_lon,{lat},{lon},{radius})",
        sample_query="$limit=1",
        used_fields=["cmplnt_fr_dt", "law_cat_cd", "ofns_desc", "lat_lon"],
        order_by="cmplnt_fr_dt DESC", limit=200, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="energyBenchmarking", dataset_id="5zyy-y8am",
        endpoint="https://data.cityofnewyork.us/resource/5zyy-y8am.json",
        query_type="bbl",
        where_template="nyc_borough_block_and_lot='{bbl}'",
        sample_query="$limit=1",
        used_fields=["nyc_borough_block_and_lot"],
        limit=1, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="landmarks", dataset_id="gpmc-yuvp",
        endpoint="https://data.cityofnewyork.us/resource/gpmc-yuvp.json",
        query_type="bbl",
        where_template="bbl='{bbl}'",
        sample_query="$limit=1",
        used_fields=["bbl"],
        limit=1, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="fdnyViolations", dataset_id="ktas-47y7",
        endpoint="https://data.cityofnewyork.us/resource/ktas-47y7.json",
        query_type="boro_block_lot",
        where_template="violation_location_borough='{boroFullName}' AND violation_location_block_no='{block}' AND violation_location_lot_no='{lot}'",
        sample_query="$where=violation_location_borough='MANHATTAN' AND violation_location_block_no='00134'&$limit=1",
        used_fields=["violation_location_borough", "violation_location_block_no",
                     "violation_location_lot_no", "balance_due"],
        limit=100, pad_block=5, pad_lot=4, **SOCRATA_DEFAULTS,
    ),
    SocrataContract(
        name="envSites", dataset_id="c6ci-rzpg",
        endpoint="https://data.ny.gov/resource/c6ci-rzpg.json",
        query_type="geo",
        where_template="within_circle(georeference,{lat},{lon},{radius})",
        sample_query="$limit=1",
        used_fields=["georeference"],
        sparse_fields=["program_facility_name", "latitude", "longitude"],
        limit=20, **SOCRATA_DEFAULTS,
    ),
]


def get_contract(name: str) -> SocrataContract | None:
    for c in SOCRATA_CONTRACTS:
        if c.name == name:
            return c
    return None


def get_generic_contracts() -> list[SocrataContract]:
    return [c for c in SOCRATA_CONTRACTS if not c.custom]
