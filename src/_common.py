# Databricks notebook source
# MAGIC %md
# MAGIC # _common — shared configuration & helpers
# MAGIC Included by every pipeline notebook via `%run ./_common`.
# MAGIC Centralises the catalog name, the medallion / hub-and-spoke schema names,
# MAGIC and small helper functions so the rest of the code stays declarative.

# COMMAND ----------

dbutils.widgets.text("catalog", "cfo_poc", "Unity Catalog catalog")
CATALOG = dbutils.widgets.get("catalog")

# Architecture zone  ->  Unity Catalog schema
SCHEMAS = {
    "lz_raw":       "lz_raw",        # Raw Data (Landing Zone)            -> Bronze
    "oz_organized": "oz_organized",  # Organized Zone (conformed)         -> Silver
    "hub":          "hub",           # HUB Primary: master data / KYC
    "underwriting": "underwriting",  # Spoke: Risk, Quotes
    "policy":       "policy",        # Spoke: Policies, Fees
    "claims":       "claims",        # Spoke: FNOL, Claims
    "efr":          "efr",           # Enterprise Finance Reporting (GL, FAH)  -> Semantic
    "reporting":    "reporting",     # CFO Reporting marts (Gold serving)
}

LANDING_VOLUME = "landing"          # Volume under lz_raw simulating source-system extracts

# Governance principals allowed to read unmasked PII / KYC.
PII_READER_GROUP = "cfo_pii_readers"
KYC_READER_GROUP = "cfo_kyc_readers"

# Synthetic-data scale (rows).
SCALE = {
    "parties": 5000, "producers": 400, "quotes": 12000, "policies": 8000,
    "policy_fees": 16000, "claims": 6000, "reinsurance": 600, "headcount": 1200,
    "seed": 42,
}

# The source systems from the CFO Data Sources column of the architecture.
SOURCE_SYSTEMS = [
    "us_genius", "procede", "wins", "guidewire_billing", "swiss_genius",
    "anaplan", "copernic", "myhr", "ship", "alt_capital", "rdu", "cash", "conformance",
]

# Batch id stamped onto bronze rows for lineage.
import datetime as _dt
NOW_BATCH = _dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")


def fq(zone: str, table: str) -> str:
    """Fully-qualified `catalog.schema.table` for an architecture zone."""
    return f"{CATALOG}.{SCHEMAS[zone]}.{table}"


def landing_path(source: str) -> str:
    """UC Volume path where a source system drops its raw extract files."""
    return f"/Volumes/{CATALOG}/{SCHEMAS['lz_raw']}/{LANDING_VOLUME}/{source}"


def show_header(title: str):
    print("=" * 78)
    print(f"  {title}")
    print(f"  catalog = {CATALOG}")
    print("=" * 78)
