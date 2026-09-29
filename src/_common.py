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
    "abc_control":  "abc_control",   # Audit, Balance & Control framework + Silver DQ results
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

# Batch id stamped onto bronze rows for lineage. Also used as the ABC / DQ run id
# so an ingestion batch, its balance reconciliation and its Silver DQ results all
# share one correlation key.
import datetime as _dt
NOW_BATCH = _dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")

# Audit, Balance & Control tuning (see src/_abc.py, src/_dq.py).
CONTROL_TOTAL_TOLERANCE = 0.0     # exact source-vs-raw control-total match (lossless bronze)
QUARANTINE_DQ_FAILURES = True     # route HIGH-severity Silver DQ failures out of the clean table


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


def rows_to_df(rows, schema):
    """Build a DataFrame from a list of dicts against an explicit schema.

    Maps by field name and orders columns to the schema, so columns whose values
    are entirely NULL (which break Spark's schema inference) still materialise with
    the right type. Used by the ABC and DQ frameworks to append their log tables.
    """
    names = [f.name for f in schema.fields]
    data = [tuple(r.get(n) for n in names) for r in rows]
    return spark.createDataFrame(data, schema)
