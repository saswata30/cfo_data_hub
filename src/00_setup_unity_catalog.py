# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Unity Catalog setup
# MAGIC Creates the catalog, the medallion + hub-and-spoke schemas, and the landing
# MAGIC Volume. This is the **Data Security & Governance – Unity Catalog** foundation
# MAGIC that spans every layer of the architecture.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

show_header("00 · Unity Catalog setup")

# Create the catalog only if it isn't already there. On locked-down metastores you may
# not hold CREATE CATALOG — in that case point the `catalog` widget/variable at an
# existing catalog you own (you still need CREATE SCHEMA on it).
existing_catalogs = [r["catalog"] for r in spark.sql("SHOW CATALOGS").collect()]
if CATALOG in existing_catalogs:
    print(f"  using existing catalog {CATALOG}")
else:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'CFO Data Platform PoC'")
    print(f"  created catalog {CATALOG}")

for zone, schema in SCHEMAS.items():
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} "
        f"COMMENT 'CFO Data Architecture zone: {zone}'"
    )
    print(f"  schema ready: {CATALOG}.{schema}")

# Landing Volume simulates the source-system extract drop zone feeding the LZ.
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMAS['lz_raw']}.{LANDING_VOLUME} "
    f"COMMENT 'Source-system raw extract landing area'"
)
print(f"  volume ready:  /Volumes/{CATALOG}/{SCHEMAS['lz_raw']}/{LANDING_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Governance groups (informational)
# MAGIC Column masks in `10_governance_masking` unmask PII/KYC only for members of the
# MAGIC groups below. Create them once in the workspace (account admin) — they are not
# MAGIC created here because group management is an account-level operation.

# COMMAND ----------

print("Expected UC groups for unmasked access:")
print(f"  - {PII_READER_GROUP}  (may read unmasked PII)")
print(f"  - {KYC_READER_GROUP}  (may read unmasked KYC identifiers)")
print("\nUnity Catalog setup complete.")
