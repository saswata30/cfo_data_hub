# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Raw Data (LZ) — Bronze
# MAGIC Ingests the landed source extracts **as-is** into Delta tables in `lz_raw`,
# MAGIC one table per business entity (unioned across the source systems that emit it),
# MAGIC adding ingestion lineage metadata. This is the **Raw Data (LZ)** column.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

# entity  ->  source systems that land it (must match 01_generate_synthetic_sources)
BRONZE_TABLES = {
    "party":                ["us_genius", "swiss_genius"],
    "producer":             ["wins"],
    "quote":                ["us_genius", "swiss_genius", "wins"],
    "policy":               ["procede"],
    "policy_fee":           ["guidewire_billing"],
    "billing_transaction":  ["guidewire_billing"],
    "claim":                ["ship"],
    "fnol":                 ["ship"],
    "reinsurance_contract": ["alt_capital", "rdu", "cash"],
    "headcount":            ["myhr"],
    "plan_forecast":        ["anaplan"],
    "reserve_factor":       ["copernic"],
    "conformance_rule":     ["conformance"],
}

show_header("02 · Landing Zone (Bronze)")

for entity, sources in BRONZE_TABLES.items():
    paths = [f"{landing_path(s)}/{entity}" for s in sources]
    df = (spark.read.parquet(*paths)
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_bronze_batch", F.lit(NOW_BATCH))
          .withColumn("_source_file", F.col("_metadata.file_path")))
    target = fq("lz_raw", entity)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"  bronze {entity:22s} rows={df.count():>7}  -> {target}")

print("\nLanding Zone (Bronze) load complete.")
