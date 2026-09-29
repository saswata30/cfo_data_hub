# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Raw Data (LZ) — Bronze, with Audit, Balance & Control (ABC)
# MAGIC Ingests the landed source extracts **as-is** into Delta tables in `lz_raw`,
# MAGIC one table per business entity (unioned across the source systems that emit it),
# MAGIC adding ingestion lineage metadata. This is the **Raw Data (LZ)** column.
# MAGIC
# MAGIC Every entity is wrapped by the **ABC framework** (`_abc`): before/after the
# MAGIC load it measures the landed source and the raw target, reconciles them
# MAGIC (row-count + numeric control-total **balance**), writes an append-only
# MAGIC **audit** row, logs any **control** breach, and finally gates the run — a
# MAGIC hard-control breach (nothing landed, or a row-count imbalance) fails the task.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %run ./_abc

# COMMAND ----------

import datetime as _dt
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

# Numeric column reconciled as the ABC "control total" (source SUM must equal raw
# SUM). None => the entity is balanced on row count only.
CONTROL_MEASURE = {
    "party":                None,
    "producer":             "commission_rate",
    "quote":                "premium_quoted",
    "policy":               "gross_written_premium",
    "policy_fee":           "amount",
    "billing_transaction":  "amount",
    "claim":                "incurred_amount",
    "fnol":                 None,
    "reinsurance_contract": "ceded_premium",
    "headcount":            "annual_cost",
    "plan_forecast":        "planned_amount",
    "reserve_factor":       None,
    "conformance_rule":     None,
}

show_header("02 · Raw Data (LZ / Bronze) + ABC controls")
abc_ensure_tables()

audit_rows, exception_rows = [], []

for entity, sources in BRONZE_TABLES.items():
    started = _dt.datetime.utcnow()
    paths = [f"{landing_path(s)}/{entity}" for s in sources]
    measure = CONTROL_MEASURE.get(entity)
    src = spark.read.parquet(*paths)

    # --- Audit/Balance: measure the SOURCE (landed extract files) ---------
    src_aggs = [F.count(F.lit(1)).alias("_n"),
                F.countDistinct(F.col("_metadata.file_path")).alias("_files")]
    if measure:
        src_aggs.append(F.sum(F.col(measure).cast("double")).alias("_ctl"))
    s = src.agg(*src_aggs).collect()[0]
    source_count, file_count = s["_n"], s["_files"]
    source_total = float(s["_ctl"]) if measure and s["_ctl"] is not None else None

    # --- Ingest to Bronze as-is + ingestion lineage (incl. ABC run id) ----
    df = (src.withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_bronze_batch", F.lit(NOW_BATCH))
             .withColumn("_source_file", F.col("_metadata.file_path"))
             .withColumn("_abc_run_id", F.lit(NOW_BATCH)))
    target = fq("lz_raw", entity)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)

    # --- Balance: measure the TARGET (raw Delta) --------------------------
    tgt_aggs = [F.count(F.lit(1)).alias("_n")]
    if measure:
        tgt_aggs.append(F.sum(F.col(measure).cast("double")).alias("_ctl"))
    t = spark.table(target).agg(*tgt_aggs).collect()[0]
    target_count = t["_n"]
    target_total = float(t["_ctl"]) if measure and t["_ctl"] is not None else None

    # --- Control: evaluate & accumulate -----------------------------------
    audit, excs = abc_evaluate(
        entity, sources, paths, file_count, source_count, target_count,
        control_column=measure, source_control_total=source_total,
        target_control_total=target_total, started_at=started)
    audit_rows.append(audit)
    exception_rows.extend(excs)

    tag = {"SUCCESS": "ok  ", "WARN": "warn", "FAILED": "FAIL"}[audit["status"]]
    print(f"  [{tag}] {entity:22s} files={file_count:>2}  src={source_count:>7}  "
          f"raw={target_count:>7}  -> {target}")

# Persist the audit + exception log, then apply the control gate.
abc_write(audit_rows, exception_rows)

n_fail = sum(a["status"] == "FAILED" for a in audit_rows)
n_warn = sum(a["status"] == "WARN" for a in audit_rows)
print(f"\nABC summary · run {NOW_BATCH}: {len(audit_rows)} entities · "
      f"{n_fail} failed · {n_warn} warnings · {len(exception_rows)} exceptions")
print(f"  audit log:  {abc_fqt(ABC_INGESTION_AUDIT)}")
print(f"  exceptions: {abc_fqt(ABC_CONTROL_EXCEPTION)}")

abc_gate(audit_rows)   # hard-control breach fails the task here
print("\nRaw Data (LZ / Bronze) load + ABC controls complete.")
