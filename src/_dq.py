# Databricks notebook source
# MAGIC %md
# MAGIC # _dq — Silver Data Quality framework
# MAGIC A small **declarative expectation library** for the **Organized Zone (Silver)**.
# MAGIC For each conformed entity it:
# MAGIC
# MAGIC 1. evaluates a set of expectations (not-null, range, allowed-set, regex,
# MAGIC    referential-integrity) in a single pass;
# MAGIC 2. flags every row with `_dq_status` (PASS / FAIL) and `_dq_failed_rules`;
# MAGIC 3. routes rows failing a **HIGH**-severity rule to `<entity>_dq_quarantine`
# MAGIC    (kept out of the clean conformed table) when quarantine is enabled;
# MAGIC 4. appends a run-scoped result log to `abc_control.dq_result`.
# MAGIC
# MAGIC Included after `_common` via `%run ./_dq` (relies on `CATALOG`, `SCHEMAS`,
# MAGIC `fq`, `NOW_BATCH`, `rows_to_df`, `QUARANTINE_DQ_FAILURES`).

# COMMAND ----------

import datetime as _dt
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType,
)

DQ_SCHEMA = SCHEMAS["abc_control"]
DQ_RESULT = "dq_result"


def dq_fqt(name: str) -> str:
    return f"{CATALOG}.{DQ_SCHEMA}.{name}"


_DQ_RESULT_SCHEMA = StructType([
    StructField("dq_run_id", StringType()),
    StructField("layer", StringType()),
    StructField("entity", StringType()),
    StructField("rule_id", StringType()),
    StructField("rule_type", StringType()),
    StructField("column_name", StringType()),
    StructField("severity", StringType()),
    StructField("rows_evaluated", LongType()),
    StructField("rows_failed", LongType()),
    StructField("fail_rate", DoubleType()),
    StructField("passed", BooleanType()),
    StructField("description", StringType()),
    StructField("checked_at", TimestampType()),
])


def dq_ensure_tables():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dq_fqt(DQ_RESULT)} (
          dq_run_id      STRING,
          layer          STRING,
          entity         STRING,
          rule_id        STRING,
          rule_type      STRING,
          column_name    STRING,
          severity       STRING,
          rows_evaluated BIGINT,
          rows_failed    BIGINT,
          fail_rate      DOUBLE,
          passed         BOOLEAN,
          description    STRING,
          checked_at     TIMESTAMP
        ) USING DELTA
        COMMENT 'Silver Data Quality results — one row per rule per run'
    """)


# ---- expectation constructors: each returns a spec dict ---------------------
# ``pass_expr`` is a boolean Column that is TRUE when a row satisfies the rule.

def dq_not_null(column, rule_id=None, severity="HIGH"):
    return {"rule_id": rule_id or f"NN_{column}", "rule_type": "NOT_NULL",
            "column": column, "severity": severity,
            "pass_expr": F.col(column).isNotNull(),
            "desc": f"{column} is not null"}


def dq_range(column, lo=None, hi=None, rule_id=None, severity="HIGH"):
    c = F.col(column).cast("double")
    cond = F.col(column).isNotNull()
    if lo is not None:
        cond = cond & (c >= F.lit(float(lo)))
    if hi is not None:
        cond = cond & (c <= F.lit(float(hi)))
    bounds = f"[{lo if lo is not None else '-inf'}, {hi if hi is not None else '+inf'}]"
    return {"rule_id": rule_id or f"RNG_{column}", "rule_type": "RANGE",
            "column": column, "severity": severity, "pass_expr": cond,
            "desc": f"{column} within {bounds}"}


def dq_in_set(column, values, rule_id=None, severity="MEDIUM"):
    vals = list(values)
    return {"rule_id": rule_id or f"SET_{column}", "rule_type": "IN_SET",
            "column": column, "severity": severity,
            "pass_expr": F.col(column).isin(vals),
            "desc": f"{column} in {sorted(map(str, vals))}"}


def dq_regex(column, pattern, rule_id=None, severity="MEDIUM"):
    return {"rule_id": rule_id or f"RGX_{column}", "rule_type": "REGEX",
            "column": column, "severity": severity,
            "pass_expr": F.col(column).isNotNull() & F.col(column).rlike(pattern),
            "desc": f"{column} matches /{pattern}/"}


def dq_referential(column, ref_entity, ref_col, rule_id=None, severity="HIGH"):
    """Referential integrity: ``column`` must exist in ``oz_organized.<ref_entity>.<ref_col>``.
    Evaluated via a broadcast membership join inside :func:`dq_run`."""
    return {"rule_id": rule_id or f"REF_{column}", "rule_type": "REFERENTIAL",
            "column": column, "severity": severity,
            "ref_entity": ref_entity, "ref_col": ref_col,
            "desc": f"{column} references {ref_entity}.{ref_col}"}


def dq_run(entity, df, checks, run_id=None, quarantine=None):
    """Evaluate ``checks`` against ``df``.

    Returns ``(clean_df, results, quarantined_count)`` where ``clean_df`` holds the
    rows passing every HIGH-severity rule (with `_dq_run_id` / `_dq_checked_at`
    lineage columns), ``results`` is a list of per-rule result dicts, and any
    HIGH-severity failures are written to ``oz_organized.<entity>_dq_quarantine``.
    """
    run_id = run_id or NOW_BATCH
    quarantine = QUARANTINE_DQ_FAILURES if quarantine is None else quarantine
    now = _dt.datetime.utcnow()

    fail_flag_cols = []      # names of the per-rule boolean fail columns on df
    hard_fail_terms = []     # boolean Columns for HIGH-severity fails
    fail_when_terms = []     # when(fail, rule_id) for the failed-rules string
    count_exprs = [F.count(F.lit(1)).alias("__n")]

    for c in checks:
        rid = c["rule_id"]
        fcol = f"__f_{rid}"
        if c["rule_type"] == "REFERENTIAL":
            ref_ids = (spark.table(fq("oz_organized", c["ref_entity"]))
                       .select(F.col(c["ref_col"]).alias("__ref_id")).distinct())
            df = df.join(F.broadcast(ref_ids),
                         df[c["column"]] == F.col("__ref_id"), "left")
            failed = F.col(c["column"]).isNotNull() & F.col("__ref_id").isNull()
            df = df.withColumn(fcol, failed).drop("__ref_id")
        else:
            passed = F.coalesce(c["pass_expr"], F.lit(False))
            df = df.withColumn(fcol, ~passed)

        fail_flag_cols.append(fcol)
        count_exprs.append(F.sum(F.col(fcol).cast("long")).alias(rid))
        fail_when_terms.append(F.when(F.col(fcol), F.lit(rid)))
        if c["severity"] == "HIGH":
            hard_fail_terms.append(F.col(fcol))

    # Per-row flags.
    df = (df
          .withColumn("_dq_failed_rules",
                      F.concat_ws(",", *fail_when_terms) if fail_when_terms else F.lit(""))
          .withColumn("_dq_status",
                      F.when(reduce(lambda a, b: a | b, hard_fail_terms), F.lit("FAIL")).otherwise(F.lit("PASS"))
                      if hard_fail_terms else F.lit("PASS"))
          .withColumn("_dq_run_id", F.lit(run_id))
          .withColumn("_dq_checked_at", F.current_timestamp()))
    df = df.cache()

    # Aggregate per-rule failure counts in a single pass.
    agg = df.agg(*count_exprs).collect()[0]
    total = int(agg["__n"])
    results = []
    for c in checks:
        failed_n = int(agg[c["rule_id"]] or 0)
        results.append({
            "dq_run_id": run_id, "layer": "oz_silver", "entity": entity,
            "rule_id": c["rule_id"], "rule_type": c["rule_type"],
            "column_name": c["column"], "severity": c["severity"],
            "rows_evaluated": total, "rows_failed": failed_n,
            "fail_rate": round(failed_n / total, 6) if total else 0.0,
            "passed": failed_n == 0, "description": c["desc"], "checked_at": now,
        })

    # Split clean vs. quarantine and drop the scratch fail columns.
    clean_all = df.drop(*fail_flag_cols)
    quarantined = clean_all.filter(F.col("_dq_status") == "FAIL")
    q_count = quarantined.count()
    if quarantine and q_count > 0:
        (quarantined.write.mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(fq("oz_organized", f"{entity}_dq_quarantine")))

    clean = (clean_all.filter(F.col("_dq_status") == "PASS")
             if quarantine else clean_all).drop("_dq_status", "_dq_failed_rules")
    return clean, results, q_count


def dq_write_results(results):
    if results:
        (rows_to_df(results, _DQ_RESULT_SCHEMA)
            .write.mode("append").saveAsTable(dq_fqt(DQ_RESULT)))
