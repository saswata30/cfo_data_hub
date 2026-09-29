# Databricks notebook source
# MAGIC %md
# MAGIC # _abc — Audit, Balance & Control (ABC) framework
# MAGIC The control layer that wraps **Source → Raw (Bronze)** ingestion. For every
# MAGIC entity landed into `lz_raw` it records:
# MAGIC
# MAGIC - **Audit** — an append-only run log: batch id, source systems, landed file
# MAGIC   count, source vs. target row counts, numeric control totals, timings, status.
# MAGIC - **Balance** — reconciliation of the *landed source* against the *raw target*:
# MAGIC   row-count balance (ingestion must be lossless) and a numeric control-total
# MAGIC   balance (e.g. `SUM(gross_written_premium)` in = out).
# MAGIC - **Control** — a gate that **fails the run** on a hard-control breach (nothing
# MAGIC   landed, or a row-count imbalance) and logs every breach as an exception.
# MAGIC
# MAGIC Included after `_common` via `%run ./_abc` (relies on `CATALOG`, `SCHEMAS`,
# MAGIC `fq`, `NOW_BATCH`, `rows_to_df`).

# COMMAND ----------

import datetime as _dt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType,
)

ABC_SCHEMA = SCHEMAS["abc_control"]
ABC_INGESTION_AUDIT = "ingestion_audit"
ABC_CONTROL_EXCEPTION = "control_exception"


def abc_fqt(name: str) -> str:
    """Fully-qualified name of an ABC control table."""
    return f"{CATALOG}.{ABC_SCHEMA}.{name}"


_AUDIT_SCHEMA = StructType([
    StructField("abc_run_id", StringType()),
    StructField("layer", StringType()),
    StructField("entity", StringType()),
    StructField("source_systems", StringType()),
    StructField("landing_paths", StringType()),
    StructField("file_count", LongType()),
    StructField("source_row_count", LongType()),
    StructField("target_row_count", LongType()),
    StructField("row_variance", LongType()),
    StructField("control_column", StringType()),
    StructField("source_control_total", DoubleType()),
    StructField("target_control_total", DoubleType()),
    StructField("control_total_variance", DoubleType()),
    StructField("rows_balanced", BooleanType()),
    StructField("control_total_balanced", BooleanType()),
    StructField("status", StringType()),
    StructField("message", StringType()),
    StructField("started_at", TimestampType()),
    StructField("ended_at", TimestampType()),
    StructField("duration_sec", DoubleType()),
])

_EXCEPTION_SCHEMA = StructType([
    StructField("abc_run_id", StringType()),
    StructField("layer", StringType()),
    StructField("entity", StringType()),
    StructField("control_name", StringType()),
    StructField("severity", StringType()),
    StructField("expected", StringType()),
    StructField("actual", StringType()),
    StructField("message", StringType()),
    StructField("detected_at", TimestampType()),
])


def abc_ensure_tables():
    """Create the ABC control tables (idempotent, append-only history)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {abc_fqt(ABC_INGESTION_AUDIT)} (
          abc_run_id             STRING,
          layer                  STRING,
          entity                 STRING,
          source_systems         STRING,
          landing_paths          STRING,
          file_count             BIGINT,
          source_row_count       BIGINT,
          target_row_count       BIGINT,
          row_variance           BIGINT,
          control_column         STRING,
          source_control_total   DOUBLE,
          target_control_total   DOUBLE,
          control_total_variance DOUBLE,
          rows_balanced          BOOLEAN,
          control_total_balanced BOOLEAN,
          status                 STRING,
          message                STRING,
          started_at             TIMESTAMP,
          ended_at               TIMESTAMP,
          duration_sec           DOUBLE
        ) USING DELTA
        COMMENT 'ABC: append-only Source->Raw ingestion audit & balance reconciliation'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {abc_fqt(ABC_CONTROL_EXCEPTION)} (
          abc_run_id   STRING,
          layer        STRING,
          entity       STRING,
          control_name STRING,
          severity     STRING,
          expected     STRING,
          actual       STRING,
          message      STRING,
          detected_at  TIMESTAMP
        ) USING DELTA
        COMMENT 'ABC: control-check breaches raised during Source->Raw ingestion'
    """)


def abc_evaluate(entity, source_systems, landing_paths, file_count,
                 source_row_count, target_row_count,
                 control_column=None, source_control_total=None,
                 target_control_total=None, started_at=None):
    """Run the balance & control checks for one entity ingestion.

    Returns ``(audit_row, exception_rows)`` as plain dicts; the caller batches
    them and writes with :func:`abc_write`.
    """
    started_at = started_at or _dt.datetime.utcnow()
    now = _dt.datetime.utcnow()
    source_row_count = int(source_row_count)
    target_row_count = int(target_row_count)
    row_variance = source_row_count - target_row_count
    rows_balanced = row_variance == 0

    control_total_variance = None
    control_total_balanced = None
    if control_column is not None and source_control_total is not None:
        src_t = float(source_control_total or 0.0)
        tgt_t = float(target_control_total or 0.0)
        control_total_variance = round(src_t - tgt_t, 2)
        tol = CONTROL_TOTAL_TOLERANCE * (abs(src_t) if src_t else 1.0)
        control_total_balanced = abs(control_total_variance) <= tol

    exceptions = []

    def add_exc(name, severity, expected, actual, message):
        exceptions.append({
            "abc_run_id": NOW_BATCH, "layer": "source_to_raw", "entity": entity,
            "control_name": name, "severity": severity,
            "expected": str(expected), "actual": str(actual),
            "message": message, "detected_at": now,
        })

    status, msgs = "SUCCESS", []
    if target_row_count == 0:                                    # hard control
        status = "FAILED"
        msgs.append("no rows ingested")
        add_exc("EMPTY_INGESTION", "HIGH", "> 0 rows", target_row_count,
                f"{entity}: raw target has 0 rows")
    if not rows_balanced:                                        # hard control
        status = "FAILED"
        msgs.append(f"row-count imbalance ({row_variance:+d})")
        add_exc("ROW_COUNT_BALANCE", "HIGH", source_row_count, target_row_count,
                f"{entity}: source {source_row_count} vs raw {target_row_count} "
                f"(variance {row_variance:+d})")
    if control_total_balanced is False:                          # soft control
        if status != "FAILED":
            status = "WARN"
        msgs.append(f"control-total variance {control_total_variance:+.2f} on {control_column}")
        add_exc("CONTROL_TOTAL_BALANCE", "MEDIUM",
                source_control_total, target_control_total,
                f"{entity}: SUM({control_column}) source {source_control_total} "
                f"vs raw {target_control_total} (variance {control_total_variance:+.2f})")

    audit = {
        "abc_run_id": NOW_BATCH, "layer": "source_to_raw", "entity": entity,
        "source_systems": ",".join(source_systems),
        "landing_paths": ";".join(landing_paths),
        "file_count": int(file_count),
        "source_row_count": source_row_count,
        "target_row_count": target_row_count,
        "row_variance": row_variance,
        "control_column": control_column,
        "source_control_total": (float(source_control_total)
                                 if source_control_total is not None else None),
        "target_control_total": (float(target_control_total)
                                 if target_control_total is not None else None),
        "control_total_variance": control_total_variance,
        "rows_balanced": bool(rows_balanced),
        "control_total_balanced": control_total_balanced,
        "status": status,
        "message": "; ".join(msgs) if msgs else "balanced",
        "started_at": started_at,
        "ended_at": now,
        "duration_sec": round((now - started_at).total_seconds(), 3),
    }
    return audit, exceptions


def abc_write(audit_rows, exception_rows):
    """Append the batch's audit + exception rows to the ABC control tables."""
    if audit_rows:
        (rows_to_df(audit_rows, _AUDIT_SCHEMA)
            .write.mode("append").saveAsTable(abc_fqt(ABC_INGESTION_AUDIT)))
    if exception_rows:
        (rows_to_df(exception_rows, _EXCEPTION_SCHEMA)
            .write.mode("append").saveAsTable(abc_fqt(ABC_CONTROL_EXCEPTION)))


def abc_gate(audit_rows):
    """Control gate: raise (failing the task) if any entity hit a hard control."""
    failed = [a for a in audit_rows if a["status"] == "FAILED"]
    if failed:
        detail = ", ".join(f"{a['entity']} ({a['message']})" for a in failed)
        raise Exception(
            f"ABC control gate FAILED for {len(failed)} of {len(audit_rows)} entities: {detail}"
        )
