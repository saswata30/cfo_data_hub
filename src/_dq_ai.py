# Databricks notebook source
# MAGIC %md
# MAGIC # _dq_ai — AI-powered Silver Data Quality
# MAGIC Semantic / contextual data-quality checks that the deterministic rule engine
# MAGIC (`_dq`) cannot express, implemented with **Databricks AI Functions**
# MAGIC (`ai_query` with structured output). Examples:
# MAGIC
# MAGIC - **legal_name validity** — is a party name a plausible real entity, or a
# MAGIC   test / placeholder / gibberish value?
# MAGIC - **cause ↔ line-of-business plausibility** — is a claim's `cause_of_loss`
# MAGIC   consistent with the policy `line_of_business`?
# MAGIC - **description ↔ cause consistency** — does the FNOL free-text match the
# MAGIC   recorded `cause_of_loss`?
# MAGIC
# MAGIC AI verdicts are **advisory**: flagged rows are logged and copied to
# MAGIC `<entity>_dq_ai_flagged`, but never quarantined out of Silver (unlike the
# MAGIC deterministic HIGH-severity rules). The pass is **sampled** (`AI_DQ_SAMPLE_ROWS`)
# MAGIC and **non-fatal** — any AI Function / endpoint error is caught so it can never
# MAGIC fail the pipeline or run away on cost. Results log to `abc_control.dq_ai_result`.
# MAGIC
# MAGIC Included after `_common` via `%run ./_dq_ai`. Requires Foundation Model API
# MAGIC access (`AI_DQ_MODEL`) and a DBR / serverless SQL that supports AI Functions.
# MAGIC
# MAGIC > The task-specific functions `ai_classify`, `ai_mask`, `ai_similarity` are
# MAGIC > drop-in alternatives for these checks; `ai_query` with a `STRUCT<...>`
# MAGIC > `responseFormat` is used here for typed, endpoint-agnostic verdicts.

# COMMAND ----------

import datetime as _dt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType,
)

DQ_AI_SCHEMA = SCHEMAS["abc_control"]
DQ_AI_RESULT = "dq_ai_result"


def dq_ai_fqt(name: str) -> str:
    return f"{CATALOG}.{DQ_AI_SCHEMA}.{name}"


_DQ_AI_RESULT_SCHEMA = StructType([
    StructField("dq_run_id", StringType()),
    StructField("layer", StringType()),
    StructField("entity", StringType()),
    StructField("check_name", StringType()),
    StructField("check_type", StringType()),
    StructField("model", StringType()),
    StructField("text_columns", StringType()),
    StructField("rows_evaluated", LongType()),
    StructField("rows_flagged", LongType()),
    StructField("flag_rate", DoubleType()),
    StructField("severity", StringType()),
    StructField("sample_rows", LongType()),
    StructField("description", StringType()),
    StructField("checked_at", TimestampType()),
])


def dq_ai_ensure_tables():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dq_ai_fqt(DQ_AI_RESULT)} (
          dq_run_id      STRING,
          layer          STRING,
          entity         STRING,
          check_name     STRING,
          check_type     STRING,
          model          STRING,
          text_columns   STRING,
          rows_evaluated BIGINT,
          rows_flagged   BIGINT,
          flag_rate      DOUBLE,
          severity       STRING,
          sample_rows    BIGINT,
          description    STRING,
          checked_at     TIMESTAMP
        ) USING DELTA
        COMMENT 'AI-powered Silver DQ results (advisory) — one row per AI check per run'
    """)


# Instruction prompts (no single quotes — embedded into a SQL string literal).
_P_LEGAL_NAME = (
    "You are a data quality validator for an insurance master data system. "
    "Decide whether the party legal name below is a plausible real organization or "
    "person name. Set is_valid to false for test, placeholder, gibberish, or clearly "
    "invalid values, otherwise true. Provide a short issue reason, empty when valid."
)
_P_CAUSE_LOB = (
    "You are validating commercial property and casualty and specialty insurance claims. "
    "Given a cause of loss and the policy line of business, decide whether the pairing is "
    "plausible. Set is_plausible to false only for clearly inconsistent pairings, otherwise "
    "true. Provide a short issue reason."
)
_P_DESC_CAUSE = (
    "You are validating insurance first notice of loss records. Decide whether the free text "
    "description is consistent with the recorded cause of loss. Set matches to true when "
    "consistent, false otherwise. Provide a short issue reason."
)


def dq_ai_check(entity, df, prompt_expr, response_struct, pass_field,
                check_name, check_type, text_columns, description,
                severity="MEDIUM", sample_rows=None):
    """Run one AI-Function DQ check over a sample of ``df``.

    ``prompt_expr`` is a Spark SQL expression producing the model prompt; the model
    returns ``response_struct`` (a ``STRUCT<...>`` DDL); ``pass_field`` is the boolean
    field that is TRUE when the row is good. Flagged rows (pass_field = false) are
    copied to ``oz_organized.<entity>_dq_ai_flagged``. Returns a result dict.
    """
    sample_rows = sample_rows or AI_DQ_SAMPLE_ROWS
    now = _dt.datetime.utcnow()
    verdict = F.expr(
        f"ai_query('{AI_DQ_MODEL}', {prompt_expr}, responseFormat => '{response_struct}')")
    sample = (df.limit(sample_rows)
              .withColumn("_ai", verdict)
              .withColumn("_ai_pass", F.col(f"_ai.{pass_field}"))
              .withColumn("_ai_issue", F.col("_ai.issue")))
    sample = sample.cache()
    try:
        evaluated = sample.count()
        # A NULL verdict means the model could not judge the row — flag it for
        # review rather than silently passing it (conservative DQ posture).
        flagged_df = sample.filter(~F.coalesce(F.col("_ai_pass"), F.lit(False)))
        flagged = flagged_df.count()
        # Always overwrite (even with 0 rows) so the table reflects THIS run and
        # never leaves a previous run's flagged rows behind.
        (flagged_df.drop("_ai", "_ai_pass")
            .withColumn("_dq_ai_run_id", F.lit(NOW_BATCH))
            .withColumn("_dq_ai_check", F.lit(check_name))
            .write.mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(fq("oz_organized", f"{entity}_dq_ai_flagged")))
        return {
            "dq_run_id": NOW_BATCH, "layer": "oz_silver_ai", "entity": entity,
            "check_name": check_name, "check_type": check_type, "model": AI_DQ_MODEL,
            "text_columns": text_columns,
            "rows_evaluated": int(evaluated), "rows_flagged": int(flagged),
            "flag_rate": round(flagged / evaluated, 6) if evaluated else 0.0,
            "severity": severity, "sample_rows": int(sample_rows),
            "description": description, "checked_at": now,
        }
    finally:
        sample.unpersist()


def dq_ai_write_results(results):
    if results:
        (rows_to_df(results, _DQ_AI_RESULT_SCHEMA)
            .write.mode("append").saveAsTable(dq_ai_fqt(DQ_AI_RESULT)))


def run_ai_dq():
    """Run the AI-Function DQ checks over the conformed Silver tables.

    No-op when ``AI_DQ_ENABLED`` is false. Wrapped so any AI Function / endpoint /
    permission error is non-fatal — it is logged and the pipeline continues.
    """
    if not AI_DQ_ENABLED:
        print("  AI DQ disabled (AI_DQ_ENABLED = False in src/_common.py) — skipping.")
        return []

    dq_ai_ensure_tables()
    results = []

    # Each check is isolated: a failure in one (endpoint throttling, permission)
    # is non-fatal and does not discard results already computed by the others.
    try:
        party = spark.table(fq("oz_organized", "party"))
        results.append(dq_ai_check(
            "party", party,
            prompt_expr=f"concat('{_P_LEGAL_NAME}  Party legal name: ', coalesce(legal_name, ''))",
            response_struct="STRUCT<is_valid:BOOLEAN, issue:STRING>",
            pass_field="is_valid", check_name="legal_name_validity",
            check_type="AI_QUERY_VALIDITY", text_columns="legal_name",
            description="Party legal name is a plausible real entity (not test/gibberish)"))
    except Exception as e:
        print(f"  AI DQ [party.legal_name_validity] skipped (non-fatal): {type(e).__name__}: {str(e)[:160]}")

    try:
        claim = spark.table(fq("oz_organized", "claim"))
        results.append(dq_ai_check(
            "claim", claim,
            prompt_expr=(f"concat('{_P_CAUSE_LOB}  Cause of loss: ', coalesce(cause_of_loss, ''), "
                         f"'. Line of business: ', coalesce(line_of_business, ''))"),
            response_struct="STRUCT<is_plausible:BOOLEAN, issue:STRING>",
            pass_field="is_plausible", check_name="cause_lob_plausibility",
            check_type="AI_QUERY_CONSISTENCY", text_columns="cause_of_loss,line_of_business",
            description="Claim cause_of_loss is plausible for the policy line_of_business"))
    except Exception as e:
        print(f"  AI DQ [claim.cause_lob_plausibility] skipped (non-fatal): {type(e).__name__}: {str(e)[:160]}")

    try:
        fnol = spark.table(fq("oz_organized", "fnol"))
        cause = spark.table(fq("oz_organized", "claim")).select("claim_id", "cause_of_loss")
        results.append(dq_ai_check(
            "fnol", fnol.join(cause, "claim_id", "left"),
            prompt_expr=(f"concat('{_P_DESC_CAUSE}  FNOL description: ', coalesce(description, ''), "
                         f"'. Recorded cause of loss: ', coalesce(cause_of_loss, ''))"),
            response_struct="STRUCT<matches:BOOLEAN, issue:STRING>",
            pass_field="matches", check_name="description_cause_consistency",
            check_type="AI_QUERY_CONSISTENCY", text_columns="description,cause_of_loss",
            description="FNOL free-text description is consistent with the recorded cause",
            severity="LOW"))
    except Exception as e:
        print(f"  AI DQ [fnol.description_cause_consistency] skipped (non-fatal): {type(e).__name__}: {str(e)[:160]}")

    # Persist whatever completed, even if a later check failed above.
    if results:
        try:
            dq_ai_write_results(results)
        except Exception as e:
            print(f"  AI DQ results write failed (non-fatal): {type(e).__name__}: {str(e)[:160]}")
    return results
