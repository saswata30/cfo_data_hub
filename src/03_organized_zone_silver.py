# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Organized Zone (OZ) — Silver, with Data Quality (DQ)
# MAGIC Cleans and **conforms** the raw entities: enforces types, standardises codes,
# MAGIC de-duplicates on business keys, casts dates/decimals. Every conformed entity is
# MAGIC then passed through the **Silver DQ engine** (`_dq`): declarative expectations
# MAGIC (not-null, range, allowed-set, regex, referential-integrity) are evaluated,
# MAGIC each row is flagged, HIGH-severity failures are routed to
# MAGIC `<entity>_dq_quarantine`, and a per-rule result log is written to
# MAGIC `abc_control.dq_result`. The clean tables in `oz_organized` are the trusted,
# MAGIC query-ready versions the Hub and Spokes build on.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %run ./_dq

# COMMAND ----------

# MAGIC %run ./_dq_ai

# COMMAND ----------

from pyspark.sql import functions as F, Window

show_header("03 · Organized Zone (Silver) + DQ")
dq_ensure_tables()

# DQ rule ids reuse the conformance_rule catalogue (CR001..CR005) where they align.
DQ_CHECKS = {
    "party": [
        dq_not_null("party_id"),
        dq_not_null("tax_id", rule_id="CR002", severity="HIGH"),
        dq_in_set("kyc_status", ["CLEARED", "PENDING", "REVIEW"], rule_id="DQ_party_kyc_status"),
        dq_regex("email", r"^[^@\s]+@[^@\s]+\.[^@\s]+$", rule_id="CR004", severity="MEDIUM"),
    ],
    "producer": [
        dq_not_null("producer_id"),
        dq_range("commission_rate", lo=0, hi=1, rule_id="DQ_producer_commission"),
    ],
    "quote": [
        dq_not_null("quote_id"),
        dq_not_null("party_id"),
        dq_range("risk_score", lo=1, hi=100, rule_id="CR005", severity="MEDIUM"),
        dq_range("premium_quoted", lo=0, rule_id="DQ_quote_premium"),
    ],
    "policy": [
        dq_not_null("policy_id"),
        dq_not_null("party_id"),
        dq_range("gross_written_premium", lo=0.01, rule_id="CR001", severity="HIGH"),
        dq_in_set("policy_status", ["INFORCE", "EXPIRED", "RENEWED", "CANCELLED"],
                  rule_id="DQ_policy_status"),
    ],
    "policy_fee": [
        dq_not_null("fee_id"),
        dq_range("amount", lo=0, rule_id="DQ_fee_amount", severity="MEDIUM"),
    ],
    "billing_transaction": [
        dq_not_null("txn_id"),
        dq_not_null("policy_id"),
    ],
    "claim": [
        dq_not_null("claim_id"),
        dq_referential("policy_id", "policy", "policy_id", rule_id="CR003", severity="HIGH"),
        dq_range("incurred_amount", lo=0, rule_id="DQ_claim_incurred"),
        dq_range("paid_amount", lo=0, rule_id="DQ_claim_paid", severity="MEDIUM"),
        dq_range("reserve_amount", lo=0, rule_id="DQ_claim_reserve", severity="MEDIUM"),
    ],
    "fnol": [
        dq_not_null("fnol_id"),
        dq_referential("claim_id", "claim", "claim_id", rule_id="DQ_fnol_claim_ref", severity="HIGH"),
    ],
    "reinsurance_contract": [
        dq_not_null("treaty_id"),
        dq_range("ceded_share", lo=0, hi=1, rule_id="DQ_reins_ceded_share"),
    ],
    "headcount": [
        dq_not_null("employee_id"),
        dq_range("fte", lo=0, hi=1, rule_id="DQ_headcount_fte", severity="MEDIUM"),
    ],
    "plan_forecast": [dq_not_null("plan_id")],
    "reserve_factor": [dq_not_null("factor_id")],
    "conformance_rule": [dq_not_null("rule_id")],
}

dq_all_results = []
dq_totals = {"rule_fails": 0, "quarantined": 0}


def dedup(df, key, order_col="_ingested_at"):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def conform(entity, key, transforms):
    df = spark.table(fq("lz_raw", entity))
    df = transforms(df)
    df = dedup(df, key).withColumn("_conformed_at", F.current_timestamp())

    # --- Silver Data Quality: evaluate expectations, flag & quarantine -----
    checks = DQ_CHECKS.get(entity, [])
    dq_note = "no dq rules"
    if checks:
        df, results, q_count = dq_run(entity, df, checks)
        dq_write_results(results)
        dq_all_results.extend(results)
        rule_fails = sum(not r["passed"] for r in results)
        dq_totals["rule_fails"] += rule_fails
        dq_totals["quarantined"] += q_count
        dq_note = f"dq {len(results)} rules · {rule_fails} failing · {q_count} quarantined"

    target = fq("oz_organized", entity)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"  silver {entity:22s} rows={df.count():>7}  {dq_note}  -> {target}")


# --- party: standardise KYC, cast DOB, upper-case codes -------------------
conform("party", "party_id", lambda df: (
    df.withColumn("kyc_status", F.upper(F.trim("kyc_status")))
      .withColumn("kyc_risk_rating", F.upper(F.trim("kyc_risk_rating")))
      .withColumn("date_of_birth", F.to_date("date_of_birth"))
      .withColumn("legal_name", F.trim("legal_name"))
      .withColumn("_dq_kyc_ok", F.col("tax_id").isNotNull() & (F.col("kyc_status") != "REVIEW"))
))

conform("producer", "producer_id", lambda df:
        df.withColumn("commission_rate", F.col("commission_rate").cast("decimal(6,3)")))

# --- quote --------------------------------------------------------------
conform("quote", "quote_id", lambda df: (
    df.withColumn("quote_date", F.to_date("quote_date"))
      .withColumn("premium_quoted", F.col("premium_quoted").cast("decimal(18,2)"))
      .withColumn("sum_insured", F.col("sum_insured").cast("decimal(18,2)"))
      .withColumn("quote_status", F.upper("quote_status"))
      .withColumn("_dq_ok", F.col("risk_score").between(1, 100))
))

# --- policy -------------------------------------------------------------
conform("policy", "policy_id", lambda df: (
    df.withColumn("inception_date", F.to_date("inception_date"))
      .withColumn("expiry_date", F.to_date("expiry_date"))
      .withColumn("gross_written_premium", F.col("gross_written_premium").cast("decimal(18,2)"))
      .withColumn("net_premium", F.col("net_premium").cast("decimal(18,2)"))
      .withColumn("policy_status", F.upper("policy_status"))
      .withColumn("_dq_ok", F.col("gross_written_premium") > 0)
))

conform("policy_fee", "fee_id", lambda df: (
    df.withColumn("amount", F.col("amount").cast("decimal(18,2)"))
      .withColumn("billed_date", F.to_date("billed_date"))))

conform("billing_transaction", "txn_id", lambda df: (
    df.withColumn("amount", F.col("amount").cast("decimal(18,2)"))
      .withColumn("txn_date", F.to_date("txn_date"))))

# --- claim / fnol -------------------------------------------------------
conform("claim", "claim_id", lambda df: (
    df.withColumn("loss_date", F.to_date("loss_date"))
      .withColumn("report_date", F.to_date("report_date"))
      .withColumn("incurred_amount", F.col("incurred_amount").cast("decimal(18,2)"))
      .withColumn("paid_amount", F.col("paid_amount").cast("decimal(18,2)"))
      .withColumn("reserve_amount", F.col("reserve_amount").cast("decimal(18,2)"))
      .withColumn("report_lag_days", F.datediff("report_date", "loss_date"))
      .withColumn("claim_status", F.upper("claim_status"))
))

conform("fnol", "fnol_id", lambda df: df.withColumn("notified_date", F.to_date("notified_date")))

conform("reinsurance_contract", "treaty_id", lambda df: (
    df.withColumn("inception_date", F.to_date("inception_date"))
      .withColumn("ceded_premium", F.col("ceded_premium").cast("decimal(18,2)"))))

conform("headcount", "employee_id", lambda df:
        df.withColumn("annual_cost", F.col("annual_cost").cast("decimal(18,2)")))
conform("plan_forecast", "plan_id", lambda df:
        df.withColumn("planned_amount", F.col("planned_amount").cast("decimal(18,2)")))
conform("reserve_factor", "factor_id", lambda df: df)
conform("conformance_rule", "rule_id", lambda df: df)

n_failing_rules = sum(not r["passed"] for r in dq_all_results)
print(f"\nSilver DQ summary · run {NOW_BATCH}: {len(dq_all_results)} rule-checks · "
      f"{n_failing_rules} failing · {dq_totals['quarantined']} rows quarantined")
print(f"  DQ results:  {dq_fqt(DQ_RESULT)}")

# --- AI-powered semantic DQ (advisory; sampled; non-fatal) ----------------
print("\nAI DQ (Databricks AI Functions):")
ai_results = run_ai_dq()
for r in ai_results:
    print(f"  [{r['severity']:<6}] {r['entity']}.{r['check_name']:<28} "
          f"flagged {r['rows_flagged']}/{r['rows_evaluated']} ({r['flag_rate']:.1%})")
if ai_results:
    print(f"  AI DQ results: {dq_ai_fqt(DQ_AI_RESULT)}")

print("\nOrganized Zone (Silver) + DQ complete.")
