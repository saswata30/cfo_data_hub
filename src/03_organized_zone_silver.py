# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Organized Zone (OZ) — Silver
# MAGIC Cleans and **conforms** the raw entities: enforces types, standardises codes,
# MAGIC de-duplicates on business keys, casts dates/decimals, and applies a light
# MAGIC conformance/data-quality flag. Output tables in `oz_organized` are the trusted,
# MAGIC query-ready versions that the Hub and Spokes build on.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F, Window

show_header("03 · Organized Zone (Silver)")


def dedup(df, key, order_col="_ingested_at"):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def conform(entity, key, transforms):
    df = spark.table(fq("lz_raw", entity))
    df = transforms(df)
    df = dedup(df, key).withColumn("_conformed_at", F.current_timestamp())
    target = fq("oz_organized", entity)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"  silver {entity:22s} rows={df.count():>7}  -> {target}")


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

print("\nOrganized Zone (Silver) complete.")
