# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · EFR — Enterprise Finance Reporting (GL, FAH-Detail)
# MAGIC The **Finance Engine** (FDM / AED / EBS / HFM) posts the spoke facts into a
# MAGIC general ledger. Produces the Semantic-Zone gold finance model:
# MAGIC - `gl_detail`   — posting-line detail (the FAH-Detail "As Is")
# MAGIC - `trial_balance` — account balances by LOB / region / period
# MAGIC - `reinsurance_ceded` — ceded premium & recoveries summary
# MAGIC
# MAGIC This is the single financial source of truth feeding CFO Reporting.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

show_header("08 · EFR (GL / FAH-Detail)")

policy = spark.table(fq("policy", "fact_policy"))
fees = spark.table(fq("policy", "fact_policy_fee"))
claim = spark.table(fq("claims", "fact_claim"))
reins = spark.table(fq("oz_organized", "reinsurance_contract"))

# Chart-of-accounts mapping for each economic event
def post(df, account_code, account_name, category, amount_col, date_col, ref_col, ref_name):
    return (df.select(
        F.col(ref_col).alias("source_ref"),
        F.lit(ref_name).alias("ref_type"),
        F.col("line_of_business"), F.col("region"),
        F.coalesce(F.col("currency"), F.lit("USD")).alias("currency"),
        F.lit(account_code).alias("account_code"),
        F.lit(account_name).alias("account_name"),
        F.lit(category).alias("gl_category"),
        F.col(amount_col).cast("decimal(18,2)").alias("amount"),
        F.col(date_col).alias("posting_date"),
    ))

gl = (
    # Premium revenue (credit) from written policies
    post(policy.filter("gross_written_premium > 0"),
         "4000", "Gross Written Premium", "REVENUE",
         "gross_written_premium", "inception_date", "policy_id", "POLICY")
    # Broker commission expense
    .unionByName(post(fees.filter("fee_type = 'BROKER_COMMISSION'"),
         "6100", "Broker Commission", "EXPENSE", "amount", "billed_date", "policy_id", "POLICY"))
    # Insurance tax / surcharge
    .unionByName(post(fees.filter("fee_type IN ('TAX','SURCHARGE','POLICY_FEE')"),
         "6200", "Taxes & Fees", "EXPENSE", "amount", "billed_date", "policy_id", "POLICY"))
    # Losses paid
    .unionByName(post(claim.filter("paid_amount > 0"),
         "5000", "Losses Paid", "LOSS", "paid_amount", "loss_date", "claim_id", "CLAIM"))
    # Loss reserves (IBNR/case)
    .unionByName(post(claim.filter("reserve_amount > 0"),
         "5100", "Loss Reserves", "RESERVE", "reserve_amount", "report_date", "claim_id", "CLAIM"))
)
gl = (gl.withColumn("gl_id", F.expr("uuid()"))
        .withColumn("posting_period", F.date_format("posting_date", "yyyy-MM"))
        .withColumn("_posted_at", F.current_timestamp()))
gl.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("efr", "gl_detail"))
print(f"  efr.gl_detail rows={gl.count()}")

# Trial balance: net position per account / LOB / region / period
tb = (gl.groupBy("account_code", "account_name", "gl_category",
                 "line_of_business", "region", "posting_period")
        .agg(F.sum("amount").alias("balance"), F.count("*").alias("posting_count")))
tb.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("efr", "trial_balance"))
print(f"  efr.trial_balance rows={tb.count()}")

# Reinsurance ceded summary
ceded = (reins.groupBy("line_of_business", "treaty_type").agg(
    F.count("*").alias("treaties"),
    F.sum("ceded_premium").alias("ceded_premium"),
    F.round(F.avg("ceded_share"), 3).alias("avg_ceded_share")))
ceded.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("efr", "reinsurance_ceded"))
print(f"  efr.reinsurance_ceded rows={ceded.count()}")

print("\nEFR semantic gold complete.")
