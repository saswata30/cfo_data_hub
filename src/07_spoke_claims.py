# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Spoke — Claims (FNOL, Claims)
# MAGIC Domain data product for claims: the claims fact enriched with FNOL notification
# MAGIC detail and hub keys, plus a fraud-triage view feeding Risk & Fraud Reporting.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

show_header("07 · Spoke: Claims")

claim = spark.table(fq("oz_organized", "claim"))
fnol = spark.table(fq("oz_organized", "fnol"))
dim_party = spark.table(fq("hub", "dim_party"))

# fact_claim conformed to hub + FNOL detail
fact_claim = (claim.alias("c")
    .join(fnol.select("claim_id", "channel", "catastrophe_flag", "notified_date"), "claim_id", "left")
    .join(dim_party.select("party_id", F.col("legal_name").alias("insured_name"), "kyc_risk_rating"),
          "party_id", "left")
    .withColumn("is_open", F.col("claim_status").isin("OPEN", "REOPENED"))
    .withColumn("accident_year", F.year("loss_date"))
    .withColumn("severity_band", F.when(F.col("incurred_amount") >= 1e7, "SEVERE")
                                  .when(F.col("incurred_amount") >= 1e6, "LARGE")
                                  .otherwise("ATTRITIONAL")))
fact_claim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("claims", "fact_claim"))
print(f"  claims.fact_claim rows={fact_claim.count()}")

# fraud triage: high fraud-score, late-reported or litigated claims
fraud = (fact_claim.filter("fraud_flag = true OR litigation_flag = true OR report_lag_days > 60")
    .select("claim_id", "policy_id", "insured_name", "line_of_business", "region",
            "incurred_amount", "paid_amount", "reserve_amount", "fraud_score", "fraud_flag",
            "litigation_flag", "report_lag_days", "cause_of_loss", "kyc_risk_rating")
    .withColumn("triage_priority",
                F.when(F.col("fraud_score") >= 0.75, "P1")
                 .when(F.col("fraud_score") >= 0.55, "P2").otherwise("P3")))
fraud.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("claims", "fraud_triage"))
print(f"  claims.fraud_triage rows={fraud.count()}")

print("\nClaims spoke published.")
