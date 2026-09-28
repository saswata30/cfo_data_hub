# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Spoke — Policy (Policies, Fees)
# MAGIC Domain data product for the policy lifecycle: in-force book, written premium,
# MAGIC and the fee/commission breakdown from Guidewire Billing. Conforms to HUB keys.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

show_header("06 · Spoke: Policy")

policy = spark.table(fq("oz_organized", "policy"))
fees = spark.table(fq("oz_organized", "policy_fee"))
billing = spark.table(fq("oz_organized", "billing_transaction"))
dim_party = spark.table(fq("hub", "dim_party"))
dim_producer = spark.table(fq("hub", "dim_producer"))

# fact_policy: the in-force / written book conformed to hub
fact_policy = (policy.alias("p")
    .join(dim_party.select("party_id", F.col("legal_name").alias("insured_name")), "party_id", "left")
    .join(dim_producer.select("producer_id", "producer_name", "producer_type"), "producer_id", "left")
    .withColumn("is_inforce", F.col("policy_status") == "INFORCE")
    .withColumn("policy_year", F.year("inception_date")))
fact_policy.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("policy", "fact_policy"))
print(f"  policy.fact_policy rows={fact_policy.count()}")

# fact_policy_fee: fees & commissions
fact_fee = (fees.join(policy.select("policy_id", "line_of_business", "region"), "policy_id", "left"))
fact_fee.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("policy", "fact_policy_fee"))
print(f"  policy.fact_policy_fee rows={fact_fee.count()}")

# premium_earned summary (written premium + collected cash) by LOB x policy_year
collected = (billing.filter("txn_type IN ('PREMIUM','INSTALLMENT') AND status = 'SETTLED'")
             .groupBy("policy_id").agg(F.sum("amount").alias("collected_premium")))
premium_summary = (fact_policy
    .join(collected, "policy_id", "left")
    .groupBy("line_of_business", "region", "policy_year").agg(
        F.count("*").alias("policies"),
        F.sum(F.col("is_inforce").cast("int")).alias("inforce_policies"),
        F.sum("gross_written_premium").alias("gwp"),
        F.sum("net_premium").alias("net_premium"),
        F.sum("collected_premium").alias("collected_premium"),
    ))
premium_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("policy", "premium_summary"))
print(f"  policy.premium_summary rows={premium_summary.count()}")

print("\nPolicy spoke published.")
