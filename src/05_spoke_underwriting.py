# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Spoke — Underwriting (Risk, Quotes)
# MAGIC Domain data product for underwriting. Conforms quotes to the HUB master keys
# MAGIC and publishes a risk/quotes fact plus a risk-appetite summary. Owned by the
# MAGIC Underwriting domain team in the mesh.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

show_header("05 · Spoke: Underwriting")

quote = spark.table(fq("oz_organized", "quote"))
dim_party = spark.table(fq("hub", "dim_party"))

# fact_quote conformed to hub keys
fact_quote = (quote.alias("q")
    .join(dim_party.select("party_id", "legal_name", "kyc_risk_rating").alias("p"),
          "party_id", "left")
    .select(
        "quote_id", "party_id", F.col("legal_name").alias("insured_name"),
        "producer_id", "line_of_business", "region", "industry_sector",
        "sum_insured", "requested_limit", "deductible", "risk_score",
        "tech_price", "premium_quoted", "quote_status", "quote_date",
        "underwriter", "currency", "kyc_risk_rating",
    )
    .withColumn("is_bound", F.col("quote_status") == "BOUND")
    .withColumn("rate_on_line", F.round(F.col("premium_quoted") / F.col("requested_limit"), 6))
    .withColumn("price_adequacy", F.round(F.col("premium_quoted") / F.col("tech_price"), 4)))
fact_quote.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("underwriting", "fact_quote"))
print(f"  underwriting.fact_quote rows={fact_quote.count()}")

# risk appetite / hit-ratio summary by LOB x region
summary = (fact_quote.groupBy("line_of_business", "region").agg(
    F.count("*").alias("quotes"),
    F.sum(F.col("is_bound").cast("int")).alias("bound"),
    F.round(F.avg("risk_score"), 1).alias("avg_risk_score"),
    F.round(F.avg("price_adequacy"), 3).alias("avg_price_adequacy"),
    F.sum(F.when(F.col("is_bound"), F.col("premium_quoted"))).alias("bound_premium"),
).withColumn("hit_ratio", F.round(F.col("bound") / F.col("quotes"), 3)))
summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("underwriting", "risk_appetite_summary"))
print(f"  underwriting.risk_appetite_summary rows={summary.count()}")

print("\nUnderwriting spoke published.")
