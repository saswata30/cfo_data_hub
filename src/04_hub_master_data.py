# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · HUB Primary — Master Data, Profiles, KYC
# MAGIC The centre of the **Hub & Spoke Data Mesh**. Publishes the golden master-data
# MAGIC dimensions every spoke conforms to:
# MAGIC - `dim_party`   — master party (insured / reinsurer) golden record
# MAGIC - `dim_producer`— broker / MGA master
# MAGIC - `party_kyc_profile` — KYC status, risk rating, sanctions & PEP screening
# MAGIC
# MAGIC Downstream spokes (Underwriting, Policy, Claims) join to these keys so metrics
# MAGIC reconcile across the mesh.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

show_header("04 · HUB Primary (master data)")

party = spark.table(fq("oz_organized", "party"))

# --- dim_party: the golden party record shared across all spokes ----------
dim_party = (party.select(
    "party_id", "party_type", "party_kind", "legal_name",
    "tax_id", "email", "phone", "date_of_birth",
    "address_line", "city", "country", "postal_code", "region",
    "kyc_status", "kyc_risk_rating",
    F.col("source_system").alias("mastered_from"),
).withColumn("is_high_risk", F.col("kyc_risk_rating") == "HIGH")
 .withColumn("_published_at", F.current_timestamp()))

dim_party.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("hub", "dim_party"))
print(f"  hub.dim_party rows={dim_party.count()}")

# --- dim_producer ---------------------------------------------------------
dim_producer = (spark.table(fq("oz_organized", "producer"))
                .select("producer_id", "producer_name", "producer_type",
                        "commission_rate", "country")
                .withColumn("_published_at", F.current_timestamp()))
dim_producer.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("hub", "dim_producer"))
print(f"  hub.dim_producer rows={dim_producer.count()}")

# --- party_kyc_profile: dedicated KYC/AML view (masked downstream) ---------
kyc = (party.select(
    "party_id", "legal_name", "tax_id", "kyc_status", "kyc_risk_rating",
    "sanctions_checked", "pep_flag", "country", "region",
).withColumn("kyc_review_required",
             (F.col("kyc_status") == "REVIEW") | (F.col("pep_flag")) | (~F.col("sanctions_checked")))
 .withColumn("_published_at", F.current_timestamp()))
kyc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("hub", "party_kyc_profile"))
print(f"  hub.party_kyc_profile rows={kyc.count()}  "
      f"review_required={kyc.filter('kyc_review_required').count()}")

print("\nHUB Primary published.")
