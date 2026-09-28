# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Data Security & Governance — Unity Catalog
# MAGIC The governance band that runs **across every layer**: PII/KYC column masking,
# MAGIC data-classification tags, and access grants. Non-privileged users see masked
# MAGIC PII; only members of the reader groups see raw values.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

show_header("10 · Governance & masking (Unity Catalog)")
hub = SCHEMAS["hub"]

# --- Masking functions (live in the hub schema) ---------------------------
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{hub}.mask_pii(val STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('{PII_READER_GROUP}') THEN val
            ELSE CONCAT('****', RIGHT(COALESCE(val,''), 2)) END
""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{hub}.mask_pii_date(val DATE)
RETURNS DATE
RETURN CASE WHEN is_account_group_member('{PII_READER_GROUP}') THEN val ELSE NULL END
""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{hub}.mask_kyc(val STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('{KYC_READER_GROUP}') THEN val ELSE 'RESTRICTED' END
""")
print("  masking functions created (mask_pii, mask_pii_date, mask_kyc)")

# --- Apply masks + PII tags to the HUB master data ------------------------
def alter(sql):
    try:
        spark.sql(sql); print(f"    ok: {sql[:70]}...")
    except Exception as e:
        print(f"    skip: {sql[:60]}... ({str(e)[:60]})")

dim_party = fq("hub", "dim_party")
for col in ["tax_id", "email", "phone"]:
    alter(f"ALTER TABLE {dim_party} ALTER COLUMN {col} SET MASK {CATALOG}.{hub}.mask_pii")
    alter(f"ALTER TABLE {dim_party} ALTER COLUMN {col} SET TAGS ('pii' = 'true')")
alter(f"ALTER TABLE {dim_party} ALTER COLUMN date_of_birth SET MASK {CATALOG}.{hub}.mask_pii_date")
alter(f"ALTER TABLE {dim_party} ALTER COLUMN date_of_birth SET TAGS ('pii' = 'true')")

kyc = fq("hub", "party_kyc_profile")
alter(f"ALTER TABLE {kyc} ALTER COLUMN tax_id SET MASK {CATALOG}.{hub}.mask_kyc")
for col in ["tax_id", "kyc_status", "kyc_risk_rating", "pep_flag"]:
    alter(f"ALTER TABLE {kyc} ALTER COLUMN {col} SET TAGS ('kyc' = 'true')")

# --- Representative grants (best-effort: groups may not exist in a PoC ws) --
grants = [
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMAS['reporting']} TO `account users`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.{hub} TO `{PII_READER_GROUP}`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.{hub} TO `{KYC_READER_GROUP}`",
]
for g in grants:
    alter(g)

print("\nGovernance applied. Verify masking with:")
print(f"  SELECT party_id, legal_name, tax_id, email, date_of_birth FROM {dim_party} LIMIT 5;")
print("  (non-members of the reader groups see masked values)")
