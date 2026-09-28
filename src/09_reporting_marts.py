# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · CFO Reporting marts (Gold serving)
# MAGIC The **CFO Reporting** column. Business-ready views over the mesh + EFR:
# MAGIC Actuarial, Risk & Fraud, Policy, Claims and Finance reporting. Exposed as views
# MAGIC so BI / Genie / dashboards always read the latest gold.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

show_header("09 · CFO Reporting marts")

R = SCHEMAS["reporting"]

# --- Actuarial Reporting: loss ratio & development by LOB / accident year ---
spark.sql(f"""
CREATE OR REPLACE VIEW {fq('reporting','actuarial_reporting')} AS
WITH losses AS (
  SELECT line_of_business, accident_year,
         SUM(incurred_amount) AS incurred_losses,
         SUM(paid_amount)     AS paid_losses,
         SUM(reserve_amount)  AS outstanding_reserves,
         COUNT(*)             AS claim_count
  FROM {fq('claims','fact_claim')}
  GROUP BY line_of_business, accident_year
),
prem AS (
  SELECT line_of_business, policy_year AS accident_year, SUM(gross_written_premium) AS earned_premium
  FROM {fq('policy','fact_policy')} GROUP BY line_of_business, policy_year
)
SELECT l.line_of_business, l.accident_year, p.earned_premium,
       l.incurred_losses, l.paid_losses, l.outstanding_reserves, l.claim_count,
       ROUND(l.incurred_losses / NULLIF(p.earned_premium,0), 4) AS loss_ratio
FROM losses l LEFT JOIN prem p
  ON l.line_of_business = p.line_of_business AND l.accident_year = p.accident_year
""")
print("  view: actuarial_reporting")

# --- Risk & Fraud Reporting ---
spark.sql(f"""
CREATE OR REPLACE VIEW {fq('reporting','risk_fraud_reporting')} AS
SELECT line_of_business, region, triage_priority,
       COUNT(*) AS flagged_claims,
       SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) AS fraud_flagged,
       SUM(CASE WHEN litigation_flag THEN 1 ELSE 0 END) AS litigated,
       ROUND(AVG(fraud_score),3) AS avg_fraud_score,
       SUM(incurred_amount) AS incurred_at_risk
FROM {fq('claims','fraud_triage')}
GROUP BY line_of_business, region, triage_priority
""")
print("  view: risk_fraud_reporting")

# --- Policy Reporting ---
spark.sql(f"""
CREATE OR REPLACE VIEW {fq('reporting','policy_reporting')} AS
SELECT line_of_business, region, policy_year,
       COUNT(*) AS policies,
       SUM(CASE WHEN is_inforce THEN 1 ELSE 0 END) AS inforce_policies,
       SUM(gross_written_premium) AS gwp,
       SUM(net_premium) AS net_premium,
       ROUND(SUM(net_premium)/NULLIF(SUM(gross_written_premium),0),4) AS retention_ratio
FROM {fq('policy','fact_policy')}
GROUP BY line_of_business, region, policy_year
""")
print("  view: policy_reporting")

# --- Claims Reporting ---
spark.sql(f"""
CREATE OR REPLACE VIEW {fq('reporting','claims_reporting')} AS
SELECT line_of_business, region, claim_status, severity_band,
       COUNT(*) AS claims,
       SUM(incurred_amount) AS incurred,
       SUM(paid_amount)     AS paid,
       SUM(reserve_amount)  AS reserves,
       ROUND(AVG(report_lag_days),1) AS avg_report_lag_days
FROM {fq('claims','fact_claim')}
GROUP BY line_of_business, region, claim_status, severity_band
""")
print("  view: claims_reporting")

# --- Finance Reporting: P&L and combined ratio from the trial balance ---
spark.sql(f"""
CREATE OR REPLACE VIEW {fq('reporting','finance_reporting')} AS
WITH tb AS (
  SELECT line_of_business, region, posting_period,
         SUM(CASE WHEN gl_category='REVENUE' THEN balance ELSE 0 END) AS premium_revenue,
         SUM(CASE WHEN gl_category='EXPENSE' THEN balance ELSE 0 END) AS expenses,
         SUM(CASE WHEN gl_category='LOSS'    THEN balance ELSE 0 END) AS losses_paid,
         SUM(CASE WHEN gl_category='RESERVE' THEN balance ELSE 0 END) AS reserves
  FROM {fq('efr','trial_balance')}
  GROUP BY line_of_business, region, posting_period
)
SELECT line_of_business, region, posting_period,
       premium_revenue, expenses, losses_paid, reserves,
       (premium_revenue - expenses - losses_paid - reserves) AS underwriting_result,
       ROUND((losses_paid + reserves)/NULLIF(premium_revenue,0),4) AS loss_ratio,
       ROUND(expenses/NULLIF(premium_revenue,0),4)                AS expense_ratio,
       ROUND((losses_paid + reserves + expenses)/NULLIF(premium_revenue,0),4) AS combined_ratio
FROM tb
""")
print("  view: finance_reporting")

print("\nCFO Reporting marts complete.")
for v in ["actuarial_reporting", "risk_fraud_reporting", "policy_reporting",
          "claims_reporting", "finance_reporting"]:
    print(f"    {fq('reporting', v)}")
