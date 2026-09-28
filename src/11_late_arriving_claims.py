# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Scenario — Late-arriving claim facts & EFR restatement
# MAGIC
# MAGIC **Standalone scenario notebook.** Run it *after* the base pipeline (`00`→`09`)
# MAGIC has populated the catalog — it doesn't rebuild the platform, it demonstrates how
# MAGIC the platform absorbs **facts that arrive after their accounting period has already
# MAGIC been reported**, and how the **EFR** (Enterprise Finance Reporting) gold layer is
# MAGIC restated for the affected periods.
# MAGIC
# MAGIC ## The business problem
# MAGIC In commercial P&C / specialty insurance, claim facts are *chronically late*:
# MAGIC - **Late-reported losses (IBNR emergence).** A loss occurs in, say, Nov-2025 but is
# MAGIC   only notified (FNOL) months later. The economic event belongs to the **accident
# MAGIC   period**, not the day we heard about it.
# MAGIC - **Loss development.** An open claim's paid amount grows and its reserve is
# MAGIC   re-estimated (strengthened or released) long after the claim was first booked.
# MAGIC
# MAGIC The base pipeline writes every gold table with a full `overwrite`. That is fine for
# MAGIC a first load, but a late fact must be handled **incrementally** so we only touch the
# MAGIC keys and periods that actually changed, and so the general ledger is corrected in
# MAGIC place rather than reloaded from scratch.
# MAGIC
# MAGIC ## How this notebook handles it
# MAGIC 1. **Land** a late batch of claim extracts (new late-reported claims + development on
# MAGIC    existing open claims) — appended to Bronze, exactly as a real late file would be.
# MAGIC 2. **Upsert** to Silver (`oz_organized.claim`) and to the Claims spoke
# MAGIC    (`claims.fact_claim`) with Delta `MERGE` — no full reload.
# MAGIC 3. **Restate EFR**: `MERGE` the recomputed postings into `efr.gl_detail`, then rebuild
# MAGIC    only the affected periods of `efr.trial_balance`. CFO reporting views pick the
# MAGIC    correction up automatically.
# MAGIC 4. **Audit**: mark late rows (`_late_arrival`, `_booking_date`), and use Delta time
# MAGIC    travel to show the pre-restatement numbers are still recoverable.
# MAGIC
# MAGIC Every step is **idempotent** — re-running the notebook with the same widgets produces
# MAGIC the same result (no double-counting), because the merges key on business identifiers.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import random, datetime as dt
from pyspark.sql import functions as F, Window

# Scenario widgets --------------------------------------------------------
dbutils.widgets.text("n_new_late_claims", "40", "New late-reported claims")
dbutils.widgets.text("n_developed_claims", "60", "Existing claims that developed")
dbutils.widgets.text("booking_date", "2026-09-28", "Date the late batch arrives (YYYY-MM-DD)")

N_NEW = int(dbutils.widgets.get("n_new_late_claims"))
N_DEV = int(dbutils.widgets.get("n_developed_claims"))
BOOKING_DATE = dbutils.widgets.get("booking_date")
BOOKING = dt.date.fromisoformat(BOOKING_DATE)
LATE_BATCH = f"{BOOKING.strftime('%Y%m%d')}_LATE"

# Delta schema evolution: lets the MERGEs add the late-arrival audit columns
# (`_late_arrival`, `_booking_date`) to tables the base pipeline created without them.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

show_header("11 · Late-arriving claim facts & EFR restatement")
print(f"  booking_date={BOOKING_DATE}  new_late={N_NEW}  developed={N_DEV}  batch={LATE_BATCH}")

CLAIM_TBL = fq("oz_organized", "claim")
FACT_TBL  = fq("claims", "fact_claim")
GL_TBL    = fq("efr", "gl_detail")
TB_TBL    = fq("efr", "trial_balance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Preconditions
# MAGIC The scenario mutates gold that the base pipeline produces. Fail fast with a clear
# MAGIC message if it hasn't run in this catalog yet.

# COMMAND ----------

required = [CLAIM_TBL, FACT_TBL, GL_TBL, TB_TBL,
            fq("oz_organized", "policy"), fq("oz_organized", "fnol"), fq("hub", "dim_party")]
missing = [t for t in required if not spark.catalog.tableExists(t)]
if missing:
    raise Exception(
        "Base pipeline tables are missing: " + ", ".join(missing) +
        f"\nRun notebooks 00→09 (or `databricks bundle run cfo_data_platform_pipeline`) "
        f"against catalog '{CATALOG}' before this scenario.")


def ensure_cols(table, coldefs):
    """Idempotently add late-arrival audit columns to a base table (safe to re-run)."""
    existing = set(spark.table(table).columns)
    to_add = [c for c in coldefs if c.split()[0] not in existing]
    if to_add:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(to_add)})")

for _t in (CLAIM_TBL, FACT_TBL, GL_TBL):
    ensure_cols(_t, ["_late_arrival boolean", "_booking_date date"])
print("  preconditions OK — base pipeline tables present; audit columns ensured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Build the late batch
# MAGIC Two flavours of late fact, both referentially valid against the existing book:
# MAGIC - **New late-reported claims** — backdated `loss_date` (8–20 months ago), `report_date`
# MAGIC   = today. Huge `report_lag_days`, so they belong to a **prior accident period**.
# MAGIC - **Developed claims** — existing OPEN/REOPENED claims whose paid grows and reserve is
# MAGIC   re-estimated (some close out entirely). Same keys, revised amounts.

# COMMAND ----------

random.seed(SCALE["seed"] + 1001)
CAUSES = ["Fire", "Flood", "Windstorm", "Cyber Breach", "Business Interruption", "Liability",
          "Cargo Loss", "Hull Damage", "Theft", "Professional Negligence", "Product Recall"]

# -- New late-reported claims: reference real, in-force-ish policies -----------
pol_sample = (spark.table(fq("oz_organized", "policy"))
    .select("policy_id", "party_id", "line_of_business", "region", "currency",
            "gross_written_premium", "inception_date")
    .orderBy(F.rand(SCALE["seed"] + 7)).limit(N_NEW * 3).collect())

late_rows = []
for i, p in enumerate(pol_sample[:N_NEW]):
    gwp = float(p["gross_written_premium"] or 0.0)
    incept = p["inception_date"]
    loss_date = BOOKING - dt.timedelta(days=random.randint(240, 600))
    if incept and loss_date < incept:                 # keep the loss inside the policy period
        loss_date = incept + dt.timedelta(days=random.randint(5, 120))
    incurred = round(max(gwp, 1.0) * random.uniform(0.3, 4.0), 2)
    if random.random() < 0.5:                          # closed & (mostly) paid
        status = "CLOSED"
        paid = round(incurred * random.uniform(0.7, 1.0), 2)
    else:                                              # still open, reserve-heavy
        status = "OPEN"
        paid = round(incurred * random.uniform(0.0, 0.4), 2)
    reserve = round(max(0.0, incurred - paid), 2)
    fraud = round(min(1.0, max(0.0, random.gauss(0.18, 0.16) + 0.25)), 3)  # late report => elevated
    late_rows.append({
        "claim_id": f"CLM9{i:05d}",                    # 9-prefixed => no collision with base CLM0xxxxxx
        "policy_id": p["policy_id"], "party_id": p["party_id"],
        "line_of_business": p["line_of_business"], "region": p["region"],
        "loss_date": loss_date.isoformat(), "report_date": BOOKING.isoformat(),
        "claim_status": status, "cause_of_loss": random.choice(CAUSES),
        "incurred_amount": incurred, "paid_amount": paid, "reserve_amount": reserve,
        "currency": p["currency"], "fraud_score": fraud, "fraud_flag": fraud >= 0.55,
        "litigation_flag": random.random() < 0.12,
        "source_system": "ship", "extract_ts": BOOKING.isoformat(),
    })

# -- Developed claims: revise amounts on existing open claims ------------------
dev_sample = (spark.table(CLAIM_TBL)
    .filter("claim_status IN ('OPEN', 'REOPENED')")
    .select("claim_id", "policy_id", "party_id", "line_of_business", "region",
            "loss_date", "report_date", "cause_of_loss", "incurred_amount",
            "paid_amount", "reserve_amount", "currency", "fraud_score",
            "fraud_flag", "litigation_flag")
    .orderBy(F.rand(SCALE["seed"] + 13)).limit(N_DEV).collect())

for c in dev_sample:
    old_incurred = float(c["incurred_amount"] or 0.0)
    old_paid = float(c["paid_amount"] or 0.0)
    extra_pay = old_incurred * random.uniform(0.10, 0.40)          # payments since last valuation
    strengthen = old_incurred * random.uniform(0.0, 0.30)          # reserve strengthening
    new_paid = round(old_paid + extra_pay, 2)
    new_incurred = round(max(old_incurred, new_paid) + strengthen, 2)
    if random.random() < 0.30:                                     # claim closes out
        status, new_paid, new_reserve = "CLOSED", new_incurred, 0.0
    else:
        status = "REOPENED" if c["claim_status"] == "REOPENED" else "OPEN"
        new_reserve = round(max(0.0, new_incurred - new_paid), 2)
    late_rows.append({
        "claim_id": c["claim_id"], "policy_id": c["policy_id"], "party_id": c["party_id"],
        "line_of_business": c["line_of_business"], "region": c["region"],
        "loss_date": c["loss_date"].isoformat(), "report_date": c["report_date"].isoformat(),
        "claim_status": status, "cause_of_loss": c["cause_of_loss"],
        "incurred_amount": new_incurred, "paid_amount": new_paid, "reserve_amount": new_reserve,
        "currency": c["currency"], "fraud_score": float(c["fraud_score"] or 0.0),
        "fraud_flag": bool(c["fraud_flag"]), "litigation_flag": bool(c["litigation_flag"]),
        "source_system": "ship", "extract_ts": BOOKING.isoformat(),
    })

print(f"  late batch built: {len(late_rows)} claim facts "
      f"({N_NEW} new late-reported + {len(dev_sample)} developed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Land the batch (Bronze append)
# MAGIC In production this file would land in the `lz_raw.landing` Volume and be picked up by
# MAGIC Auto Loader / `COPY INTO`. Here we append it straight to the Bronze claim table with
# MAGIC the same ingestion lineage columns, tagged with a distinct `_bronze_batch`.

# COMMAND ----------

bronze_cols = spark.table(fq("lz_raw", "claim")).columns
late_raw = (spark.createDataFrame(late_rows)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_bronze_batch", F.lit(LATE_BATCH))
    .withColumn("_source_file", F.lit("late_arriving_batch/ship/claim"))
    .select(*bronze_cols))
late_raw.write.mode("append").saveAsTable(fq("lz_raw", "claim"))
print(f"  appended {late_raw.count()} rows to bronze {fq('lz_raw', 'claim')} (batch {LATE_BATCH})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Upsert Silver (`oz_organized.claim`)
# MAGIC Conform only the late batch (same transforms as notebook `03`) and `MERGE` on
# MAGIC `claim_id`: developed claims **update in place**, new claims **insert**. Late rows are
# MAGIC stamped with `_late_arrival` / `_booking_date` for audit; schema evolution adds those
# MAGIC columns to the table (existing rows stay `NULL`).

# COMMAND ----------

w = Window.partitionBy("claim_id").orderBy(F.col("_ingested_at").desc())
late_silver = (spark.table(fq("lz_raw", "claim"))
    .filter(F.col("_bronze_batch") == LATE_BATCH)
    .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    .withColumn("loss_date", F.to_date("loss_date"))
    .withColumn("report_date", F.to_date("report_date"))
    .withColumn("incurred_amount", F.col("incurred_amount").cast("decimal(18,2)"))
    .withColumn("paid_amount", F.col("paid_amount").cast("decimal(18,2)"))
    .withColumn("reserve_amount", F.col("reserve_amount").cast("decimal(18,2)"))
    .withColumn("report_lag_days", F.datediff("report_date", "loss_date"))
    .withColumn("claim_status", F.upper("claim_status"))
    .withColumn("_conformed_at", F.current_timestamp())
    .withColumn("_late_arrival", F.lit(True))
    .withColumn("_booking_date", F.to_date(F.lit(BOOKING_DATE))))
late_silver.createOrReplaceTempView("late_silver")

spark.sql(f"""
MERGE INTO {CLAIM_TBL} t
USING late_silver s ON t.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f"  silver claim merged. max report_lag_days in batch = "
      f"{late_silver.agg(F.max('report_lag_days')).first()[0]} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Upsert the Claims spoke (`claims.fact_claim`)
# MAGIC Recompute the fact **only for the affected claim keys** (re-deriving `is_open`,
# MAGIC `accident_year`, `severity_band` and re-joining FNOL + hub party), then `MERGE` on
# MAGIC `claim_id`.

# COMMAND ----------

affected = late_silver.select("claim_id").distinct()
affected.createOrReplaceTempView("late_affected")

fnol = spark.table(fq("oz_organized", "fnol"))
dim_party = spark.table(fq("hub", "dim_party"))

late_fact = (spark.table(CLAIM_TBL).join(affected, "claim_id").alias("c")
    .join(fnol.select("claim_id", "channel", "catastrophe_flag", "notified_date"), "claim_id", "left")
    .join(dim_party.select("party_id", F.col("legal_name").alias("insured_name"), "kyc_risk_rating"),
          "party_id", "left")
    .withColumn("is_open", F.col("claim_status").isin("OPEN", "REOPENED"))
    .withColumn("accident_year", F.year("loss_date"))
    .withColumn("severity_band", F.when(F.col("incurred_amount") >= 1e7, "SEVERE")
                                  .when(F.col("incurred_amount") >= 1e6, "LARGE")
                                  .otherwise("ATTRITIONAL")))
late_fact.createOrReplaceTempView("late_fact")

spark.sql(f"""
MERGE INTO {FACT_TBL} t
USING late_fact s ON t.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f"  claims.fact_claim merged for {affected.count()} affected claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Recompute the affected GL postings
# MAGIC Re-post the affected claims through the **same chart-of-accounts mapping the Finance
# MAGIC Engine uses in notebook `08`** — Losses Paid (`5000`, dated `loss_date`) and Loss
# MAGIC Reserves (`5100`, dated `report_date`). Because late claims carry a **backdated
# MAGIC `loss_date`**, their paid losses post into the **prior accident period** — that is the
# MAGIC period EFR must restate.

# COMMAND ----------

def post(df, account_code, account_name, category, amount_col, date_col, ref_col, ref_name):
    """Chart-of-accounts posting — identical to the Finance Engine mapping in notebook 08."""
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

fact_aff = spark.table(FACT_TBL).join(affected, "claim_id")
late_gl = (
    post(fact_aff.filter("paid_amount > 0"),
         "5000", "Losses Paid", "LOSS", "paid_amount", "loss_date", "claim_id", "CLAIM")
    .unionByName(post(fact_aff.filter("reserve_amount > 0"),
         "5100", "Loss Reserves", "RESERVE", "reserve_amount", "report_date", "claim_id", "CLAIM"))
    .withColumn("gl_id", F.expr("uuid()"))
    .withColumn("posting_period", F.date_format("posting_date", "yyyy-MM"))
    .withColumn("_posted_at", F.current_timestamp())
    .withColumn("_late_arrival", F.lit(True))
    .withColumn("_booking_date", F.to_date(F.lit(BOOKING_DATE))))
late_gl.cache()
late_gl.createOrReplaceTempView("late_gl")
print(f"  recomputed {late_gl.count()} posting lines for the affected claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Snapshot the affected periods *before* restating
# MAGIC The periods to restate are every period the affected claims touch — both the periods
# MAGIC they used to post to and the periods they post to now. We snapshot Finance Reporting
# MAGIC and record the current Delta version of `gl_detail` so we can prove the change and
# MAGIC time-travel back to it.

# COMMAND ----------

old_p = spark.sql(f"""
    SELECT DISTINCT posting_period FROM {GL_TBL}
    WHERE ref_type = 'CLAIM' AND source_ref IN (SELECT claim_id FROM late_affected)""").collect()
new_p = spark.sql("SELECT DISTINCT posting_period FROM late_gl").collect()
affected_periods = sorted({r[0] for r in old_p} | {r[0] for r in new_p})
plist = ", ".join(f"'{p}'" for p in affected_periods)
print(f"  affected posting periods ({len(affected_periods)}): {affected_periods}")


def fr_snapshot():
    """Finance Reporting rolled up to posting_period, for the affected periods only."""
    rows = spark.sql(f"""
        SELECT posting_period,
               ROUND(SUM(losses_paid + reserves), 2)                                    AS losses,
               ROUND(SUM(losses_paid + reserves) / NULLIF(SUM(premium_revenue), 0), 4)  AS loss_ratio,
               ROUND(SUM(losses_paid + reserves + expenses)
                     / NULLIF(SUM(premium_revenue), 0), 4)                              AS combined_ratio
        FROM {fq('reporting', 'finance_reporting')}
        WHERE posting_period IN ({plist})
        GROUP BY posting_period""").collect()
    return {r["posting_period"]: r for r in rows}

before = fr_snapshot()
gl_version_before = spark.sql(f"DESCRIBE HISTORY {GL_TBL}").agg(F.max("version")).first()[0]
print(f"  captured BEFORE snapshot; gl_detail is at Delta version {gl_version_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Restate EFR
# MAGIC **`gl_detail`**: `MERGE` on the natural posting key `(ref_type, source_ref,
# MAGIC account_code)` — developed postings update, new ones insert — then delete any claim
# MAGIC posting that no longer applies (e.g. a reserve line for a claim that has now fully
# MAGIC paid out). **`trial_balance`**: rebuild only the affected periods from the corrected
# MAGIC ledger. This is a targeted restatement, not a full reload.

# COMMAND ----------

# 7a · Upsert the ledger
spark.sql(f"""
MERGE INTO {GL_TBL} t
USING late_gl s
  ON t.ref_type = 'CLAIM' AND t.source_ref = s.source_ref AND t.account_code = s.account_code
WHEN MATCHED THEN UPDATE SET
  t.line_of_business = s.line_of_business, t.region = s.region, t.currency = s.currency,
  t.amount = s.amount, t.posting_date = s.posting_date, t.posting_period = s.posting_period,
  t._posted_at = current_timestamp(), t._late_arrival = true, t._booking_date = s._booking_date
WHEN NOT MATCHED THEN INSERT
  (source_ref, ref_type, line_of_business, region, currency, account_code, account_name,
   gl_category, amount, posting_date, gl_id, posting_period, _posted_at, _late_arrival, _booking_date)
  VALUES
  (s.source_ref, 'CLAIM', s.line_of_business, s.region, s.currency, s.account_code, s.account_name,
   s.gl_category, s.amount, s.posting_date, s.gl_id, s.posting_period,
   current_timestamp(), true, s._booking_date)
""")

# 7b · Remove postings that no longer apply for the affected claims (e.g. reserve released to 0)
spark.sql(f"""
DELETE FROM {GL_TBL}
WHERE ref_type = 'CLAIM'
  AND source_ref IN (SELECT claim_id FROM late_affected)
  AND NOT EXISTS (
      SELECT 1 FROM late_gl s
      WHERE s.source_ref = {GL_TBL}.source_ref AND s.account_code = {GL_TBL}.account_code)
""")

# 7c · Rebuild only the affected periods of the trial balance from the corrected ledger
spark.sql(f"DELETE FROM {TB_TBL} WHERE posting_period IN ({plist})")
spark.sql(f"""
INSERT INTO {TB_TBL}
SELECT account_code, account_name, gl_category, line_of_business, region, posting_period,
       SUM(amount) AS balance, COUNT(*) AS posting_count
FROM {GL_TBL}
WHERE posting_period IN ({plist})
GROUP BY account_code, account_name, gl_category, line_of_business, region, posting_period
""")

gl_version_after = spark.sql(f"DESCRIBE HISTORY {GL_TBL}").agg(F.max("version")).first()[0]
print(f"  EFR restated. gl_detail Delta version {gl_version_before} -> {gl_version_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Before / after — the CFO-visible impact
# MAGIC The `reporting.finance_reporting` view is unchanged code, yet the restated
# MAGIC `trial_balance` moves the loss and combined ratios for the affected periods.

# COMMAND ----------

after = fr_snapshot()
cmp_rows = []
for p in affected_periods:
    b, a = before.get(p), after.get(p)
    cmp_rows.append((
        p,
        float(b["loss_ratio"]) if b and b["loss_ratio"] is not None else None,
        float(a["loss_ratio"]) if a and a["loss_ratio"] is not None else None,
        float(b["combined_ratio"]) if b and b["combined_ratio"] is not None else None,
        float(a["combined_ratio"]) if a and a["combined_ratio"] is not None else None,
        float(b["losses"]) if b and b["losses"] is not None else None,
        float(a["losses"]) if a and a["losses"] is not None else None,
    ))
cmp_df = (spark.createDataFrame(
        cmp_rows,
        "posting_period string, loss_ratio_before double, loss_ratio_after double, "
        "combined_before double, combined_after double, losses_before double, losses_after double")
    .withColumn("combined_delta", F.round(F.col("combined_after") - F.col("combined_before"), 4))
    .withColumn("losses_delta", F.round(F.col("losses_after") - F.col("losses_before"), 2))
    .orderBy("posting_period"))

print("  Finance Reporting — before vs after the late batch (by posting period):")
cmp_df.show(len(affected_periods), truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Auditability — Delta time travel
# MAGIC The restatement is fully reversible / explainable: the pre-restatement ledger is still
# MAGIC readable via `VERSION AS OF`. Here we compare booked claim cost (Losses + Reserves) for
# MAGIC one affected period, at the version before vs after.

# COMMAND ----------

probe = affected_periods[0]
old_amt = spark.sql(f"""
    SELECT ROUND(SUM(amount), 2) AS booked_loss FROM {GL_TBL} VERSION AS OF {gl_version_before}
    WHERE gl_category IN ('LOSS', 'RESERVE') AND posting_period = '{probe}'""").first()["booked_loss"]
new_amt = spark.sql(f"""
    SELECT ROUND(SUM(amount), 2) AS booked_loss FROM {GL_TBL}
    WHERE gl_category IN ('LOSS', 'RESERVE') AND posting_period = '{probe}'""").first()["booked_loss"]
late_added = spark.sql(f"""
    SELECT ROUND(SUM(amount), 2) AS late_loss FROM {GL_TBL}
    WHERE gl_category IN ('LOSS', 'RESERVE') AND posting_period = '{probe}' AND _late_arrival = true
    """).first()["late_loss"]

print(f"  period {probe}: booked claim cost  before(v{gl_version_before})={old_amt}  "
      f"after(v{gl_version_after})={new_amt}  (of which late-arriving={late_added})")
print(f"\n  audit trail — last gl_detail operations:")
(spark.sql(f"DESCRIBE HISTORY {GL_TBL}")
    .select("version", "timestamp", "operation")
    .orderBy(F.col("version").desc()).show(6, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Layer | Table | How the late fact was handled |
# MAGIC |---|---|---|
# MAGIC | Bronze | `lz_raw.claim` | Appended (raw is immutable; the late file just adds rows) |
# MAGIC | Silver | `oz_organized.claim` | `MERGE` on `claim_id` — develop-in-place, insert-new |
# MAGIC | Spoke | `claims.fact_claim` | Recompute affected keys only, `MERGE` on `claim_id` |
# MAGIC | EFR | `efr.gl_detail` | `MERGE` on `(ref_type, source_ref, account_code)` + delete obsolete |
# MAGIC | EFR | `efr.trial_balance` | Rebuild **only the affected posting periods** |
# MAGIC | Reporting | `reporting.finance_reporting` (+ actuarial/claims) | No code change — reads the corrected gold |
# MAGIC
# MAGIC **Key ideas**
# MAGIC - Late facts post to the **accident period** (`loss_date`), so historical periods are
# MAGIC   restated correctly, while `_booking_date` records when the fact actually arrived.
# MAGIC - Everything keys on **business identifiers**, so the notebook is **idempotent** — safe
# MAGIC   to re-run.
# MAGIC - Delta **time travel** + the `_late_arrival` flag give a complete, queryable audit of
# MAGIC   what changed and when.
# MAGIC
# MAGIC **Reset:** to return the catalog to its pristine base-pipeline state, re-run notebooks
# MAGIC `01`→`09` (they full-overwrite), or `databricks bundle run cfo_data_platform_pipeline`.

# COMMAND ----------

print("\nLate-arriving claims scenario complete — EFR restated for periods:", affected_periods)
