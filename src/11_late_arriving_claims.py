# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Scenario — Late-arriving claim facts & how they reach EFR
# MAGIC
# MAGIC **Standalone scenario notebook.** Run it *after* the base pipeline (`00`→`10`) has
# MAGIC populated the catalog. It doesn't rebuild the platform: it shows how the platform
# MAGIC absorbs **claim facts that arrive after their accounting period has been reported**,
# MAGIC and exactly how they are populated in **EFR** (Enterprise Finance Reporting).
# MAGIC
# MAGIC ## The business problem
# MAGIC Claim facts are chronically late in commercial P&C / specialty insurance:
# MAGIC - **Late-reported losses (IBNR emergence):** a loss occurs in, say, Nov-2025 but FNOL
# MAGIC   only arrives months later. The economic event belongs to the **accident period**.
# MAGIC - **Loss development:** an open claim's paid amount grows and its reserve is
# MAGIC   re-estimated (strengthened or released) long after it was first booked.
# MAGIC
# MAGIC Finance has two needs that pull in opposite directions:
# MAGIC 1. **The booked ledger must not change after period close.** Reported periods are
# MAGIC    frozen for audit and regulators.
# MAGIC 2. **Actuarial and management views need the truth by accident period**, including
# MAGIC    losses we only learned about later.
# MAGIC
# MAGIC ## How EFR is populated (the rule this notebook implements)
# MAGIC For every affected claim × GL account, EFR compares what the claim **now requires**
# MAGIC with what is **already booked** and posts only the **difference** as a new journal line.
# MAGIC Original lines are never edited (append-only ledger):
# MAGIC
# MAGIC | Origin period of the posting | Journal posted | `entry_type` | `posting_period` |
# MAGIC |---|---|---|---|
# MAGIC | **OPEN** (not yet closed) | delta in that same period | `LATE_ADJ` | origin period |
# MAGIC | **CLOSED** | delta in the **current open period** | `PRIOR_PERIOD_ADJ` | booking period |
# MAGIC
# MAGIC Every line keeps `origin_period`, so two trial balances come out of one ledger:
# MAGIC - `efr.trial_balance` — **booked view** by `posting_period`; closed periods never move.
# MAGIC - `efr.trial_balance_by_origin` — **restated view** by `origin_period`; late facts
# MAGIC   restate the accident period they belong to.
# MAGIC
# MAGIC ## Steps
# MAGIC 0. Preconditions and **period close** (`close_through_period`)
# MAGIC 1. Build a late batch: new late-reported claims + development on open claims
# MAGIC 2. **Land** the file in the landing Volume and append it to Bronze, with **ABC** controls
# MAGIC 3. **Conform + DQ** to Silver and `MERGE` on `claim_id`
# MAGIC 4. `MERGE` the **Claims spoke** (`fact_claim`, `fraud_triage`) for affected keys only
# MAGIC 5. Compute the **required postings** for the affected claims
# MAGIC 6. Compare with the **booked ledger** and build **delta journals** per the rule above
# MAGIC 7. Post to `efr.gl_detail`, rebuild only the affected periods of both trial balances,
# MAGIC    and **prove closed periods did not move**
# MAGIC 8. Before / after for the CFO: booked vs restated loss and combined ratios
# MAGIC 9. Audit trail: `efr.late_arrival_log` + Delta time travel
# MAGIC
# MAGIC Every step is **idempotent**: re-running with the same widgets posts nothing new,
# MAGIC because deltas are computed against what is already booked.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %run ./_abc

# COMMAND ----------

# MAGIC %run ./_dq

# COMMAND ----------

import random, datetime as dt
from pyspark.sql import functions as F, Window

# Scenario widgets --------------------------------------------------------
dbutils.widgets.text("n_new_late_claims", "40", "New late-reported claims")
dbutils.widgets.text("n_developed_claims", "60", "Existing claims that developed")
dbutils.widgets.text("booking_date", "", "Date the late batch arrives (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("close_through_period", "", "Last CLOSED period (YYYY-MM, blank = month before booking)")

N_NEW = int(dbutils.widgets.get("n_new_late_claims"))
N_DEV = int(dbutils.widgets.get("n_developed_claims"))
BOOKING = (dt.date.fromisoformat(dbutils.widgets.get("booking_date"))
           if dbutils.widgets.get("booking_date").strip() else dt.date.today())
BOOKING_DATE = BOOKING.isoformat()
BOOKING_PERIOD = BOOKING.strftime("%Y-%m")
_prev_month = (BOOKING.replace(day=1) - dt.timedelta(days=1)).strftime("%Y-%m")
CLOSE_THROUGH = dbutils.widgets.get("close_through_period").strip() or _prev_month
LATE_BATCH = f"{BOOKING.strftime('%Y%m%d')}_LATE"

if CLOSE_THROUGH >= BOOKING_PERIOD:
    raise Exception(f"close_through_period {CLOSE_THROUGH} must be before the booking period "
                    f"{BOOKING_PERIOD}: late facts need an open period to post into.")

show_header("11 · Late-arriving claim facts → EFR")
print(f"  booking_date={BOOKING_DATE} (period {BOOKING_PERIOD})  close_through={CLOSE_THROUGH}")
print(f"  new_late={N_NEW}  developed={N_DEV}  batch={LATE_BATCH}")

CLAIM_TBL = fq("oz_organized", "claim")
FACT_TBL  = fq("claims", "fact_claim")
FRAUD_TBL = fq("claims", "fraud_triage")
GL_TBL    = fq("efr", "gl_detail")
TB_TBL    = fq("efr", "trial_balance")
TBO_TBL   = fq("efr", "trial_balance_by_origin")
PC_TBL    = fq("efr", "period_control")
LOG_TBL   = fq("efr", "late_arrival_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Preconditions and period close
# MAGIC Fail fast if the base pipeline hasn't run. Then make sure the EFR ledger carries the
# MAGIC late-arrival columns (`entry_type`, `origin_period`, …) — catalogs built by an older
# MAGIC version of notebook `08` are upgraded in place — and **close** every period up to
# MAGIC `close_through_period`.

# COMMAND ----------

required = [CLAIM_TBL, FACT_TBL, GL_TBL, TB_TBL, fq("lz_raw", "claim"),
            fq("oz_organized", "policy"), fq("oz_organized", "fnol"), fq("hub", "dim_party")]
missing = [t for t in required if not spark.catalog.tableExists(t)]
if missing:
    raise Exception(
        "Base pipeline tables are missing: " + ", ".join(missing) +
        f"\nRun `databricks bundle run cfo_data_platform_pipeline` against catalog "
        f"'{CATALOG}' before this scenario.")


def ensure_cols(table, coldefs):
    """Idempotently add audit columns to a base table (safe to re-run)."""
    existing = set(spark.table(table).columns)
    to_add = [c for c in coldefs if c.split()[0] not in existing]
    if to_add:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(to_add)})")


for _t in (CLAIM_TBL, FACT_TBL):
    ensure_cols(_t, ["_late_arrival boolean", "_booking_date date"])
ensure_cols(GL_TBL, ["entry_type string", "origin_period string", "_late_arrival boolean",
                     "_booking_date date", "_batch_id string"])
spark.sql(f"""UPDATE {GL_TBL} SET entry_type = coalesce(entry_type, 'ORIGINAL'),
              origin_period = coalesce(origin_period, posting_period),
              _late_arrival = coalesce(_late_arrival, false)
              WHERE entry_type IS NULL OR origin_period IS NULL OR _late_arrival IS NULL""")

if not spark.catalog.tableExists(TBO_TBL):
    spark.sql(f"""CREATE TABLE {TBO_TBL} AS
        SELECT account_code, account_name, gl_category, line_of_business, region, origin_period,
               SUM(amount) AS balance, COUNT(*) AS posting_count
        FROM {GL_TBL} GROUP BY ALL""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {PC_TBL} (
    posting_period STRING NOT NULL, status STRING NOT NULL, closed_at TIMESTAMP, closed_by STRING)
    USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {LOG_TBL} (
    batch_id STRING, booking_date DATE, claim_id STRING, late_type STRING,
    account_code STRING, gl_category STRING, line_of_business STRING, region STRING,
    origin_period STRING, posting_period STRING, entry_type STRING,
    previously_booked DECIMAL(18,2), now_required DECIMAL(18,2), adjustment DECIMAL(18,2),
    report_lag_days INT, logged_at TIMESTAMP) USING DELTA""")
abc_ensure_tables()
dq_ensure_tables()

# Period close: register every ledger period (plus the booking period) and close through
# CLOSE_THROUGH. Closing is one-way: an already-closed period is never reopened here.
spark.sql(f"""
MERGE INTO {PC_TBL} t
USING (SELECT DISTINCT posting_period FROM {GL_TBL}
       UNION SELECT '{BOOKING_PERIOD}') s
ON t.posting_period = s.posting_period
WHEN NOT MATCHED THEN INSERT (posting_period, status) VALUES (s.posting_period, 'OPEN')
""")
spark.sql(f"""UPDATE {PC_TBL} SET status = 'CLOSED', closed_at = current_timestamp(),
              closed_by = current_user()
              WHERE status = 'OPEN' AND posting_period <= '{CLOSE_THROUGH}'""")
if spark.sql(f"SELECT status FROM {PC_TBL} WHERE posting_period = '{BOOKING_PERIOD}'").first()[0] != "OPEN":
    raise Exception(f"Booking period {BOOKING_PERIOD} is CLOSED; choose a later booking_date.")

pc = spark.sql(f"SELECT status, count(*) n, min(posting_period) lo, max(posting_period) hi "
               f"FROM {PC_TBL} GROUP BY status").collect()
print("  period control:", {r["status"]: f"{r['n']} periods ({r['lo']}..{r['hi']})" for r in pc})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Build the late batch
# MAGIC Built once per `booking_date` and frozen as a file in the landing Volume; a re-run
# MAGIC replays that file. Two flavours of late fact, both referentially valid:
# MAGIC - **New late-reported claims:** backdated `loss_date` (8–20 months ago),
# MAGIC   `report_date` = booking date. Their paid losses belong to **closed** periods.
# MAGIC - **Developed claims:** existing OPEN / REOPENED claims whose paid grows and reserve is
# MAGIC   re-estimated (some close out). Same keys, revised amounts.

# COMMAND ----------

late_path = f"{landing_path('ship')}/claim_late/{LATE_BATCH}"


def _landed():
    try:
        return any(f.name.endswith(".parquet") for f in dbutils.fs.ls(late_path))
    except Exception:
        return False


# A batch is generated once and then frozen as its landed file: re-running the notebook for
# the same booking_date replays exactly the same facts (that is what makes it idempotent).
if not _landed():
    random.seed(SCALE["seed"] + 1001)
    CAUSES = ["Fire", "Flood", "Windstorm", "Cyber Breach", "Business Interruption", "Liability",
              "Cargo Loss", "Hull Damage", "Theft", "Professional Negligence", "Product Recall"]

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
        loss_date = min(loss_date, BOOKING - dt.timedelta(days=30))
        incurred = round(max(gwp, 1.0) * random.uniform(0.3, 4.0), 2)
        if random.random() < 0.5:
            status, paid = "CLOSED", round(incurred * random.uniform(0.7, 1.0), 2)
        else:
            status, paid = "OPEN", round(incurred * random.uniform(0.0, 0.4), 2)
        reserve = round(max(0.0, incurred - paid), 2)
        fraud = round(min(1.0, max(0.0, random.gauss(0.18, 0.16) + 0.25)), 3)  # late report => elevated
        cid = f"CLM9{BOOKING.strftime('%y%m%d')}{i:03d}"   # 9-prefixed + booking date: never collides
        late_rows.append({
            "claim_id": cid, "policy_id": p["policy_id"], "party_id": p["party_id"],
            "line_of_business": p["line_of_business"], "region": p["region"],
            "loss_date": loss_date.isoformat(), "report_date": BOOKING_DATE,
            "claim_status": status, "cause_of_loss": random.choice(CAUSES),
            "incurred_amount": incurred, "paid_amount": paid, "reserve_amount": reserve,
            "currency": p["currency"], "fraud_score": fraud, "fraud_flag": fraud >= 0.55,
            "litigation_flag": random.random() < 0.12,
            "source_system": "ship", "extract_ts": BOOKING_DATE,
        })

    dev_sample = (spark.table(CLAIM_TBL)
        .filter("claim_status IN ('OPEN', 'REOPENED') AND NOT claim_id LIKE 'CLM9%'")
        .select("claim_id", "policy_id", "party_id", "line_of_business", "region",
                "loss_date", "report_date", "claim_status", "cause_of_loss", "incurred_amount",
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
        if random.random() < 0.30:                                     # claim closes out: reserve released
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
            "source_system": "ship", "extract_ts": BOOKING_DATE,
        })
    spark.createDataFrame(late_rows).write.mode("overwrite").parquet(late_path)
    print(f"  late batch built and landed: {len(late_rows)} claim facts "
          f"({N_NEW} new late-reported + {len(dev_sample)} developed)")
else:
    print(f"  replaying the already-landed batch {late_path}")

late_types = (spark.read.parquet(late_path).select("claim_id").distinct()
    .withColumn("late_type", F.when(F.col("claim_id").startswith("CLM9"), "LATE_REPORTED")
                              .otherwise("DEVELOPMENT")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Land the file and append to Bronze, under ABC control
# MAGIC The late extract lands in the `lz_raw.landing` Volume like any other source file. It
# MAGIC is appended to `lz_raw.claim` (raw is immutable; a late file only adds rows) and the
# MAGIC **ABC** framework reconciles file → Bronze on row count and `SUM(incurred_amount)`.
# MAGIC A hard-control breach fails the run here, before anything reaches Silver or EFR.

# COMMAND ----------

started = dt.datetime.utcnow()

# Idempotent: drop rows from a previous run of this same batch before re-landing.
spark.sql(f"DELETE FROM {fq('lz_raw', 'claim')} WHERE _bronze_batch = '{LATE_BATCH}'")

src = spark.read.parquet(late_path)
s = src.agg(F.count(F.lit(1)).alias("n"), F.sum("incurred_amount").alias("ctl"),
            F.countDistinct("_metadata.file_path").alias("files")).first()

bronze_cols = spark.table(fq("lz_raw", "claim")).columns
landed = (src.withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_bronze_batch", F.lit(LATE_BATCH))
             .withColumn("_source_file", F.col("_metadata.file_path"))
             .withColumn("_abc_run_id", F.lit(NOW_BATCH)))
landed = landed.select(*[c if c in landed.columns else F.lit(None).alias(c) for c in bronze_cols])
landed.write.mode("append").saveAsTable(fq("lz_raw", "claim"))

t = (spark.table(fq("lz_raw", "claim")).filter(F.col("_bronze_batch") == LATE_BATCH)
     .agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("incurred_amount").cast("double")).alias("ctl")).first())
audit, excs = abc_evaluate("claim", ["ship"], [late_path], s["files"], s["n"], t["n"],
                           control_column="incurred_amount", source_control_total=s["ctl"],
                           target_control_total=t["ctl"], started_at=started)
abc_write([audit], excs)
print(f"  ABC {audit['status']}: file {s['n']} rows / bronze {t['n']} rows; "
      f"SUM(incurred) variance {audit['control_total_variance']}")
abc_gate([audit])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Conform + DQ, then MERGE into Silver (`oz_organized.claim`)
# MAGIC Same transforms and the same claim DQ rules as notebook `03`. Rows failing a
# MAGIC HIGH-severity rule (e.g. a claim on an unknown policy) are **appended** to the claim
# MAGIC quarantine and do not flow on; the rest `MERGE` on `claim_id`: developed claims
# MAGIC update in place, new claims insert. Late rows are stamped `_late_arrival` /
# MAGIC `_booking_date`.

# COMMAND ----------

def align_to(df, table):
    """Select ``table``'s columns from ``df`` (typed NULL for any it lacks) so MERGE * is exact."""
    tgt = spark.table(table).schema
    return df.select(*[(F.col(f.name) if f.name in df.columns else F.lit(None)).cast(f.dataType).alias(f.name)
                       for f in tgt.fields])


w = Window.partitionBy("claim_id").orderBy(F.col("_ingested_at").desc())
conformed = (spark.table(fq("lz_raw", "claim"))
    .filter(F.col("_bronze_batch") == LATE_BATCH)
    .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    .withColumn("loss_date", F.to_date("loss_date"))
    .withColumn("report_date", F.to_date("report_date"))
    .withColumn("incurred_amount", F.col("incurred_amount").cast("decimal(18,2)"))
    .withColumn("paid_amount", F.col("paid_amount").cast("decimal(18,2)"))
    .withColumn("reserve_amount", F.col("reserve_amount").cast("decimal(18,2)"))
    .withColumn("report_lag_days", F.datediff("report_date", "loss_date"))
    .withColumn("claim_status", F.upper("claim_status"))
    .withColumn("_conformed_at", F.current_timestamp()))

CLAIM_CHECKS = [
    dq_not_null("claim_id"),
    dq_referential("policy_id", "policy", "policy_id", rule_id="CR003", severity="HIGH"),
    dq_range("incurred_amount", lo=0, rule_id="DQ_claim_incurred"),
    dq_range("paid_amount", lo=0, rule_id="DQ_claim_paid", severity="MEDIUM"),
    dq_range("reserve_amount", lo=0, rule_id="DQ_claim_reserve", severity="MEDIUM"),
]
# quarantine=False keeps every row so we can append (not overwrite) the base quarantine.
checked, dq_results, _ = dq_run("claim", conformed, CLAIM_CHECKS, quarantine=False)
dq_write_results(dq_results)

known_pol = spark.table(fq("oz_organized", "policy")).select("policy_id").distinct()
bad = checked.join(known_pol, "policy_id", "left_anti").unionByName(checked.filter("claim_id IS NULL"))
n_bad = bad.count()
if n_bad:
    q_tbl = fq("oz_organized", "claim_dq_quarantine")
    bad_q = bad.withColumn("_dq_status", F.lit("FAIL")).withColumn("_dq_failed_rules", F.lit("CR003"))
    if spark.catalog.tableExists(q_tbl):
        align_to(bad_q, q_tbl).write.mode("append").saveAsTable(q_tbl)
    else:
        bad_q.write.saveAsTable(q_tbl)

late_silver = (checked.join(bad.select("claim_id"), "claim_id", "left_anti")
    .withColumn("_late_arrival", F.lit(True))
    .withColumn("_booking_date", F.to_date(F.lit(BOOKING_DATE))))
align_to(late_silver, CLAIM_TBL).createOrReplaceTempView("late_silver")

spark.sql(f"""
MERGE INTO {CLAIM_TBL} t
USING late_silver s ON t.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f"  DQ: {sum(not r['passed'] for r in dq_results)} rules failing, {n_bad} rows quarantined")
print(f"  silver claim merged; max report_lag_days in batch = "
      f"{late_silver.agg(F.max('report_lag_days')).first()[0]} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · MERGE the Claims spoke for the affected keys only
# MAGIC Recompute `fact_claim` (same derivations as notebook `07`: `is_open`,
# MAGIC `accident_year`, `severity_band`, FNOL + hub party) only for the affected
# MAGIC `claim_id`s, and refresh their rows in `fraud_triage` (late reports are a fraud signal).

# COMMAND ----------

affected = late_silver.select("claim_id").distinct()
affected.createOrReplaceTempView("late_affected")

fnol = spark.table(fq("oz_organized", "fnol"))
dim_party = spark.table(fq("hub", "dim_party"))
late_fact = (spark.table(CLAIM_TBL).join(affected, "claim_id")
    .join(fnol.select("claim_id", "channel", "catastrophe_flag", "notified_date"), "claim_id", "left")
    .join(dim_party.select("party_id", F.col("legal_name").alias("insured_name"), "kyc_risk_rating"),
          "party_id", "left")
    .withColumn("is_open", F.col("claim_status").isin("OPEN", "REOPENED"))
    .withColumn("accident_year", F.year("loss_date"))
    .withColumn("severity_band", F.when(F.col("incurred_amount") >= 1e7, "SEVERE")
                                  .when(F.col("incurred_amount") >= 1e6, "LARGE")
                                  .otherwise("ATTRITIONAL")))
align_to(late_fact, FACT_TBL).createOrReplaceTempView("late_fact")
spark.sql(f"""
MERGE INTO {FACT_TBL} t
USING late_fact s ON t.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

late_fraud = (spark.table(FACT_TBL).join(affected, "claim_id")
    .filter("fraud_flag = true OR litigation_flag = true OR report_lag_days > 60")
    .withColumn("triage_priority", F.when(F.col("fraud_score") >= 0.75, "P1")
                                    .when(F.col("fraud_score") >= 0.55, "P2").otherwise("P3")))
spark.sql(f"DELETE FROM {FRAUD_TBL} WHERE claim_id IN (SELECT claim_id FROM late_affected)")
align_to(late_fraud, FRAUD_TBL).write.mode("append").saveAsTable(FRAUD_TBL)
print(f"  claims.fact_claim merged for {affected.count()} claims; "
      f"{late_fraud.count()} routed to fraud triage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Required postings for the affected claims
# MAGIC Run the affected claims through the **same chart-of-accounts mapping as the Finance
# MAGIC Engine in notebook `08`**: Losses Paid (`5000`, dated `loss_date`) and Loss Reserves
# MAGIC (`5100`, dated `report_date`). The posting date's month is the **origin period**.
# MAGIC Both accounts are emitted even at zero, so a released reserve produces a reversal.

# COMMAND ----------

fact_aff = spark.table(FACT_TBL).join(affected, "claim_id")
required_lines = (
    fact_aff.select("claim_id", "line_of_business", "region",
                    F.coalesce("currency", F.lit("USD")).alias("currency"),
                    F.lit("5000").alias("account_code"), F.lit("Losses Paid").alias("account_name"),
                    F.lit("LOSS").alias("gl_category"),
                    F.col("paid_amount").cast("decimal(18,2)").alias("amount"),
                    F.col("loss_date").alias("origin_date"), "report_lag_days")
    .unionByName(fact_aff.select("claim_id", "line_of_business", "region",
                    F.coalesce("currency", F.lit("USD")).alias("currency"),
                    F.lit("5100").alias("account_code"), F.lit("Loss Reserves").alias("account_name"),
                    F.lit("RESERVE").alias("gl_category"),
                    F.col("reserve_amount").cast("decimal(18,2)").alias("amount"),
                    F.col("report_date").alias("origin_date"), "report_lag_days"))
    .withColumn("origin_period", F.date_format("origin_date", "yyyy-MM")))
required_lines.createOrReplaceTempView("required_lines")
print(f"  {required_lines.count()} required claim × account positions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Compare with the booked ledger and build the delta journals
# MAGIC `booked` = everything already in `gl_detail` for these claims (originals plus any
# MAGIC earlier adjustments), summed by claim × account × origin period. The journal amount
# MAGIC is `required − booked`. Where the origin period is **CLOSED**, the journal is posted in
# MAGIC the **booking period** as a `PRIOR_PERIOD_ADJ`; otherwise in the origin period as a
# MAGIC `LATE_ADJ`. Zero deltas produce nothing, which is what makes re-runs idempotent.

# COMMAND ----------

journals = spark.sql(f"""
WITH booked AS (
  SELECT source_ref AS claim_id, account_code, origin_period,
         SUM(amount) AS booked, any_value(account_name) AS account_name,
         any_value(gl_category) AS gl_category, any_value(line_of_business) AS line_of_business,
         any_value(region) AS region, any_value(currency) AS currency
  FROM {GL_TBL}
  WHERE ref_type = 'CLAIM' AND source_ref IN (SELECT claim_id FROM late_affected)
    AND account_code IN ('5000', '5100')
  GROUP BY ALL
),
cmp AS (
  SELECT coalesce(r.claim_id, b.claim_id)                 AS claim_id,
         coalesce(r.account_code, b.account_code)         AS account_code,
         coalesce(r.origin_period, b.origin_period)       AS origin_period,
         coalesce(r.account_name, b.account_name)         AS account_name,
         coalesce(r.gl_category, b.gl_category)           AS gl_category,
         coalesce(r.line_of_business, b.line_of_business) AS line_of_business,
         coalesce(r.region, b.region)                     AS region,
         coalesce(r.currency, b.currency)                 AS currency,
         r.origin_date, r.report_lag_days,
         CAST(coalesce(b.booked, 0) AS DECIMAL(18,2))     AS previously_booked,
         CAST(coalesce(r.amount, 0) AS DECIMAL(18,2))     AS now_required
  FROM required_lines r
  FULL OUTER JOIN booked b
    ON r.claim_id = b.claim_id AND r.account_code = b.account_code AND r.origin_period = b.origin_period
)
SELECT c.*,
       CAST(now_required - previously_booked AS DECIMAL(18,2))          AS adjustment,
       coalesce(pc.status, 'OPEN')                                       AS origin_status,
       CASE WHEN coalesce(pc.status, 'OPEN') = 'CLOSED'
            THEN 'PRIOR_PERIOD_ADJ' ELSE 'LATE_ADJ' END                  AS entry_type,
       CASE WHEN coalesce(pc.status, 'OPEN') = 'CLOSED'
            THEN '{BOOKING_PERIOD}' ELSE c.origin_period END             AS posting_period,
       CASE WHEN coalesce(pc.status, 'OPEN') = 'CLOSED'
            THEN DATE'{BOOKING_DATE}'
            ELSE coalesce(c.origin_date, to_date(c.origin_period || '-01')) END AS posting_date
FROM cmp c
LEFT JOIN {PC_TBL} pc ON pc.posting_period = c.origin_period
WHERE now_required - previously_booked <> 0
""").join(late_types, "claim_id", "left").cache()
journals.createOrReplaceTempView("journals")

print("  delta journals by entry type:")
journals.groupBy("entry_type", "gl_category").agg(
    F.count("*").alias("lines"), F.sum("adjustment").alias("adjustment")).orderBy("entry_type").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Post to EFR and rebuild only what changed
# MAGIC 7a appends the journals to `gl_detail` (insert-only `MERGE` on a deterministic
# MAGIC `gl_id`). 7b rebuilds `trial_balance` for the touched **posting** periods (open periods
# MAGIC only, by construction) and `trial_balance_by_origin` for the touched **origin**
# MAGIC periods. 7c is a **control check**: the booked balances of every closed period must be
# MAGIC identical before and after, or the notebook fails.

# COMMAND ----------

posting_periods = sorted(r[0] for r in journals.select("posting_period").distinct().collect())
origin_periods = sorted(r[0] for r in journals.select("origin_period").distinct().collect())
closed_periods = [r[0] for r in spark.sql(f"SELECT posting_period FROM {PC_TBL} WHERE status='CLOSED'").collect()]


def in_list(xs):
    return ", ".join(f"'{x}'" for x in xs) or "''"


def closed_tb_fingerprint():
    return spark.sql(f"""SELECT count(*) n, CAST(coalesce(SUM(balance), 0) AS DECIMAL(38,2)) total,
                         coalesce(SUM(abs(balance) * (pmod(hash(account_code, line_of_business, region,
                         posting_period), 997) + 1)), 0) checksum
                         FROM {TB_TBL} WHERE posting_period IN ({in_list(closed_periods)})""").first()


def fr_booked(periods):
    return {r["posting_period"]: r for r in spark.sql(f"""
        SELECT posting_period, ROUND(SUM(losses_paid + reserves), 2) AS losses,
               ROUND(SUM(losses_paid + reserves) / NULLIF(SUM(premium_revenue), 0), 4) AS loss_ratio,
               ROUND(SUM(losses_paid + reserves + expenses) / NULLIF(SUM(premium_revenue), 0), 4) AS combined_ratio
        FROM {fq('reporting', 'finance_reporting')} WHERE posting_period IN ({in_list(periods)})
        GROUP BY posting_period""").collect()}


def fr_origin(periods):
    return {r["origin_period"]: r for r in spark.sql(f"""
        SELECT origin_period, ROUND(SUM(losses_paid + reserves), 2) AS losses,
               ROUND(SUM(losses_paid + reserves) / NULLIF(SUM(premium_revenue), 0), 4) AS loss_ratio,
               ROUND(SUM(losses_paid + reserves + expenses) / NULLIF(SUM(premium_revenue), 0), 4) AS combined_ratio
        FROM {fq('reporting', 'finance_reporting_by_origin')} WHERE origin_period IN ({in_list(periods)})
        GROUP BY origin_period""").collect()}


has_origin_view = spark.catalog.tableExists(fq("reporting", "finance_reporting_by_origin"))
fp_before = closed_tb_fingerprint()
booked_before = fr_booked(posting_periods)
origin_before = fr_origin(origin_periods) if has_origin_view else {}
gl_version_before = spark.sql(f"DESCRIBE HISTORY {GL_TBL}").agg(F.max("version")).first()[0]

# 7a · Append the journals (deterministic key => re-running the same batch inserts nothing twice)
spark.sql(f"""
MERGE INTO {GL_TBL} t
USING (
  SELECT claim_id AS source_ref, 'CLAIM' AS ref_type, line_of_business, region, currency,
         account_code, account_name, gl_category, adjustment AS amount, posting_date,
         concat_ws('-', 'CLAIM', claim_id, account_code, entry_type, posting_period, origin_period,
                   '{LATE_BATCH}') AS gl_id,
         posting_period, entry_type, origin_period
  FROM journals) s
ON t.gl_id = s.gl_id
WHEN NOT MATCHED THEN INSERT
  (source_ref, ref_type, line_of_business, region, currency, account_code, account_name,
   gl_category, amount, posting_date, gl_id, posting_period, entry_type, origin_period,
   _posted_at, _late_arrival, _booking_date, _batch_id)
  VALUES
  (s.source_ref, s.ref_type, s.line_of_business, s.region, s.currency, s.account_code,
   s.account_name, s.gl_category, s.amount, s.posting_date, s.gl_id, s.posting_period,
   s.entry_type, s.origin_period, current_timestamp(), true, DATE'{BOOKING_DATE}', '{LATE_BATCH}')
""")

# 7b · Rebuild only the touched periods of both trial balances
if posting_periods:
    spark.sql(f"DELETE FROM {TB_TBL} WHERE posting_period IN ({in_list(posting_periods)})")
    spark.sql(f"""INSERT INTO {TB_TBL}
        SELECT account_code, account_name, gl_category, line_of_business, region, posting_period,
               SUM(amount) AS balance, COUNT(*) AS posting_count
        FROM {GL_TBL} WHERE posting_period IN ({in_list(posting_periods)}) GROUP BY ALL""")
if origin_periods:
    spark.sql(f"DELETE FROM {TBO_TBL} WHERE origin_period IN ({in_list(origin_periods)})")
    spark.sql(f"""INSERT INTO {TBO_TBL}
        SELECT account_code, account_name, gl_category, line_of_business, region, origin_period,
               SUM(amount) AS balance, COUNT(*) AS posting_count
        FROM {GL_TBL} WHERE origin_period IN ({in_list(origin_periods)}) GROUP BY ALL""")

# 7c · Control: closed periods of the booked trial balance must not move
fp_after = closed_tb_fingerprint()
if (fp_before["n"], fp_before["total"], fp_before["checksum"]) != (fp_after["n"], fp_after["total"], fp_after["checksum"]):
    raise Exception(f"CONTROL BREACH: closed-period trial balance changed {fp_before} -> {fp_after}")
gl_version_after = spark.sql(f"DESCRIBE HISTORY {GL_TBL}").agg(F.max("version")).first()[0]
print(f"  posted {journals.count()} journals; gl_detail v{gl_version_before} -> v{gl_version_after}")
print(f"  touched posting periods {posting_periods}; restated origin periods: {len(origin_periods)}")
print(f"  CONTROL OK: {len(closed_periods)} closed periods unchanged "
      f"({fp_after['n']} balances, total {fp_after['total']})")

# Audit log (only when something was posted, so a re-run doesn't duplicate)
if journals.count():
    (journals.select(
        F.lit(LATE_BATCH).alias("batch_id"), F.to_date(F.lit(BOOKING_DATE)).alias("booking_date"),
        "claim_id", F.coalesce("late_type", F.lit("DEVELOPMENT")).alias("late_type"),
        "account_code", "gl_category", "line_of_business", "region", "origin_period",
        "posting_period", "entry_type", "previously_booked", "now_required", "adjustment",
        F.col("report_lag_days").cast("int").alias("report_lag_days"),
        F.current_timestamp().alias("logged_at"))
     .write.mode("append").saveAsTable(LOG_TBL))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Before / after: what the CFO sees
# MAGIC - **Booked** (`finance_reporting`): closed periods are untouched; the booking period
# MAGIC   carries the prior-period adjustments.
# MAGIC - **Restated** (`finance_reporting_by_origin`): each accident period now includes
# MAGIC   the losses that belong to it, however late they were reported.

# COMMAND ----------

booked_after = fr_booked(posting_periods)
print("  BOOKED view (finance_reporting) — periods that received journals:")
for p in posting_periods:
    b, a = booked_before.get(p), booked_after.get(p)
    print(f"    {p}: losses {b['losses'] if b else None} -> {a['losses'] if a else None}   "
          f"combined {b['combined_ratio'] if b else None} -> {a['combined_ratio'] if a else None}")

if has_origin_view:
    origin_after = fr_origin(origin_periods)
    rows = [(p, float(origin_before[p]["loss_ratio"]) if p in origin_before and origin_before[p]["loss_ratio"] is not None else None,
             float(origin_after[p]["loss_ratio"]) if p in origin_after and origin_after[p]["loss_ratio"] is not None else None,
             float(origin_before[p]["losses"] or 0) if p in origin_before else 0.0,
             float(origin_after[p]["losses"] or 0) if p in origin_after else 0.0) for p in origin_periods]
    cmp = (spark.createDataFrame(rows, "origin_period string, loss_ratio_before double, "
                                       "loss_ratio_after double, losses_before double, losses_after double")
           .withColumn("losses_delta", F.round(F.col("losses_after") - F.col("losses_before"), 2))
           .orderBy(F.abs("losses_delta").desc()))
    print("  RESTATED view (finance_reporting_by_origin) — largest restatements:")
    cmp.show(12, truncate=False)
else:
    print("  (re-run notebook 09 to create reporting.finance_reporting_by_origin)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Audit trail
# MAGIC `efr.late_arrival_log` records, per batch × claim × account, what was booked, what is
# MAGIC now required and the journal that bridged them. Delta time travel still shows the
# MAGIC ledger exactly as it was before this batch.

# COMMAND ----------

spark.sql(f"""SELECT entry_type, late_type, count(DISTINCT claim_id) claims, count(*) lines,
                     ROUND(SUM(adjustment), 2) adjustment
              FROM {LOG_TBL} WHERE batch_id = '{LATE_BATCH}' GROUP BY ALL ORDER BY ALL""").show()

before_total = spark.sql(f"""SELECT ROUND(SUM(amount), 2) FROM {GL_TBL} VERSION AS OF {gl_version_before}
                             WHERE gl_category IN ('LOSS', 'RESERVE')""").first()[0]
after_total = spark.sql(f"""SELECT ROUND(SUM(amount), 2) FROM {GL_TBL}
                            WHERE gl_category IN ('LOSS', 'RESERVE')""").first()[0]
print(f"  total booked claim cost: v{gl_version_before}={before_total}  ->  v{gl_version_after}={after_total}")
(spark.sql(f"DESCRIBE HISTORY {GL_TBL}").select("version", "timestamp", "operation")
    .orderBy(F.col("version").desc()).show(5, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: how a late claim fact reaches EFR
# MAGIC
# MAGIC | Layer | Table | Handling |
# MAGIC |---|---|---|
# MAGIC | Landing | `lz_raw.landing/ship/claim_late/<batch>` | Late extract lands as a file |
# MAGIC | Bronze | `lz_raw.claim` | Append + **ABC** row-count and control-total balance |
# MAGIC | Silver | `oz_organized.claim` | Conform + **DQ** (HIGH fails quarantined), `MERGE` on `claim_id` |
# MAGIC | Spoke | `claims.fact_claim`, `fraud_triage` | Recompute affected keys only, `MERGE` |
# MAGIC | EFR | `efr.gl_detail` | Append **delta journals**: `LATE_ADJ` (open origin) or `PRIOR_PERIOD_ADJ` (closed origin → booking period) |
# MAGIC | EFR | `efr.trial_balance` | Rebuild touched **posting** periods; closed periods proven unchanged |
# MAGIC | EFR | `efr.trial_balance_by_origin` | Rebuild touched **origin** periods (restated view) |
# MAGIC | EFR | `efr.period_control`, `efr.late_arrival_log` | Close status and full audit of every adjustment |
# MAGIC | Reporting | `finance_reporting` / `finance_reporting_by_origin` | Booked vs restated, no code change |
# MAGIC
# MAGIC **Reset:** re-run `databricks bundle run cfo_data_platform_pipeline` (full overwrite).

# COMMAND ----------

print(f"\nLate-arriving claims → EFR complete. Batch {LATE_BATCH}: "
      f"{len(posting_periods)} posting period(s), {len(origin_periods)} origin period(s) restated.")
