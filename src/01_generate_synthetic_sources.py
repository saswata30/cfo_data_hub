# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate synthetic source-system extracts
# MAGIC Simulates the **CFO Data Sources** column: each source system drops a raw
# MAGIC extract into the landing Volume (`/Volumes/<catalog>/lz_raw/landing/<system>/...`).
# MAGIC Data models commercial P&C / specialty / reinsurance underwriting (AXA XL-style):
# MAGIC parties + KYC, quotes, policies, fees, claims, FNOL, reinsurance treaties,
# MAGIC headcount, plan forecasts, actuarial factors and conformance rules.
# MAGIC
# MAGIC Deterministic (seeded) so re-runs are reproducible.

# COMMAND ----------

# MAGIC %pip install faker==30.3.0 -q

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import random, datetime as dt
from faker import Faker

fake = Faker()
SEED = SCALE["seed"]
random.seed(SEED)
Faker.seed(SEED)

REGIONS = ["US", "EMEA", "APAC", "BERMUDA"]
LOBS = ["Property", "Casualty", "Cyber", "Marine", "Energy", "Aviation", "D&O", "Political Risk"]
CURRENCIES = {"US": "USD", "EMEA": "EUR", "APAC": "SGD", "BERMUDA": "USD"}


def wchoice(options):
    """Weighted choice: options = [(value, weight), ...]."""
    vals, wts = zip(*options)
    return random.choices(vals, weights=wts, k=1)[0]


def rdate(start_year=2023, end_year=2026):
    start = dt.date(start_year, 1, 1)
    return start + dt.timedelta(days=random.randint(0, (dt.date(end_year, 12, 31) - start).days))


NOW = dt.datetime(2026, 9, 28, 12, 0, 0)

# COMMAND ----------

# MAGIC %md ### Parties (insureds & reinsurers) + KYC  → feeds HUB Primary

# COMMAND ----------

parties = []
for i in range(SCALE["parties"]):
    region = wchoice([("US", 45), ("EMEA", 35), ("APAC", 12), ("BERMUDA", 8)])
    kind = wchoice([("ORGANIZATION", 82), ("INDIVIDUAL", 18)])
    ptype = wchoice([("INSURED", 88), ("REINSURER", 12)])
    is_org = kind == "ORGANIZATION"
    kyc_rating = wchoice([("LOW", 60), ("MEDIUM", 28), ("HIGH", 12)])
    parties.append({
        "party_id": f"PTY{i:06d}",
        "party_type": ptype,
        "party_kind": kind,
        "legal_name": fake.company() if is_org else fake.name(),
        "tax_id": fake.bothify("##-#######") if is_org else fake.ssn(),   # PII
        "email": fake.company_email() if is_org else fake.email(),         # PII
        "phone": fake.phone_number(),                                      # PII
        "date_of_birth": None if is_org else fake.date_of_birth(minimum_age=25, maximum_age=80).isoformat(),  # PII
        "address_line": fake.street_address(),
        "city": fake.city(),
        "country": region,
        "postal_code": fake.postcode(),
        "kyc_status": wchoice([("CLEARED", 78), ("PENDING", 15), ("REVIEW", 7)]),
        "kyc_risk_rating": kyc_rating,
        "sanctions_checked": random.random() > 0.05,
        "pep_flag": random.random() < (0.10 if kyc_rating == "HIGH" else 0.02),
        "region": region,
        "source_system": "swiss_genius" if region in ("EMEA", "APAC") else "us_genius",
        "extract_ts": (NOW - dt.timedelta(minutes=random.randint(0, 240))).isoformat(),
    })

party_ids = [p["party_id"] for p in parties]
insured_ids = [p["party_id"] for p in parties if p["party_type"] == "INSURED"]
reinsurer_ids = [p["party_id"] for p in parties if p["party_type"] == "REINSURER"]
print(f"parties={len(parties)}  insureds={len(insured_ids)}  reinsurers={len(reinsurer_ids)}")

# COMMAND ----------

# MAGIC %md ### Producers (brokers / MGAs)  → landing/wins

# COMMAND ----------

producers = []
for i in range(SCALE["producers"]):
    producers.append({
        "producer_id": f"PRD{i:05d}",
        "producer_name": fake.company() + wchoice([(" Brokers", 5), (" Risk Partners", 3), (" Underwriting", 2)]),
        "producer_type": wchoice([("BROKER", 70), ("MGA", 20), ("DIRECT", 10)]),
        "commission_rate": round(random.uniform(0.05, 0.20), 3),
        "country": random.choice(REGIONS),
        "source_system": "wins",
        "extract_ts": NOW.isoformat(),
    })
producer_ids = [p["producer_id"] for p in producers]

# COMMAND ----------

# MAGIC %md ### Quotes (Underwriting)  → landing/us_genius, swiss_genius, wins

# COMMAND ----------

quotes = []
for i in range(SCALE["quotes"]):
    region = wchoice([("US", 45), ("EMEA", 35), ("APAC", 12), ("BERMUDA", 8)])
    lob = random.choice(LOBS)
    sum_insured = round(random.uniform(2.5e5, 5.0e8), 2)
    risk_score = min(100, max(1, int(random.gauss(52, 20))))
    tech_price = round(sum_insured * random.uniform(0.0008, 0.02) * (1 + (risk_score - 50) / 120), 2)
    status = wchoice([("BOUND", 42), ("QUOTED", 28), ("DECLINED", 18), ("LAPSED", 12)])
    # US Genius handles US; Swiss Genius handles EMEA/APAC; WINS handles specialty lines
    if lob in ("Cyber", "Marine", "Aviation", "Political Risk"):
        src = "wins"
    else:
        src = "us_genius" if region in ("US", "BERMUDA") else "swiss_genius"
    quotes.append({
        "quote_id": f"QTE{i:07d}",
        "party_id": random.choice(insured_ids),
        "producer_id": random.choice(producer_ids),
        "line_of_business": lob,
        "region": region,
        "industry_sector": random.choice(
            ["Energy", "Manufacturing", "Financial Services", "Healthcare", "Technology",
             "Construction", "Transport", "Real Estate", "Public Sector"]),
        "sum_insured": sum_insured,
        "requested_limit": round(sum_insured * random.uniform(0.2, 1.0), 2),
        "deductible": round(sum_insured * random.uniform(0.001, 0.05), 2),
        "risk_score": risk_score,
        "tech_price": tech_price,
        "premium_quoted": round(tech_price * random.uniform(0.85, 1.25), 2),
        "quote_status": status,
        "quote_date": rdate(2024, 2026).isoformat(),
        "underwriter": fake.name(),
        "currency": CURRENCIES[region],
        "source_system": src,
        "extract_ts": NOW.isoformat(),
    })
bound_quotes = [q for q in quotes if q["quote_status"] == "BOUND"]
print(f"quotes={len(quotes)}  bound={len(bound_quotes)}")

# COMMAND ----------

# MAGIC %md ### Policies (Policy)  → landing/procede   +   Fees & Billing → landing/guidewire_billing

# COMMAND ----------

policies, fees, billing = [], [], []
for i in range(SCALE["policies"]):
    q = random.choice(bound_quotes) if bound_quotes and random.random() < 0.9 else None
    region = q["region"] if q else random.choice(REGIONS)
    lob = q["line_of_business"] if q else random.choice(LOBS)
    party_id = q["party_id"] if q else random.choice(insured_ids)
    producer_id = q["producer_id"] if q else random.choice(producer_ids)
    sum_insured = q["sum_insured"] if q else round(random.uniform(2.5e5, 5.0e8), 2)
    gwp = round((q["premium_quoted"] if q else sum_insured * 0.01) * random.uniform(0.95, 1.05), 2)
    inception = rdate(2024, 2026)
    pid = f"POL{i:07d}"
    status = wchoice([("INFORCE", 58), ("EXPIRED", 22), ("RENEWED", 12), ("CANCELLED", 8)])
    policies.append({
        "policy_id": pid,
        "quote_id": q["quote_id"] if q else None,
        "party_id": party_id,
        "producer_id": producer_id,
        "line_of_business": lob,
        "region": region,
        "inception_date": inception.isoformat(),
        "expiry_date": (inception + dt.timedelta(days=365)).isoformat(),
        "sum_insured": sum_insured,
        "policy_limit": round(sum_insured * random.uniform(0.2, 1.0), 2),
        "deductible": round(sum_insured * random.uniform(0.001, 0.05), 2),
        "gross_written_premium": gwp,
        "net_premium": round(gwp * random.uniform(0.78, 0.92), 2),
        "currency": CURRENCIES[region],
        "policy_status": status,
        "source_system": "procede",
        "extract_ts": NOW.isoformat(),
    })
    # Fees (Guidewire Billing Center)
    for ft, share in [("POLICY_FEE", 0.02), ("BROKER_COMMISSION", 0.12), ("TAX", 0.05), ("SURCHARGE", 0.015)]:
        if random.random() < 0.72:
            fees.append({
                "fee_id": f"FEE{len(fees):08d}",
                "policy_id": pid,
                "fee_type": ft,
                "amount": round(gwp * share * random.uniform(0.6, 1.4), 2),
                "currency": CURRENCIES[region],
                "billed_date": (inception + dt.timedelta(days=random.randint(0, 60))).isoformat(),
                "paid_flag": random.random() > 0.18,
                "source_system": "guidewire_billing",
                "extract_ts": NOW.isoformat(),
            })
    # Billing transactions (premium installments)
    n_inst = random.choice([1, 1, 2, 4])
    for k in range(n_inst):
        billing.append({
            "txn_id": f"TXN{len(billing):09d}",
            "policy_id": pid,
            "txn_type": wchoice([("PREMIUM", 80), ("INSTALLMENT", 15), ("REFUND", 5)]),
            "amount": round(gwp / n_inst, 2),
            "currency": CURRENCIES[region],
            "txn_date": (inception + dt.timedelta(days=random.randint(0, 300))).isoformat(),
            "status": wchoice([("SETTLED", 82), ("PENDING", 13), ("FAILED", 5)]),
            "source_system": "guidewire_billing",
            "extract_ts": NOW.isoformat(),
        })
policy_index = {p["policy_id"]: p for p in policies}
print(f"policies={len(policies)}  fees={len(fees)}  billing_txns={len(billing)}")

# COMMAND ----------

# MAGIC %md ### Claims & FNOL (Claims)  → landing/ship

# COMMAND ----------

CAUSES = ["Fire", "Flood", "Windstorm", "Cyber Breach", "Business Interruption", "Liability",
          "Cargo Loss", "Hull Damage", "Theft", "Professional Negligence", "Product Recall"]
claims, fnols = [], []
pol_ids = list(policy_index.keys())
for i in range(SCALE["claims"]):
    pol = policy_index[random.choice(pol_ids)]
    incept = dt.date.fromisoformat(pol["inception_date"])
    loss_date = incept + dt.timedelta(days=random.randint(5, 360))
    report_lag = random.randint(0, 90)
    report_date = loss_date + dt.timedelta(days=report_lag)
    incurred = round(pol["gross_written_premium"] * random.uniform(0.1, 6.0), 2)
    status = wchoice([("CLOSED", 46), ("OPEN", 34), ("REOPENED", 8), ("DENIED", 12)])
    paid = round(incurred * (random.uniform(0.7, 1.0) if status == "CLOSED" else random.uniform(0.0, 0.6)), 2)
    reserve = round(max(0.0, incurred - paid), 2)
    fraud_score = round(min(1.0, max(0.0, random.gauss(0.18, 0.16) + (0.25 if report_lag > 60 else 0))), 3)
    cid = f"CLM{i:07d}"
    claims.append({
        "claim_id": cid,
        "policy_id": pol["policy_id"],
        "party_id": pol["party_id"],
        "line_of_business": pol["line_of_business"],
        "region": pol["region"],
        "loss_date": loss_date.isoformat(),
        "report_date": report_date.isoformat(),
        "claim_status": status,
        "cause_of_loss": random.choice(CAUSES),
        "incurred_amount": incurred,
        "paid_amount": paid,
        "reserve_amount": reserve,
        "currency": pol["currency"],
        "fraud_score": fraud_score,
        "fraud_flag": fraud_score >= 0.55,
        "litigation_flag": random.random() < 0.09,
        "source_system": "ship",
        "extract_ts": NOW.isoformat(),
    })
    fnols.append({
        "fnol_id": f"FNL{i:07d}",
        "claim_id": cid,
        "policy_id": pol["policy_id"],
        "notified_date": report_date.isoformat(),
        "channel": wchoice([("BROKER", 40), ("PORTAL", 25), ("EMAIL", 20), ("PHONE", 15)]),
        "description": random.choice(CAUSES) + " reported by insured",
        "catastrophe_flag": random.random() < 0.06,
        "source_system": "ship",
        "extract_ts": NOW.isoformat(),
    })
print(f"claims={len(claims)}  fnol={len(fnols)}")

# COMMAND ----------

# MAGIC %md ### Reinsurance treaties  → landing/alt_capital, rdu, cash

# COMMAND ----------

reins = []
for i in range(SCALE["reinsurance"]):
    ttype = wchoice([("QUOTA_SHARE", 40), ("XOL", 45), ("FAC", 15)])
    src = {"QUOTA_SHARE": "alt_capital", "XOL": "rdu", "FAC": "cash"}[ttype]
    limit = round(random.uniform(5e6, 5e8), 2)
    reins.append({
        "treaty_id": f"TRT{i:05d}",
        "reinsurer_party_id": random.choice(reinsurer_ids) if reinsurer_ids else random.choice(party_ids),
        "treaty_type": ttype,
        "line_of_business": random.choice(LOBS),
        "ceded_share": round(random.uniform(0.1, 0.6), 3),
        "attachment_point": round(limit * random.uniform(0.05, 0.3), 2),
        "treaty_limit": limit,
        "ceded_premium": round(limit * random.uniform(0.01, 0.08), 2),
        "inception_date": rdate(2024, 2026).isoformat(),
        "source_system": src,
        "extract_ts": NOW.isoformat(),
    })
print(f"reinsurance={len(reins)}")

# COMMAND ----------

# MAGIC %md ### myHR headcount, Anaplan plan, Copernic actuarial factors, Conformance rules

# COMMAND ----------

DEPTS = ["Underwriting", "Claims", "Finance", "Actuarial", "Risk & Compliance", "Operations"]
headcount = [{
    "employee_id": f"EMP{i:05d}",
    "department": random.choice(DEPTS),
    "region": random.choice(REGIONS),
    "fte": round(random.choice([0.5, 0.8, 1.0, 1.0, 1.0]), 1),
    "annual_cost": round(random.uniform(65000, 320000), 2),
    "cost_center": f"CC{random.randint(1000, 1099)}",
    "source_system": "myhr",
    "extract_ts": NOW.isoformat(),
} for i in range(SCALE["headcount"])]

plan = []
for cc in [f"CC{n}" for n in range(1000, 1020)]:
    for lob in LOBS:
        for m in range(1, 13):
            plan.append({
                "plan_id": f"PLN{len(plan):06d}",
                "cost_center": cc, "line_of_business": lob, "period": f"2026-{m:02d}",
                "metric": random.choice(["GWP", "LOSS", "EXPENSE"]),
                "planned_amount": round(random.uniform(1e5, 5e7), 2),
                "source_system": "anaplan", "extract_ts": NOW.isoformat(),
            })

reserve_factors = []
for lob in LOBS:
    for ay in range(2021, 2027):
        for dev in range(1, 6):
            reserve_factors.append({
                "factor_id": f"RF{len(reserve_factors):05d}",
                "line_of_business": lob, "accident_year": ay, "development_period": dev,
                "ldf": round(random.uniform(1.0, 2.2) / dev + 0.9, 4),
                "ultimate_loss_ratio": round(random.uniform(0.45, 0.95), 3),
                "source_system": "copernic", "extract_ts": NOW.isoformat(),
            })

conformance_rules = [
    {"rule_id": "CR001", "dataset": "policy", "column_name": "gross_written_premium", "rule_type": "RANGE", "severity": "HIGH", "description": "GWP must be > 0"},
    {"rule_id": "CR002", "dataset": "party", "column_name": "tax_id", "rule_type": "NOT_NULL", "severity": "HIGH", "description": "Tax ID required for KYC"},
    {"rule_id": "CR003", "dataset": "claim", "column_name": "policy_id", "rule_type": "REFERENTIAL", "severity": "HIGH", "description": "Claim must reference a valid policy"},
    {"rule_id": "CR004", "dataset": "party", "column_name": "email", "rule_type": "REGEX", "severity": "MEDIUM", "description": "Email format check"},
    {"rule_id": "CR005", "dataset": "quote", "column_name": "risk_score", "rule_type": "RANGE", "severity": "MEDIUM", "description": "Risk score between 1 and 100"},
]
conformance_rules = [{**r, "source_system": "conformance", "extract_ts": NOW.isoformat()} for r in conformance_rules]

# COMMAND ----------

# MAGIC %md ### Write extracts to the landing Volume, split by source system

# COMMAND ----------

def write_landing(rows, entity):
    """Write rows to /Volumes/.../landing/<source_system>/<entity>, one folder per source."""
    if not rows:
        return
    df = spark.createDataFrame(rows)
    sources = [r[0] for r in df.select("source_system").distinct().collect()]
    for src in sources:
        path = f"{landing_path(src)}/{entity}"
        (df.filter(df.source_system == src)
           .write.mode("overwrite").parquet(path))
        print(f"  landed {entity:22s} -> {path}")

show_header("01 · Landing synthetic extracts")
write_landing(parties, "party")
write_landing(producers, "producer")
write_landing(quotes, "quote")
write_landing(policies, "policy")
write_landing(fees, "policy_fee")
write_landing(billing, "billing_transaction")
write_landing(claims, "claim")
write_landing(fnols, "fnol")
write_landing(reins, "reinsurance_contract")
write_landing(headcount, "headcount")
write_landing(plan, "plan_forecast")
write_landing(reserve_factors, "reserve_factor")
write_landing(conformance_rules, "conformance_rule")
print("\nSynthetic source generation complete.")
