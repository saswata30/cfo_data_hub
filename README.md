# CFO Data Platform — PoC

A runnable proof-of-concept of the **CFO Data Architecture** on Databricks: a medallion
lakehouse with a **Hub & Spoke data mesh**, an **Enterprise Finance Reporting (EFR)**
gold layer, CFO reporting marts, and **Unity Catalog** governance spanning every layer.

It ships with a synthetic commercial P&C / specialty / reinsurance dataset (AXA XL-style)
so the whole thing runs end-to-end in any Databricks workspace with no external data.

Source → Raw ingestion is wrapped by an **Audit, Balance & Control (ABC)** framework, and
the Silver layer runs a declarative **Data Quality (DQ)** engine — deterministic rules **plus
AI-Function checks** (Databricks `ai_query`) for semantic/contextual quality — both writing a
queryable control/audit trail to the `abc_control` schema.

> Sources → ⟨**ABC** gate⟩ → Raw (LZ / Bronze) → Organized Zone (Silver **+ DQ**) → Hub & Spoke + EFR (Gold) → CFO Reporting — governed by Unity Catalog.

## Use-case story

*Why this platform exists, who it serves, and what changes for the finance organisation of
a global commercial insurer once it is live. The code in this repo is the runnable proof of
that story on Databricks.*

### The organisation

**Persona:** the Group CFO (and the finance, actuarial, and risk leaders who report into
the office of the CFO) of a large **commercial P&C / specialty / reinsurance** insurer —
the AXA XL-style carrier the synthetic dataset is modelled on.

The finance function sits *downstream of everything*: underwriting binds risk, policy
administration books premium, claims pays losses, reinsurance cedes exposure, and HR
carries the cost base. Every one of those domains runs on its own system of record —
US & Swiss **Genius**, **Procede**, **WINS**, **Guidewire**, **Anaplan**, **Copernic**,
**myHR**, **SHIP**, plus reinsurance sources (Alternative Capital, RDU, Cash). The CFO's
job is to turn all of it into one trusted, auditable, timely view of the group's financial
and underwriting performance — the loss ratio, expense ratio, and combined ratio that the
board, regulators, and rating agencies live and die by.

### The problem

Today that view is assembled the hard way:

- **Fragmented sources, no conformed keys.** The same broker, insured, or policy is
  represented differently in Genius, Guidewire, and Anaplan. Reconciling premium against
  losses against exposure means a central team manually stitching identifiers together.
- **Reconciliation is a fire drill, not a control.** Nobody can prove that what left the
  source systems actually arrived in the warehouse — row-for-row, penny-for-penny — so
  every close is shadowed by manual tie-outs and "why doesn't this number match?" emails.
- **Data quality is discovered in the boardroom.** A claim pointing at a non-existent
  policy, an implausible cause of loss, gross written premium booked as a negative — these
  surface *after* they've contaminated a ratio, not before.
- **Governance is bolted on.** PII and KYC data (tax IDs, dates of birth, sanctions/PEP
  status) live in spreadsheets and extracts with inconsistent masking, making every
  regulatory review and audit slow and risky.
- **One monolithic warehouse, one central bottleneck.** A single team models everything,
  so underwriting can't evolve its data without risking the claims or finance reports.

The net effect: a **slow, low-trust close**, actuaries and analysts spending their time
reconciling instead of analysing, and a CFO who cannot answer "*is this number right, and
can you prove it?*" without a week of manual work.

### The solution — the CFO Data Hub

A single **governed lakehouse on Databricks** that industrialises the whole journey from
source extract to CFO report, with control and governance built into every layer rather
than bolted on at the end. Four ideas carry the design:

1. **A medallion pipeline** (Bronze → Silver → Gold) refines data in disciplined stages —
   raw and immutable, then cleaned and conformed, then business-ready.
2. **A hub-and-spoke data mesh** gives each domain — Underwriting, Policy, Claims —
   ownership of its own curated *data product*, all conforming to master data (party,
   producer, KYC) published once by the **HUB**. Domains evolve independently; the numbers
   still reconcile because everyone joins to the same golden keys.
3. **Audit, Balance & Control (ABC) + a Data Quality engine** make trust *provable*. Every
   ingestion is reconciled source-vs-raw on row count and a numeric control total (e.g.
   `SUM(gross_written_premium)` in = out), and a control gate stops the run before bad data
   spreads. In Silver, a declarative DQ engine enforces business rules — including
   **AI-Function semantic checks** that catch what deterministic rules can't — and writes a
   queryable audit trail.
4. **Unity Catalog governs the lot** — lineage, access control, and PII/KYC masking across
   every schema, from raw landing to CFO report.

The result is the **Enterprise Finance Reporting (EFR)** gold layer: a general ledger,
trial balance, and reinsurance-ceded posting built from the domain data products, feeding
CFO reporting marts for actuarial, risk & fraud, policy, claims, and finance.

### How the data flows (the journey of one policy)

Follow a single bound policy through the platform:

| Stage | What happens | Where it lives |
|---|---|---|
| **Source** | The policy is booked in Genius; an extract lands as a file. | `lz_raw.landing` (Volume) |
| **ABC gate** | Landed rows and `SUM(gross_written_premium)` are reconciled against the raw target. A mismatch raises a control exception and **fails the run**. | `abc_control.ingestion_audit` |
| **Bronze** | The raw policy lands, immutable, stamped with ingestion lineage. | `lz_raw` |
| **Silver + DQ** | Typed, deduped, conformed. Rules check GWP > 0, referential integrity to a real party; AI checks the legal name is a real entity. HIGH-severity failures are quarantined. | `oz_organized` (+ `_dq_quarantine`) |
| **HUB** | The policy's insured and broker are resolved to golden `dim_party` / `dim_producer` keys. | `hub` |
| **Spoke — Policy** | Published as a `fact_policy` data product (GWP, net premium, in-force). | `policy` |
| **EFR (Gold)** | The Finance Engine posts the premium to GL account 4000; the matching claim posts losses to 5000/5100. | `efr.gl_detail` |
| **CFO Reporting** | The trial balance derives loss / expense / **combined ratio** by LOB, region, period. | `reporting.finance_reporting` |

Every hop is governed by Unity Catalog and leaves an audit trail — so the final combined
ratio is not just a number, it's a number you can **trace back to the source extract and
prove was reconciled and quality-checked at every step.**

### Business outcomes

- **A close you can trust — and prove.** Source-to-raw reconciliation and a DQ audit trail
  turn manual tie-outs into an automated control. The CFO can answer "is it right?" with
  evidence, not faith.
- **Faster time to insight.** Conformed keys and pre-built reporting marts mean actuaries
  and analysts consume ready data instead of reconciling it — days of manual prep removed
  from every reporting cycle.
- **Bad data caught early, not in the boardroom.** Deterministic + AI-powered DQ quarantines
  or flags problems in Silver, before they reach a ratio or a regulatory filing.
- **Regulatory & audit readiness by design.** PII/KYC masking, classification tags, and
  end-to-end lineage make Solvency II, IFRS 17, and audit reviews a query, not a project.
- **Domain agility without chaos.** Underwriting, Policy, and Claims each own and evolve
  their data product; the hub contract keeps everything reconciling.
- **One platform, every workload.** Ingestion, transformation, data quality, AI, BI, and
  governance on a single lakehouse — no copies, no bolt-ons, no data leaving the estate.

### Why Databricks

- **Lakehouse + Unity Catalog** — one governed copy of the data serves finance, actuarial,
  risk, and data science, with lineage, access control, and masking native to the platform.
- **Medallion + data mesh on open Delta** — the refinement discipline and domain ownership
  this story needs, on an open format, no lock-in.
- **AI Functions built in** — `ai_query` brings LLM-powered semantic data-quality checks
  into plain SQL, governed and metered, with no separate AI stack to run.
- **Serverless & Asset Bundles** — the entire pipeline deploys and runs as a serverless,
  chained job (`databricks bundle run`), reproducibly, in any workspace.
- **A clear path to production** — swap synthetic generation for **Lakeflow Connect** on the
  real Genius / Guidewire / Anaplan extracts, add a **Genie space** for natural-language
  CFO queries, and promote reporting to **Lakeview dashboards** — all extensions of the same
  governed platform, not rebuilds.

## Architecture

![CFO Data Hub architecture flow](docs/architecture-flow.svg)

<details>
<summary>Text (Mermaid) version</summary>

```mermaid
flowchart LR
  subgraph SRC["CFO Data Sources"]
    direction TB
    S1["US / Swiss Genius · Procede<br/>WINS · Guidewire · Anaplan<br/>Copernic · myHR · SHIP"]
    S2["Reinsurance:<br/>Alt Capital · RDU · Cash"]
  end

  ABCGATE{{"ABC gate<br/><i>audit · balance · control</i>"}}

  subgraph LZ["Raw Data (LZ) · Bronze<br/><i>lz_raw</i>"]
    B["party · quote · policy · fee<br/>claim · fnol · reinsurance · hr"]
  end

  subgraph LAKE["CFO Data Lake (OZ & SZ)"]
    direction TB
    OZ["Organized Zone · Silver<br/><i>oz_organized</i><br/>cleaned · conformed · deduped<br/><b>+ DQ engine</b> (rules + AI · flag · quarantine)"]
    subgraph MESH["Hub & Spoke Data Mesh"]
      direction LR
      HUB(("HUB Primary<br/><i>hub</i><br/>Master Data<br/>Profiles · KYC"))
      UW["Underwriting<br/><i>Risk, Quotes</i>"]
      POL["Policy<br/><i>Policies, Fees</i>"]
      CLM["Claims<br/><i>FNOL, Claims</i>"]
    end
    EFR["EFR · Gold<br/><i>efr</i><br/>GL · FAH-Detail"]
  end

  subgraph RPT["CFO Reporting<br/><i>reporting</i>"]
    direction TB
    R["Actuarial · Risk & Fraud<br/>Policy · Claims · Finance"]
  end

  ABC[["Audit, Balance & Control<br/><i>abc_control</i><br/>ingestion_audit · control_exception<br/>dq_result · *_dq_quarantine"]]

  GOV["Data Security & Governance — Unity Catalog<br/>Lineage · Access Control · PII/KYC Masking · Audit — across every layer"]

  SRC --> ABCGATE --> LZ --> OZ --> HUB
  HUB --> UW & POL & CLM
  UW & POL & CLM --> EFR --> RPT
  ABCGATE -.audit + balance.-> ABC
  OZ -.DQ results.-> ABC
  GOV -.governs.- LZ & LAKE & RPT
```

</details>

Each box maps to code:

| Architecture element | Unity Catalog schema | Notebook |
|---|---|---|
| CFO Data Sources (synthetic extracts) | `lz_raw.landing` (Volume) | `01_generate_synthetic_sources` |
| Raw Data (LZ) — Bronze **+ ABC controls** | `lz_raw` | `02_landing_zone_bronze` (+ `_abc`) |
| Organized Zone — Silver **+ DQ (rules + AI)** | `oz_organized` (+ `*_dq_quarantine`, `*_dq_ai_flagged`) | `03_organized_zone_silver` (+ `_dq`, `_dq_ai`) |
| HUB Primary (Master Data, KYC) | `hub` | `04_hub_master_data` |
| Spoke — Underwriting (Risk, Quotes) | `underwriting` | `05_spoke_underwriting` |
| Spoke — Policy (Policies, Fees) | `policy` | `06_spoke_policy` |
| Spoke — Claims (FNOL, Claims) | `claims` | `07_spoke_claims` |
| EFR — Finance Engine (GL, FAH) | `efr` | `08_efr_semantic_gold` |
| CFO Reporting marts | `reporting` | `09_reporting_marts` |
| Audit, Balance & Control + DQ results | `abc_control` | `_abc`, `_dq` (invoked by `02` / `03`) |
| Data Security & Governance | *(all schemas)* | `00_setup_unity_catalog`, `10_governance_masking` |

## What the pipeline builds

- **13 source-system extracts** landed to a Volume (parties+KYC, quotes, policies, fees,
  billing, claims, FNOL, reinsurance treaties, headcount, plan, actuarial factors, DQ rules).
- **Bronze** raw Delta tables with ingestion lineage; **Silver** conformed/deduped entities.
- **Audit, Balance & Control (ABC)** on Source → Raw: every entity's landed source is
  reconciled against the raw target on **row count** and a **numeric control total**
  (e.g. `SUM(gross_written_premium)` in = out); results append to
  `abc_control.ingestion_audit`, breaches to `abc_control.control_exception`, and a
  **control gate fails the run** on a hard breach (nothing landed / row-count imbalance).
- **Silver Data Quality (DQ)**: a declarative expectation set per entity (not-null, range,
  allowed-set, regex, **referential integrity** — e.g. every `claim.policy_id` must resolve
  to a policy). Rows are flagged, HIGH-severity failures routed to `<entity>_dq_quarantine`,
  and per-rule pass/fail metrics logged to `abc_control.dq_result`.
- **AI-powered DQ** (Databricks **AI Functions**, `ai_query` with structured output) for the
  semantic checks rules can't express: is a `party.legal_name` a real entity vs. gibberish;
  is a claim's `cause_of_loss` plausible for its `line_of_business`; does an FNOL
  `description` match the coded cause. **Advisory** (flag + log, never quarantine),
  **sampled** and **non-fatal**; results in `abc_control.dq_ai_result`, flagged rows in
  `<entity>_dq_ai_flagged`. Toggle/tune in `src/_common.py` (`AI_DQ_ENABLED`,
  `AI_DQ_MODEL`, `AI_DQ_SAMPLE_ROWS`) — the effective values; `conf/config.yml`
  mirrors them under `controls.ai_dq` for documentation.
- **HUB Primary**: `dim_party`, `dim_producer`, `party_kyc_profile` — the golden keys
  every spoke conforms to.
- **Three domain spokes** publishing data products: Underwriting (`fact_quote`,
  `risk_appetite_summary`), Policy (`fact_policy`, `premium_summary`), Claims
  (`fact_claim`, `fraud_triage`).
- **EFR gold**: a general ledger (`gl_detail`), `trial_balance`, and `reinsurance_ceded`
  posted by a Finance Engine step.
- **CFO Reporting views**: `actuarial_reporting`, `risk_fraud_reporting`,
  `policy_reporting`, `claims_reporting`, `finance_reporting` (loss / expense / combined ratios).
- **Governance**: column masks on PII (`tax_id`, `email`, `phone`, `date_of_birth`) and KYC,
  `pii`/`kyc` classification tags, and access grants.

## Run it

Prereqs: a Unity Catalog-enabled Databricks workspace, the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) (≥ v0.240), and permission to create a catalog. Serverless workflows must be enabled (tasks run on serverless jobs compute).

```bash
# 1. Authenticate
databricks configure                      # or set DATABRICKS_HOST / DATABRICKS_TOKEN

# 2. Point the bundle at your workspace: edit workspace.host in databricks.yml

# 3. Deploy + run the whole pipeline
databricks bundle deploy -t dev
databricks bundle run cfo_data_platform_pipeline -t dev
```

Or run the notebooks in `src/` in order (`00` → `10`) in a notebook, passing the
`catalog` widget. Adjust data volume in `conf/config.yml` (`scale`).

Try the results:

```sql
SELECT * FROM cfo_poc.reporting.finance_reporting  ORDER BY combined_ratio DESC;
SELECT * FROM cfo_poc.reporting.actuarial_reporting ORDER BY loss_ratio DESC;
SELECT * FROM cfo_poc.claims.fraud_triage WHERE triage_priority = 'P1';
-- PII masking in action (masked unless you're in cfo_pii_readers):
SELECT party_id, legal_name, tax_id, email, date_of_birth FROM cfo_poc.hub.dim_party LIMIT 5;

-- ABC: Source -> Raw audit & balance trail (one row per entity per run)
SELECT entity, source_row_count, target_row_count, row_variance,
       control_column, control_total_variance, rows_balanced, status
FROM   cfo_poc.abc_control.ingestion_audit
ORDER  BY abc_run_id DESC, entity;

-- ABC: any control breaches raised
SELECT * FROM cfo_poc.abc_control.control_exception ORDER BY detected_at DESC;

-- Silver DQ: per-rule pass/fail for the latest run
SELECT entity, rule_id, rule_type, severity, rows_evaluated, rows_failed, fail_rate, passed
FROM   cfo_poc.abc_control.dq_result
WHERE  dq_run_id = (SELECT MAX(dq_run_id) FROM cfo_poc.abc_control.dq_result)
ORDER  BY passed, severity, entity;

-- AI DQ: semantic checks via Databricks AI Functions (advisory)
SELECT entity, check_name, check_type, model, rows_evaluated, rows_flagged, flag_rate
FROM   cfo_poc.abc_control.dq_ai_result
ORDER  BY dq_run_id DESC, flag_rate DESC;
-- inspect the rows the model flagged, e.g. implausible claims:
SELECT * FROM cfo_poc.oz_organized.claim_dq_ai_flagged LIMIT 20;
```

## Layout

```
saswata30/
├── databricks.yml                 # Asset Bundle: serverless job, 11 chained tasks
├── conf/config.yml                # catalog, schemas, data scale, governance groups
├── src/
│   ├── _common.py                 # shared config + helpers (%run-included)
│   ├── _abc.py                    # Audit, Balance & Control framework (%run-included by 02)
│   ├── _dq.py                     # Silver Data Quality engine — rules (%run-included by 03)
│   ├── _dq_ai.py                  # Silver Data Quality — AI Functions (%run-included by 03)
│   ├── 00_setup_unity_catalog.py  # catalog / schemas / volume (incl. abc_control)
│   ├── 01_generate_synthetic_sources.py
│   ├── 02_landing_zone_bronze.py  # Bronze ingest + ABC audit/balance/control gate
│   ├── 03_organized_zone_silver.py # Silver conform + DQ expectations/quarantine
│   ├── 04_hub_master_data.py
│   ├── 05_spoke_underwriting.py
│   ├── 06_spoke_policy.py
│   ├── 07_spoke_claims.py
│   ├── 08_efr_semantic_gold.py
│   ├── 09_reporting_marts.py
│   └── 10_governance_masking.py
└── docs/architecture.md           # deeper design: data model, lineage, mesh rationale
```

See [`docs/architecture.md`](docs/architecture.md) for the data model and design notes.
The business story — who this serves, the problem it solves, and the outcomes for the
office of the CFO — is on this page under [Use-case story](#use-case-story).

---
*Proof of concept — synthetic data only, not for production use.*
