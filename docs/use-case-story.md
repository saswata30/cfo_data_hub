# Use-Case Story — The CFO Data Hub

> A narrative walkthrough of *why* this platform exists, *who* it serves, and *what*
> changes for the finance organisation of a global commercial insurer once it is live.
> The code in this repo is the runnable proof of that story on Databricks.

---

## 1. The organisation

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

## 2. The problem

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

## 3. The solution — the CFO Data Hub

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

## 4. How the data flows (the journey of one policy)

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

## 5. Business outcomes

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

## 6. Why Databricks

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

---

*This story is realised end-to-end by the notebooks in [`src/`](../src) and deployed by
[`databricks.yml`](../databricks.yml). See [`architecture.md`](architecture.md) for the
technical design. Proof of concept — synthetic data only, not for production use.*
