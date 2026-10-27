"""Generate dashboards/efr_finance.lvdash.json — the EFR (Enterprise Finance Reporting) AI/BI dashboard.

Queries use schema-qualified names (`efr.gl_detail`, `reporting.finance_reporting`); the bundle sets
`dataset_catalog: ${var.catalog}` so the same file works in any catalog. Re-run after editing:

    python scripts/build_efr_dashboard.py
"""
import hashlib
import json
import pathlib

COLORS = ["#1B3139", "#FF3621", "#00A972", "#F2A900", "#2272B4", "#8BCAE7"]
OUT = pathlib.Path(__file__).resolve().parents[1] / "dashboards" / "efr_finance.lvdash.json"


def _id(*parts):
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:10]


RATIO = "SUM(losses_paid + reserves) / NULLIF(SUM(premium_revenue), 0)"
COMBINED = "SUM(losses_paid + reserves + expenses) / NULLIF(SUM(premium_revenue), 0)"
WINDOW = "to_date(period || '-01') >= add_months((SELECT to_date(max(posting_period) || '-01') FROM efr.trial_balance), -36)"

DATASETS = {
    "kpi": f"""
        SELECT SUM(premium_revenue) AS premium, SUM(losses_paid + reserves) AS claims_cost,
               SUM(expenses) AS expenses, SUM(underwriting_result) AS underwriting_result,
               {RATIO} AS loss_ratio, {COMBINED} AS combined_ratio
        FROM reporting.finance_reporting""",
    "trend": f"""
        SELECT to_date(period || '-01') AS period_month, view, loss_ratio, combined_ratio FROM (
          SELECT posting_period AS period, 'Booked (posting period)' AS view,
                 {RATIO} AS loss_ratio, {COMBINED} AS combined_ratio
          FROM reporting.finance_reporting GROUP BY 1, 2
          UNION ALL
          SELECT origin_period AS period, 'Restated (origin period)' AS view,
                 {RATIO} AS loss_ratio, {COMBINED} AS combined_ratio
          FROM reporting.finance_reporting_by_origin GROUP BY 1, 2)
        WHERE {WINDOW}""",
    "lob": f"""
        SELECT line_of_business, SUM(premium_revenue) AS premium,
               SUM(losses_paid + reserves) AS claims_cost, SUM(underwriting_result) AS underwriting_result,
               {COMBINED} AS combined_ratio
        FROM reporting.finance_reporting GROUP BY 1""",
    "tb": f"""
        SELECT to_date(posting_period || '-01') AS period_month, gl_category, SUM(balance) AS balance
        FROM efr.trial_balance
        WHERE to_date(posting_period || '-01') >= add_months((SELECT to_date(max(posting_period) || '-01')
                                                             FROM efr.trial_balance), -24)
        GROUP BY 1, 2""",
    "accounts": """
        SELECT account_code, account_name, gl_category, SUM(balance) AS balance,
               SUM(posting_count) AS postings
        FROM efr.trial_balance GROUP BY ALL""",
    "reins": """
        SELECT line_of_business, treaty_type, treaties, ceded_premium, avg_ceded_share
        FROM efr.reinsurance_ceded""",
    "late_kpi": """
        SELECT COUNT(DISTINCT batch_id) AS late_batches, COUNT(DISTINCT claim_id) AS late_claims,
               coalesce(SUM(CASE WHEN entry_type = 'PRIOR_PERIOD_ADJ' THEN adjustment END), 0) AS prior_period_adj,
               coalesce(SUM(CASE WHEN entry_type = 'LATE_ADJ' THEN adjustment END), 0) AS open_period_adj,
               (SELECT COUNT(*) FROM efr.period_control WHERE status = 'CLOSED') AS closed_periods,
               (SELECT max(posting_period) FROM efr.period_control WHERE status = 'CLOSED') AS closed_through
        FROM efr.late_arrival_log""",
    "late_origin": """
        SELECT to_date(origin_period || '-01') AS origin_month, entry_type, SUM(adjustment) AS adjustment
        FROM efr.late_arrival_log GROUP BY ALL""",
    "late_type": """
        SELECT late_type, gl_category, SUM(adjustment) AS adjustment, COUNT(DISTINCT claim_id) AS claims
        FROM efr.late_arrival_log GROUP BY ALL""",
    "late_detail": """
        SELECT batch_id, booking_date, late_type, entry_type, gl_category, line_of_business,
               origin_period, posting_period, claims, adjustment, avg_report_lag_days
        FROM reporting.efr_late_arrival_summary""",
    "restatement": f"""
        WITH b AS (SELECT posting_period AS period, {RATIO} AS booked FROM reporting.finance_reporting GROUP BY 1),
             o AS (SELECT origin_period AS period, {RATIO} AS restated FROM reporting.finance_reporting_by_origin GROUP BY 1)
        SELECT o.period, b.booked AS booked_loss_ratio, o.restated AS restated_loss_ratio,
               (o.restated - b.booked) * 100 AS restatement_pts
        FROM o JOIN b USING (period)
        WHERE abs(o.restated - b.booked) > 0.00005
        ORDER BY abs(o.restated - b.booked) DESC LIMIT 50""",
    "periods": """
        SELECT posting_period, status, closed_at, closed_by FROM efr.period_control""",
}


def dataset(name):
    return {"name": name, "displayName": name.replace("_", " ").title(),
            "queryLines": [" ".join(DATASETS[name].split())]}


def fmt(kind, places=1):
    if kind == "pct":
        return {"type": "number-percent", "decimalPlaces": {"type": "exact", "places": places}}
    if kind == "usd":
        return {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact",
                "decimalPlaces": {"type": "max", "places": places}}
    return {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 0}}


def text(lines, x, y, w, h):
    return {"widget": {"name": _id("t", lines[0]), "multilineTextboxSpec": {"lines": lines}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def counter(ds, field, title, kind, x, y, w=1, h=3):
    return {"widget": {"name": _id("c", ds, field),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": True,
                                    "fields": [{"name": field, "expression": f"`{field}`"}]}}],
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": field, "displayName": title,
                                                        **({"format": fmt(kind)} if kind else {})}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def chart(kind, ds, xf, yf, title, pos, color=None, xtype="temporal", yfmt=None, agg="SUM", horizontal=False):
    fields = [{"name": xf, "expression": f"`{xf}`"}, {"name": yf, "expression": f"{agg}(`{yf}`)"}]
    if color:
        fields.append({"name": color, "expression": f"`{color}`"})
    xenc = {"fieldName": xf, "scale": {"type": xtype}, "displayName": xf.replace("_", " ")}
    yenc = {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": yf.replace("_", " ")}
    if yfmt:
        yenc["format"] = yfmt
    enc = {"x": yenc, "y": xenc} if horizontal else {"x": xenc, "y": yenc}
    if horizontal:
        enc["y"]["scale"]["sort"] = {"by": "x-reversed"}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}
    return {"widget": {"name": _id(kind, ds, xf, yf, title),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": False,
                                                                    "fields": fields}}],
                       "spec": {"version": 3, "widgetType": kind, "encodings": enc,
                                "frame": {"showTitle": True, "title": title}, "mark": {"colors": COLORS}}},
            "position": dict(zip(("x", "y", "width", "height"), pos))}


def table(ds, cols, title, pos):
    return {"widget": {"name": _id("tbl", ds, title),
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "disaggregated": True,
                                    "fields": [{"name": c, "expression": f"`{c}`"} for c in cols]}}],
                       "spec": {"version": 2, "widgetType": "table",
                                "encodings": {"columns": [{"fieldName": c, "displayName": c.replace("_", " ")}
                                                          for c in cols]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": dict(zip(("x", "y", "width", "height"), pos))}


overview = [
    text(["## EFR — Enterprise Finance Reporting\n",
          "General ledger, trial balance and P&L ratios from the CFO Data Hub semantic gold (`efr.*`). "
          "**Booked** = by posting period (closed periods frozen). **Restated** = by origin (accident) period, "
          "including late-arriving claims.\n"], 0, 0, 6, 2),
    counter("kpi", "premium", "Premium revenue", "usd", 0, 2),
    counter("kpi", "claims_cost", "Claims cost (paid + reserves)", "usd", 1, 2),
    counter("kpi", "expenses", "Expenses", "usd", 2, 2),
    counter("kpi", "underwriting_result", "Underwriting result", "usd", 3, 2),
    counter("kpi", "loss_ratio", "Loss ratio", "pct", 4, 2),
    counter("kpi", "combined_ratio", "Combined ratio", "pct", 5, 2),
    chart("line", "trend", "period_month", "combined_ratio", "Combined ratio: booked vs restated (last 36 months)",
          (0, 5, 4, 6), color="view", yfmt=fmt("pct", 0)),
    chart("bar", "lob", "line_of_business", "underwriting_result", "Underwriting result by line of business",
          (4, 5, 2, 6), xtype="categorical", yfmt=fmt("usd"), horizontal=True),
    chart("bar", "tb", "period_month", "balance", "Trial balance by GL category (last 24 months)",
          (0, 11, 4, 6), color="gl_category", yfmt=fmt("usd")),
    chart("bar", "lob", "line_of_business", "combined_ratio", "Combined ratio by line of business",
          (4, 11, 2, 6), xtype="categorical", yfmt=fmt("pct", 0), horizontal=True, agg="MAX"),
    table("accounts", ["account_code", "account_name", "gl_category", "balance", "postings"],
          "Chart of accounts: balances", (0, 17, 3, 5)),
    table("reins", ["line_of_business", "treaty_type", "treaties", "ceded_premium", "avg_ceded_share"],
          "Reinsurance ceded", (3, 17, 3, 5)),
]

late = [
    text(["## Late-arriving claims & period close\n",
          "Late claim facts never re-post a **closed** period: the difference between what a claim now requires and "
          "what was booked becomes a **PRIOR_PERIOD_ADJ** in the open booking period, or a **LATE_ADJ** when its "
          "origin period is still open. Every line keeps its `origin_period`, which drives the restated view.\n"],
         0, 0, 6, 2),
    counter("late_kpi", "late_batches", "Late batches", None, 0, 2),
    counter("late_kpi", "late_claims", "Late claims", None, 1, 2),
    counter("late_kpi", "prior_period_adj", "Prior-period adjustments", "usd", 2, 2),
    counter("late_kpi", "open_period_adj", "Open-period adjustments", "usd", 3, 2),
    counter("late_kpi", "closed_periods", "Closed periods", None, 4, 2),
    counter("late_kpi", "closed_through", "Closed through", None, 5, 2),
    chart("bar", "late_origin", "origin_month", "adjustment",
          "Late adjustments by origin (accident) period", (0, 5, 4, 6), color="entry_type", yfmt=fmt("usd")),
    chart("bar", "late_type", "late_type", "adjustment", "By type of late fact", (4, 5, 2, 6),
          color="gl_category", xtype="categorical", yfmt=fmt("usd")),
    table("restatement", ["period", "booked_loss_ratio", "restated_loss_ratio", "restatement_pts"],
          "Largest restatements: booked vs restated loss ratio", (0, 11, 3, 6)),
    table("periods", ["posting_period", "status", "closed_at", "closed_by"], "Period control", (3, 11, 3, 6)),
    table("late_detail", ["batch_id", "booking_date", "late_type", "entry_type", "gl_category", "line_of_business",
                          "origin_period", "posting_period", "claims", "adjustment", "avg_report_lag_days"],
          "Late-arrival journal summary", (0, 17, 6, 6)),
]

dash = {
    "datasets": [dataset(n) for n in DATASETS],
    "pages": [{"name": "efr_overview", "displayName": "EFR overview", "layout": overview},
              {"name": "late_arriving", "displayName": "Late-arriving claims", "layout": late}],
}
OUT.parent.mkdir(exist_ok=True)
OUT.write_text(json.dumps(dash, indent=2) + "\n")
print(f"wrote {OUT}")
