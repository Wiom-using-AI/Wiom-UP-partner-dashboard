"""
PAYG Master Dashboard — Flask Server
-------------------------------------
Serves a live dashboard at http://localhost:8080
Refresh button re-queries Snowflake via Metabase API.
Auto-refreshes every hour in the background.

Usage:
    python payg_dashboard.py          # start server + open browser
    python payg_dashboard.py --no-open  # start server only
"""

import os
import sys
import json
import threading
import webbrowser
import requests
from datetime import datetime
from flask import Flask, jsonify
from dotenv import load_dotenv

load_dotenv()  # Railway sets env vars directly; local dev: use .env in project root

METABASE_URL = os.getenv("METABASE_URL", "").rstrip("/")
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
DATABASE_ID = 113  # Snowflake
PORT = int(os.getenv("PORT", 8080))

if not METABASE_URL:
    print("ERROR: METABASE_URL not found in C:\\credentials\\.env")
    sys.exit(1)
if not METABASE_API_KEY:
    print("ERROR: METABASE_API_KEY not found in C:\\credentials\\.env")
    sys.exit(1)

HEADERS = {"x-api-key": METABASE_API_KEY, "Content-Type": "application/json"}

# ── Global state ──────────────────────────────────────────────
app = Flask(__name__)
_cache = {"results": [], "generated_at": None, "refreshing": False}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def run_sql(sql: str) -> tuple[list[str], list[list]]:
    """Run SQL via Metabase /api/dataset. Returns (cols, rows)."""
    payload = {
        "database": DATABASE_ID,
        "type": "native",
        "native": {"query": sql},
    }
    resp = requests.post(f"{METABASE_URL}/api/dataset", headers=HEADERS, json=payload)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Metabase query error: {data['error']}")
    cols = [c["display_name"] for c in data["data"]["cols"]]
    rows = data["data"]["rows"]
    return cols, rows


# ─────────────────────────────────────────────
# BASE CTE (shared by all sections)
# ─────────────────────────────────────────────

BASE_CTES = """
with

migration_base as (
    select
        partner_id,
        min(created_time) + interval '330 minute'           as migration_ts,
        case
            when to_date(min(created_time) + interval '330 minute') < '2026-03-15'::date
                then to_date(min(created_time) + interval '330 minute') + interval '1 day'
            else to_date(min(created_time) + interval '330 minute')
        end as migration_dt
    from prod_db.master_db_read_dbo.payg_migration
    group by partner_id
),

payg_installs as (
    select idmaker(trum.shard, 0, trum.router_nas_id) as nas_id
    from t_router_user_mapping trum
    join t_plan_configuration tpc on tpc.id = trum.selected_plan_id
    where trum.otp = 'DONE'
      and trum.store_group_id = 0
      and trum.device_limit > 1
      and trum.mobile > '5999999999'
      and tpc.time_limit / 86400 = 2
      and to_date(dateadd('minute', 330, trum.otp_issued_time)) >= '2026-01-26'
    qualify row_number() over (
        partition by idmaker(trum.shard, 0, trum.router_nas_id)
        order by trum.otp_issued_time
    ) = 1
),

qualified_customers as (
    select
        a.nas_id,
        a.partner_account_id,
        a.mobile,
        a.speed_limit_mbps,
        a.plan_expiry_time,
        mb.migration_ts,
        mb.migration_dt,
        to_date(a.plan_expiry_time) as due_date,
        case
            when a.plan_expiry_time >= mb.migration_ts       then 'ACTIVE_ON_MIGRATION'
            when datediff('day', to_date(a.plan_expiry_time), to_date(mb.migration_ts)) = 0
                                                              then 'R0_ON_MIGRATION'
            else 'R1_R30_ON_MIGRATION'
        end as migration_cohort
    from prod_db.dbt.payg_migration_audit a
    join migration_base mb on a.partner_account_id = mb.partner_id
    where a.speed_limit_mbps in (50, 100)
      and a.has_mandate = 'No'
      and coalesce(a.is_picked_up, 0) = 0
      and a.mobile > '5999999999'
      and (
            a.plan_expiry_time >= mb.migration_ts
            or datediff('day', to_date(a.plan_expiry_time), to_date(mb.migration_ts)) between 0 and 30
          )
      and a.nas_id not in (select nas_id from payg_installs)
    qualify row_number() over (
        partition by a.nas_id
        order by
            case when to_date(a.record_ingest_date) <= mb.migration_dt then 0 else 1 end,
            a.record_ingest_date desc,
            a.plan_expiry_time desc
    ) = 1
),

recharge_events as (
    select
        idmaker(trum.shard, 0, trum.router_nas_id)                          as nas_id,
        to_date(dateadd('minute', 330, trum.otp_issued_time))               as plan_start_ist,
        tpc.combined_setting_id,
        tpc.plan_code,
        tpc.price                                                            as plan_price,
        tpc.time_limit / 86400.0                                            as plan_days,
        try_to_number(try_parse_json(trum.extra_data):totalPaid::string)    as actual_paid
    from t_router_user_mapping trum
    join t_plan_configuration tpc on tpc.id = trum.selected_plan_id
    where trum.otp = 'DONE'
      and trum.store_group_id = 0
      and trum.device_limit > 1
      and trum.mobile > '5999999999'
      and to_date(dateadd('minute', 330, trum.otp_issued_time)) >= '2026-02-11'
      and trum.mobile not in ('6900099267','7679376747')
),

due_cohorts as (
    select q.nas_id, q.partner_account_id, q.speed_limit_mbps,
           q.migration_ts, q.migration_cohort, q.due_date
    from qualified_customers q
    where q.due_date <= current_date
),

payg_recharges as (
    select
        dc.nas_id, dc.due_date, dc.speed_limit_mbps, dc.migration_cohort,
        re.plan_start_ist as payg_recharge_date,
        re.plan_code, re.plan_price, re.plan_days, re.actual_paid,
        datediff('day', dc.due_date, re.plan_start_ist) as r_day
    from due_cohorts dc
    join recharge_events re
        on  re.nas_id = dc.nas_id
        and re.combined_setting_id = 22
        and re.plan_start_ist >= dc.due_date
    qualify row_number() over (partition by dc.nas_id order by re.plan_start_ist) = 1
),

any_recharges as (
    select dc.nas_id, dc.due_date, re.plan_start_ist, re.combined_setting_id,
           re.plan_price, re.actual_paid
    from due_cohorts dc
    join recharge_events re
        on  re.nas_id = dc.nas_id
        and re.plan_start_ist >= dc.due_date
    qualify row_number() over (partition by dc.nas_id order by re.plan_start_ist) = 1
),

education_completed as (
    select try_to_number(nullif(ev.nasid_long, '')) as nas_id
    from prod_db.public.ct_customer_payg_migration_events_mv ev
    where ev.event_name = 'migration_50mbps_education_complete'
      and try_to_number(nullif(ev.nasid_long, '')) is not null
    group by 1
)
"""

# ─────────────────────────────────────────────
# SECTION QUERIES
# ─────────────────────────────────────────────

SECTIONS = [
    {
        "id": "A",
        "title": "Top-line Summary",
        "subtitle": "Overall eligible base, recharge and migration funnel",
        "definitions": {
            "speed_limit_mbps":         "Pre-migration speed tier of the customer (50 or 100 Mbps)",
            "total_eligible":           "All customers in the migration base on their partner's migration date",
            "total_due":                "Customers whose plan has expired — recharge window is open",
            "total_recharged":          "Customers who made any recharge after their due date",
            "total_migrated":           "Customers who bought a PAYG plan (combined_setting_id = 22) after due date — core KPI",
            "recharged_not_migrated":   "Retained but chose old plan — revenue leak risk",
            "zero_recharge_due":        "Due but no recharge at all — churn risk",
            "migration_rate_pct":       "migrated / due × 100 — primary conversion metric",
            "recharge_rate_pct":        "recharged (any plan) / due × 100 — overall retention signal",
        },
        "sql": BASE_CTES + """
select
    q.speed_limit_mbps,
    count(distinct q.nas_id)                                                as total_eligible,
    count(distinct dc.nas_id)                                               as total_due,
    count(distinct ar.nas_id)                                               as total_recharged,
    count(distinct pr.nas_id)                                               as total_migrated,
    count(distinct case when ar.nas_id is not null and pr.nas_id is null then ar.nas_id end) as recharged_not_migrated,
    count(distinct case when dc.nas_id is not null and ar.nas_id is null then dc.nas_id end) as zero_recharge_due,
    round(100.0 * count(distinct pr.nas_id) / nullif(count(distinct dc.nas_id),0), 2)       as migration_rate_pct,
    round(100.0 * count(distinct ar.nas_id) / nullif(count(distinct dc.nas_id),0), 2)       as recharge_rate_pct
from qualified_customers q
left join due_cohorts   dc on q.nas_id = dc.nas_id
left join any_recharges ar on q.nas_id = ar.nas_id
left join payg_recharges pr on q.nas_id = pr.nas_id
group by 1
order by 1
""",
    },
    {
        "id": "B",
        "title": "Cohort Breakdown",
        "subtitle": "Active / R0 / R1-R30 — migration rate by cohort and speed",
        "definitions": {
            "migration_cohort":     "ACTIVE_ON_MIGRATION = plan was live on migration date | R0_ON_MIGRATION = expired same day | R1_R30_ON_MIGRATION = expired 1-30 days before migration",
            "speed_limit_mbps":     "Pre-migration speed tier",
            "total_eligible":       "Customers in this cohort",
            "total_due":            "Whose plan has now expired",
            "total_recharged":      "Made any recharge after due date",
            "total_migrated":       "Bought a PAYG plan after due date",
            "migration_rate_pct":   "migrated / due × 100",
            "recharge_rate_pct":    "recharged / due × 100",
        },
        "sql": BASE_CTES + """
select
    q.migration_cohort,
    q.speed_limit_mbps,
    count(distinct q.nas_id)                                                as total_eligible,
    count(distinct dc.nas_id)                                               as total_due,
    count(distinct ar.nas_id)                                               as total_recharged,
    count(distinct pr.nas_id)                                               as total_migrated,
    round(100.0 * count(distinct pr.nas_id) / nullif(count(distinct dc.nas_id),0), 2) as migration_rate_pct,
    round(100.0 * count(distinct ar.nas_id) / nullif(count(distinct dc.nas_id),0), 2) as recharge_rate_pct
from qualified_customers q
left join due_cohorts   dc on q.nas_id = dc.nas_id
left join any_recharges ar on q.nas_id = ar.nas_id
left join payg_recharges pr on q.nas_id = pr.nas_id
group by 1, 2
order by
    case q.migration_cohort
        when 'ACTIVE_ON_MIGRATION' then 1
        when 'R0_ON_MIGRATION'     then 2
        when 'R1_R30_ON_MIGRATION' then 3
    end, q.speed_limit_mbps
""",
    },
    {
        "id": "C",
        "title": "R-Day Windows",
        "subtitle": "Kitne din mein recharge hua — cumulative recharge & migration by day",
        "definitions": {
            "speed_limit_mbps":         "Pre-migration speed tier",
            "migration_cohort":         "Which cohort the customer belongs to",
            "due_customers":            "Total customers with an open recharge window",
            "recharged_r0":             "Recharged on the exact due date",
            "recharged_r1":             "Cumulative recharges by due_date + 1",
            "recharged_r3":             "Cumulative recharges by due_date + 3",
            "recharged_r5":             "Cumulative recharges by due_date + 5",
            "recharged_total":          "Total recharges (no day cap)",
            "migrated_r0":              "Bought PAYG plan on exact due date",
            "migrated_r1":              "Cumulative PAYG migrations by due_date + 1",
            "migrated_r3":              "Cumulative PAYG migrations by due_date + 3",
            "migrated_r5":              "Cumulative PAYG migrations by due_date + 5",
            "migrated_total":           "Total PAYG migrations (no day cap)",
            "migrated_rate_r0_pct":     "% migrated on exact due date",
            "migrated_rate_r3_pct":     "% migrated within 3 days of due date",
            "migrated_rate_total_pct":  "Overall migration rate",
        },
        "sql": BASE_CTES + """
select
    dc.speed_limit_mbps,
    dc.migration_cohort,
    count(distinct dc.nas_id) as due_customers,
    count(distinct case when ar.plan_start_ist = dc.due_date                      then ar.nas_id end) as recharged_r0,
    count(distinct case when datediff('day',dc.due_date,ar.plan_start_ist) <= 1   then ar.nas_id end) as recharged_r1,
    count(distinct case when datediff('day',dc.due_date,ar.plan_start_ist) <= 3   then ar.nas_id end) as recharged_r3,
    count(distinct case when datediff('day',dc.due_date,ar.plan_start_ist) <= 5   then ar.nas_id end) as recharged_r5,
    count(distinct ar.nas_id) as recharged_total,
    count(distinct case when pr.r_day = 0  then pr.nas_id end) as migrated_r0,
    count(distinct case when pr.r_day <= 1 then pr.nas_id end) as migrated_r1,
    count(distinct case when pr.r_day <= 3 then pr.nas_id end) as migrated_r3,
    count(distinct case when pr.r_day <= 5 then pr.nas_id end) as migrated_r5,
    count(distinct pr.nas_id) as migrated_total,
    round(100.0 * count(distinct case when pr.r_day = 0  then pr.nas_id end) / nullif(count(distinct dc.nas_id),0),2) as migrated_rate_r0_pct,
    round(100.0 * count(distinct case when pr.r_day <= 3 then pr.nas_id end) / nullif(count(distinct dc.nas_id),0),2) as migrated_rate_r3_pct,
    round(100.0 * count(distinct pr.nas_id) / nullif(count(distinct dc.nas_id),0),2)                                  as migrated_rate_total_pct
from due_cohorts dc
left join payg_recharges pr on dc.nas_id = pr.nas_id
left join any_recharges  ar on dc.nas_id = ar.nas_id
group by 1, 2
order by 1, 2
""",
    },
    {
        "id": "D",
        "title": "Plan Distribution",
        "subtitle": "Kaun sa plan chuna — which PAYG plan customers are picking",
        "definitions": {
            "pre_migration_speed_mbps": "Customer's speed tier before migration (50 or 100 Mbps)",
            "plan_code":                "PAYG plan identifier (e.g. 565_22_28DAY)",
            "plan_price":               "Listed price of the plan in Rs",
            "plan_days":                "Duration of the plan in days (1 / 2 / 7 / 14 / 28)",
            "city":                     "Partner's city (Delhi / Mumbai / Bharat)",
            "migrated_customers":       "Customers who chose this plan",
            "pct_of_speed_city":        "Share within same speed tier + city bucket",
            "total_actual_revenue":     "Sum of actual amounts paid (after discounts)",
            "avg_actual_paid":          "Average actual amount paid per customer",
            "avg_discount":             "Average discount applied (plan_price − actual_paid)",
        },
        "sql": BASE_CTES + """
select
    q.speed_limit_mbps                              as pre_migration_speed_mbps,
    pr.plan_code,
    pr.plan_price,
    cast(pr.plan_days as int)                       as plan_days,
    sm.city,
    count(distinct pr.nas_id)                       as migrated_customers,
    round(100.0 * count(distinct pr.nas_id)
          / nullif(sum(count(distinct pr.nas_id)) over (partition by q.speed_limit_mbps, sm.city),0),2) as pct_of_speed_city,
    sum(pr.actual_paid)                             as total_actual_revenue,
    round(avg(pr.actual_paid),2)                    as avg_actual_paid,
    round(avg(pr.plan_price - pr.actual_paid),2)    as avg_discount
from payg_recharges pr
join qualified_customers q on q.nas_id = pr.nas_id
join prod_db.public.supply_model sm on q.partner_account_id = sm.partner_account_id
group by 1,2,3,4,5
order by 5,1,plan_days
""",
    },
    {
        "id": "E",
        "title": "NAS Setting Hygiene (100 Mbps)",
        "subtitle": "Tech check — all eligible 100 Mbps customers should have NAS combined_setting_id = 22",
        "definitions": {
            "partner_account_id":   "Partner identifier",
            "partner_name":         "Partner name",
            "zone":                 "Geographic zone (Agra++, Bareilly, etc.)",
            "city":                 "City",
            "eligible_100mbps":     "Eligible 100 Mbps customers for this partner",
            "nas_set_to_22":        "Customers whose router was correctly configured to PAYG setting",
            "nas_not_set_bug":      "BUG COUNT — router not updated to setting 22 on migration date. Escalate to engineering.",
            "nas_correct_pct":      "% of eligible customers correctly configured. Should be 100%.",
        },
        "sql": BASE_CTES + """
select
    sm.partner_account_id,
    sm.partner_name,
    sm.zone,
    sm.city,
    count(distinct q.nas_id)                                                                     as eligible_100mbps,
    count(distinct case when csm.combined_setting_id = 22 then q.nas_id end)                     as nas_set_to_22,
    count(distinct case when coalesce(csm.combined_setting_id,-1) != 22 then q.nas_id end)        as nas_not_set_bug,
    round(100.0 * count(distinct case when csm.combined_setting_id = 22 then q.nas_id end)
          / nullif(count(distinct q.nas_id),0),2)                                                as nas_correct_pct
from qualified_customers q
left join prod_db.master_db_dbo.t_combined_setting_nas_mapping csm
    on  csm.nas_id = q.nas_id
    and csm._fivetran_active = true
join prod_db.public.supply_model sm on q.partner_account_id = sm.partner_account_id
where q.speed_limit_mbps = 100
group by 1,2,3,4
having nas_not_set_bug > 0
order by nas_not_set_bug desc
""",
    },
    {
        "id": "F",
        "title": "Education Tracking (50 Mbps)",
        "subtitle": "Education completion is a prerequisite for 50 Mbps customers to see PAYG plan",
        "definitions": {
            "city":                         "Partner city",
            "zone":                         "Geographic zone",
            "eligible_50mbps_due":          "50 Mbps customers whose plan has expired",
            "education_completed":          "Completed the PAYG education flow in-app (requires latest app version)",
            "not_educated":                 "Have NOT completed education — cannot see PAYG plan yet",
            "educated_and_migrated":        "Completed education AND bought PAYG plan",
            "migrated_without_education":   "Bought PAYG plan without completing education (edge case)",
            "total_migrated":               "Total 50 Mbps customers who migrated",
            "education_completion_pct":     "% who completed education. Low % = likely old app version blocking flow.",
            "migration_rate_pct":           "% who bought PAYG plan",
        },
        "sql": BASE_CTES + """
select
    sm.city,
    sm.zone,
    count(distinct dc.nas_id)                                                                   as eligible_50mbps_due,
    count(distinct ec.nas_id)                                                                   as education_completed,
    count(distinct case when ec.nas_id is null then dc.nas_id end)                              as not_educated,
    count(distinct case when ec.nas_id is not null and pr.nas_id is not null then dc.nas_id end) as educated_and_migrated,
    count(distinct case when ec.nas_id is null  and pr.nas_id is not null then dc.nas_id end)   as migrated_without_education,
    count(distinct pr.nas_id)                                                                   as total_migrated,
    round(100.0 * count(distinct ec.nas_id) / nullif(count(distinct dc.nas_id),0),2)            as education_completion_pct,
    round(100.0 * count(distinct pr.nas_id) / nullif(count(distinct dc.nas_id),0),2)            as migration_rate_pct
from due_cohorts dc
left join education_completed ec on dc.nas_id = ec.nas_id
left join payg_recharges pr
    on  pr.nas_id = dc.nas_id
    and to_date(pr.payg_recharge_date) < current_date
join prod_db.public.supply_model sm on dc.partner_account_id = sm.partner_account_id
where dc.speed_limit_mbps = 50
group by 1,2
order by education_completion_pct asc
""",
    },
    {
        "id": "G",
        "title": "Discount Analysis",
        "subtitle": "True revenue vs listed plan price — flag high-discount partners",
        "definitions": {
            "partner_name":             "Partner name",
            "city":                     "City",
            "zone":                     "Zone",
            "plan_code":                "PAYG plan identifier",
            "plan_days":                "Plan duration",
            "plan_price":               "Listed plan price in Rs",
            "migrated_customers":       "Customers who chose this plan at this partner",
            "total_actual_revenue":     "Actual money collected (after discount)",
            "total_gross_revenue":      "Revenue at full listed price",
            "total_discount_given":     "Total discount amount = gross − actual",
            "avg_discount_per_customer":"Average Rs discount per customer",
            "discount_pct":             "Discount as % of gross revenue",
            "high_discount_customers":  "Customers who got > 10% discount on plan price",
        },
        "sql": BASE_CTES + """
select
    sm.partner_name,
    sm.city,
    sm.zone,
    pr.plan_code,
    cast(pr.plan_days as int) as plan_days,
    pr.plan_price,
    count(distinct pr.nas_id)                                               as migrated_customers,
    sum(pr.actual_paid)                                                     as total_actual_revenue,
    sum(pr.plan_price)                                                      as total_gross_revenue,
    sum(pr.plan_price - pr.actual_paid)                                     as total_discount_given,
    round(avg(pr.plan_price - pr.actual_paid),2)                            as avg_discount_per_customer,
    round(100.0 * sum(pr.plan_price - pr.actual_paid) / nullif(sum(pr.plan_price),0),2) as discount_pct,
    count(distinct case when (pr.plan_price - pr.actual_paid) > 0.1 * pr.plan_price then pr.nas_id end) as high_discount_customers
from payg_recharges pr
join qualified_customers q on q.nas_id = pr.nas_id
join prod_db.public.supply_model sm on q.partner_account_id = sm.partner_account_id
where pr.actual_paid is not null
group by 1,2,3,4,5,6
having total_discount_given > 0
order by discount_pct desc
""",
    },
    {
        "id": "H",
        "title": "Partner Drill-Down",
        "subtitle": "Worst performers pehle — all KPIs at partner × speed tier",
        "definitions": {
            "partner_name":             "Partner name",
            "zone":                     "Geographic zone",
            "city":                     "City",
            "account_manager":          "PSH/AM responsible for the partner",
            "speed_limit_mbps":         "Speed tier",
            "migration_date":           "Date the partner was migrated",
            "total_eligible":           "Total customers in migration base",
            "total_due":                "Customers with open recharge window",
            "total_recharged":          "Made any recharge after due date",
            "total_migrated":           "Bought PAYG plan",
            "recharged_not_migrated":   "Retained but on old plan",
            "zero_recharge_due":        "Due with zero recharge — churned",
            "migration_rate_pct":       "% migrated out of due. Sorted asc = worst first.",
            "recharge_rate_pct":        "% recharged (any plan) out of due",
            "nas_bug_count":            "100 Mbps only: NAS not set to 22 — tech bug count",
        },
        "sql": BASE_CTES + """
select
    sm.partner_name,
    sm.zone,
    sm.city,
    sm.account_manager,
    q.speed_limit_mbps,
    to_date(mb.migration_ts) as migration_date,
    count(distinct q.nas_id)  as total_eligible,
    count(distinct dc.nas_id) as total_due,
    count(distinct ar.nas_id) as total_recharged,
    count(distinct pr.nas_id) as total_migrated,
    count(distinct case when ar.nas_id is not null and pr.nas_id is null then ar.nas_id end) as recharged_not_migrated,
    count(distinct case when dc.nas_id is not null and ar.nas_id is null then dc.nas_id end) as zero_recharge_due,
    round(100.0 * count(distinct pr.nas_id) / nullif(count(distinct dc.nas_id),0),2) as migration_rate_pct,
    round(100.0 * count(distinct ar.nas_id) / nullif(count(distinct dc.nas_id),0),2) as recharge_rate_pct,
    case when q.speed_limit_mbps = 100 then
        count(distinct case when coalesce(csm.combined_setting_id,-1) != 22 then q.nas_id end)
    end as nas_bug_count
from qualified_customers q
join migration_base mb on q.partner_account_id = mb.partner_id
join prod_db.public.supply_model sm on q.partner_account_id = sm.partner_account_id
left join due_cohorts   dc on q.nas_id = dc.nas_id
left join any_recharges ar on q.nas_id = ar.nas_id
left join payg_recharges pr on q.nas_id = pr.nas_id
left join prod_db.master_db_dbo.t_combined_setting_nas_mapping csm
    on  csm.nas_id = q.nas_id
    and csm._fivetran_active = true
group by 1,2,3,4,5,6
order by migration_rate_pct asc nulls first
""",
    },
]


# ─────────────────────────────────────────────
# HTML GENERATION
# ─────────────────────────────────────────────

def pct_color(val):
    """Return a CSS color class based on a percentage value."""
    try:
        v = float(val)
        if v >= 70:  return "good"
        if v >= 40:  return "warn"
        return "bad"
    except Exception:
        return ""


def bug_color(val):
    try:
        return "bad" if float(val) > 0 else "good"
    except Exception:
        return ""


def html_table(cols: list[str], rows: list[list], defs: dict) -> str:
    pct_cols = {c for c in cols if c.endswith("_pct")}
    bug_cols  = {c for c in cols if "bug" in c.lower()}

    header = "".join(f"<th>{c}</th>" for c in cols)
    body_rows = []
    for row in rows:
        cells = []
        for c, v in zip(cols, row):
            cls = ""
            if c in pct_cols:    cls = pct_color(v)
            elif c in bug_cols:  cls = bug_color(v)
            cells.append(f'<td class="{cls}">{v if v is not None else ""}</td>')
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    def_rows = "".join(
        f"<tr><td class='def-key'>{c}</td><td>{defs[c]}</td></tr>"
        for c in cols if c in defs
    )

    return f"""
<div class="table-wrap">
  <table>
    <thead><tr>{header}</tr></thead>
    <tbody>{"".join(body_rows)}</tbody>
  </table>
</div>
<details class="defs">
  <summary>Metric Definitions</summary>
  <table class="def-table"><tbody>{def_rows}</tbody></table>
</details>
"""


def fetch_all_sections() -> list[dict]:
    """Run all 8 section queries and return results list."""
    results = []
    for s in SECTIONS:
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] Section {s['id']}: {s['title']} ...", end=" ", flush=True)
        try:
            cols, rows = run_sql(s["sql"])
            print(f"{len(rows)} rows")
            results.append({
                "id": s["id"], "title": s["title"], "subtitle": s["subtitle"],
                "defs": s["definitions"], "cols": cols, "rows": rows, "status": "ok",
            })
        except Exception as e:
            print(f"ERROR — {e}")
            results.append({
                "id": s["id"], "title": s["title"], "subtitle": s["subtitle"],
                "defs": s["definitions"], "status": "error", "error": str(e),
            })
    return results


def refresh_cache():
    """Fetch fresh data and update the global cache."""
    if _cache["refreshing"]:
        return
    _cache["refreshing"] = True
    print(f"\nRefreshing data...")
    try:
        results = fetch_all_sections()
        _cache["results"] = results
        _cache["generated_at"] = datetime.now().strftime("%d %b %Y, %I:%M %p")
        print(f"Refresh complete — {_cache['generated_at']}\n")
    finally:
        _cache["refreshing"] = False


def auto_refresh_loop():
    """Background thread: refresh every hour."""
    import time
    while True:
        time.sleep(3600)
        refresh_cache()


def build_html(results: list[dict]) -> str:
    generated_at = _cache.get("generated_at") or datetime.now().strftime("%d %b %Y, %I:%M %p")
    sections_html = ""
    for r in results:
        status = r.get("status", "ok")
        if status == "ok":
            content = html_table(r["cols"], r["rows"], r["defs"])
            row_count = f'<span class="row-count">{len(r["rows"])} row{"s" if len(r["rows"]) != 1 else ""}</span>'
        else:
            content = f'<div class="error">Query failed: {r.get("error", "unknown error")}</div>'
            row_count = ""

        sections_html += f"""
<section id="section-{r['id']}">
  <div class="section-header">
    <span class="section-badge">{r['id']}</span>
    <div>
      <h2>{r['title']} {row_count}</h2>
      <p class="subtitle">{r['subtitle']}</p>
    </div>
  </div>
  {content}
</section>
"""

    nav_links = "".join(
        f'<a href="#section-{r["id"]}">{r["id"]} — {r["title"]}</a>'
        for r in results
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PAYG Master Dashboard</title>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d27; --surface2: #22263a;
    --border: #2e3250; --text: #e2e8f0; --muted: #8892b0;
    --blue: #4f8ef7; --green: #22c55e; --yellow: #f59e0b; --red: #ef4444;
    --badge-bg: #4f8ef722;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif; font-size: 14px; }}

  header {{ background: var(--surface); border-bottom: 1px solid var(--border); padding: 16px 32px; display: flex; justify-content: space-between; align-items: center; position: sticky; top: 0; z-index: 100; gap: 16px; }}
  header h1 {{ font-size: 18px; font-weight: 700; color: var(--blue); white-space: nowrap; }}
  .header-right {{ display: flex; align-items: center; gap: 16px; }}
  .meta {{ font-size: 12px; color: var(--muted); }}
  #status-msg {{ font-size: 12px; color: var(--muted); }}

  #refresh-btn {{
    background: var(--blue); color: #fff; border: none; padding: 8px 18px;
    border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer;
    transition: opacity .15s; white-space: nowrap;
  }}
  #refresh-btn:hover {{ opacity: .85; }}
  #refresh-btn:disabled {{ opacity: .5; cursor: not-allowed; }}

  nav {{ background: var(--surface2); border-bottom: 1px solid var(--border); padding: 10px 32px; display: flex; gap: 16px; flex-wrap: wrap; overflow-x: auto; }}
  nav a {{ color: var(--muted); text-decoration: none; font-size: 12px; white-space: nowrap; padding: 4px 8px; border-radius: 4px; transition: all .15s; }}
  nav a:hover {{ color: var(--blue); background: var(--badge-bg); }}

  main {{ max-width: 1400px; margin: 0 auto; padding: 32px; display: flex; flex-direction: column; gap: 40px; }}

  section {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }}
  .section-header {{ padding: 20px 24px; border-bottom: 1px solid var(--border); display: flex; align-items: flex-start; gap: 16px; }}
  .section-badge {{ background: var(--blue); color: #fff; font-weight: 800; font-size: 16px; width: 36px; height: 36px; border-radius: 8px; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }}
  h2 {{ font-size: 16px; font-weight: 600; display: flex; align-items: center; gap: 10px; }}
  .subtitle {{ color: var(--muted); font-size: 13px; margin-top: 4px; }}
  .row-count {{ font-size: 11px; font-weight: 400; color: var(--muted); background: var(--surface2); padding: 2px 8px; border-radius: 20px; }}

  .table-wrap {{ overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  thead tr {{ background: var(--surface2); }}
  th {{ padding: 10px 14px; text-align: left; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: .05em; color: var(--muted); border-bottom: 1px solid var(--border); white-space: nowrap; }}
  td {{ padding: 9px 14px; border-bottom: 1px solid var(--border); color: var(--text); white-space: nowrap; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: var(--surface2); }}

  td.good {{ color: var(--green); font-weight: 600; }}
  td.warn {{ color: var(--yellow); font-weight: 600; }}
  td.bad  {{ color: var(--red);   font-weight: 600; }}

  .error {{ padding: 20px 24px; color: var(--red); font-size: 13px; }}

  details.defs {{ border-top: 1px solid var(--border); }}
  details.defs summary {{ padding: 12px 24px; cursor: pointer; font-size: 12px; color: var(--muted); user-select: none; }}
  details.defs summary:hover {{ color: var(--blue); }}
  .def-table {{ width: 100%; border-collapse: collapse; font-size: 12px; background: var(--bg); }}
  .def-table td {{ padding: 8px 24px; border-bottom: 1px solid var(--border); }}
  .def-table tr:last-child td {{ border-bottom: none; }}
  .def-key {{ font-family: monospace; color: var(--blue); width: 260px; }}

  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
  .spinner {{ display: inline-block; width: 12px; height: 12px; border: 2px solid #fff4; border-top-color: #fff; border-radius: 50%; animation: spin .6s linear infinite; margin-right: 6px; vertical-align: middle; }}
</style>
</head>
<body>

<header>
  <h1>⚡ PAYG Master Dashboard</h1>
  <div class="header-right">
    <span class="meta" id="gen-time">Data as of: {generated_at}</span>
    <span id="status-msg"></span>
    <button id="refresh-btn" onclick="triggerRefresh()">🔄 Refresh Data</button>
  </div>
</header>

<nav>{nav_links}</nav>

<main id="dashboard-main">
{sections_html}
</main>

<script>
function triggerRefresh() {{
  const btn = document.getElementById('refresh-btn');
  const msg = document.getElementById('status-msg');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Refreshing...';
  msg.textContent = '';

  fetch('/api/refresh', {{method: 'POST'}})
    .then(r => r.json())
    .then(() => pollStatus())
    .catch(e => {{ btn.disabled = false; btn.textContent = '🔄 Refresh Data'; msg.textContent = 'Error: ' + e; }});
}}

function pollStatus() {{
  fetch('/api/status')
    .then(r => r.json())
    .then(data => {{
      if (data.refreshing) {{
        setTimeout(pollStatus, 2000);
      }} else {{
        // reload page to show new data
        window.location.reload();
      }}
    }});
}}
</script>

</body>
</html>"""


# ─────────────────────────────────────────────
# FLASK ROUTES
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return build_html(_cache["results"])


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if not _cache["refreshing"]:
        threading.Thread(target=refresh_cache, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/status")
def api_status():
    return jsonify({
        "refreshing":    _cache["refreshing"],
        "generated_at":  _cache["generated_at"],
        "section_count": len(_cache["results"]),
    })


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    open_browser = "--no-open" not in sys.argv

    # Initial data load
    print("PAYG Master Dashboard — starting up...")
    refresh_cache()

    # Background auto-refresh every hour
    threading.Thread(target=auto_refresh_loop, daemon=True).start()

    # Open browser after short delay
    if open_browser:
        def _open():
            import time; time.sleep(1)
            webbrowser.open(f"http://localhost:{PORT}")
        threading.Thread(target=_open, daemon=True).start()

    print(f"\nServer running at http://localhost:{PORT}")
    print("Press Ctrl+C to stop.\n")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
