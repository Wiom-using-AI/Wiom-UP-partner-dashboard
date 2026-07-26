"""
City Launch Tracker — new city launch scorecard for UP.
Run with: streamlit run up_partner_dashboard.py --server.port 8502
(this file lives in pages/ so Streamlit picks it up automatically as a second page)

Source: rebuilt from "New City Launch tracker (2).xlsx" (Google Sheet shared by Anurag,
26-Jul-2026). The sheet's own formulas were broken (Excel-only XLOOKUP/#REF! errors in
Google Sheets), so this page pulls the same numbers two ways:
  - Targets (Households, Wiom TAM, End Case Partner target, and M6/M12/M18/M24 milestones)
    come from the sheet's "Base Numbers" tab — these are planning inputs, not live data,
    so they're hardcoded below as CITY_CONFIG.
  - Actuals (Active Base, Gross Installs, Booked Leads, Partners) are queried live from
    Snowflake per city:
      - DBT.AGG_PARTNER_FUNNEL (PARTNER_CITY column) for partners / leads / installs
      - PROD_DB.PUBLIC.WIOM_DEVICE_DATA (C_CITY column) for active customer base
Only the 11 cities that have their own tab in the source sheet are covered here
(Saharanpur, Bijnor, Muzaffarnagar, Hapur, Aligarh, Mathura, Firozabad, Moradabad,
Bareilly, Meerut City, Agra). Agra has no M6-M24 targets in the source sheet (it was
already an established zone before this tracker existed), so its milestone table is
shown as "not set".
"""

import os
import requests
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import date, datetime

# ── Credentials (same pattern as up_partner_dashboard.py) ──────────────────
API_KEY = os.getenv('METABASE_API_KEY')
if not API_KEY:
    from pathlib import Path
    env_path = Path(r'C:\credentials\.env')
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())
    API_KEY = os.getenv('METABASE_API_KEY')

BASE_URL = 'https://metabase.wiom.in'
DB_ID    = 113
HEADERS  = {'x-api-key': API_KEY, 'Content-Type': 'application/json'}

today = date.today()

st.set_page_config(page_title="City Launch Tracker", layout="wide")


def run_sql(sql):
    if not API_KEY:
        raise RuntimeError("METABASE_API_KEY not set — add it to C:\\credentials\\.env")
    payload = {'database': DB_ID, 'type': 'native', 'native': {'query': sql}}
    r = requests.post(f"{BASE_URL}/api/dataset", headers=HEADERS, json=payload, timeout=300)
    data = r.json()
    if isinstance(data, dict) and data.get('status') == 'failed':
        raise RuntimeError(f"Query failed: {data.get('error', 'unknown')}")
    result = data.get('data', data) if isinstance(data, dict) else {}
    cols = [c['name'] for c in result.get('cols', [])]
    rows = result.get('rows', [])
    return pd.DataFrame(rows, columns=cols)


def safe_int(val):
    try:
        return int(float(val)) if val is not None else 0
    except Exception:
        return 0


def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except Exception:
        return 0.0


# ── Static targets, from the "Base Numbers" tab of the source sheet ────────
# milestones = (month_number, target_count, target_penetration_pct) or None if not set
CITY_CONFIG = {
    "Saharanpur": dict(launch="2025-01-22", population=981938, households=196388, tam=81513,
                        end_case_partner=32,
                        milestones=[(6, 1285, 1.58), (12, 3408, 4.18), (18, 4662, 5.72), (24, 5328, 6.54)]),
    "Bijnor": dict(launch="2024-11-03", population=130252, households=26050, tam=10812,
                   end_case_partner=4,
                   milestones=[(6, 320, 2.96), (12, 499, 4.61), (18, 594, 5.49), (24, 645, 5.96)]),
    "Muzaffarnagar": dict(launch="2024-06-25", population=547900, households=109580, tam=45482,
                           end_case_partner=18,
                           milestones=[(6, 1299, 2.86), (12, 2225, 4.89), (18, 2718, 5.98), (24, 2979, 6.55)]),
    "Hapur": dict(launch="2024-06-05", population=366896, households=73379, tam=30457,
                  end_case_partner=12,
                  milestones=[(6, 878, 2.88), (12, 1563, 5.13), (18, 1927, 6.33), (24, 2121, 6.96)]),
    "Aligarh": dict(launch="2025-03-10", population=1218200, households=243640, tam=101125,
                     end_case_partner=40,
                     milestones=[(6, 1233, 1.22), (12, 2573, 2.54), (18, 3814, 3.77), (24, 5302, 5.24)]),
    "Mathura": dict(launch="2025-03-28", population=487707, households=97541, tam=40486,
                     end_case_partner=16,
                     milestones=[(6, 1186, 2.93), (12, 2056, 5.08), (18, 2518, 6.22), (24, 2763, 6.83)]),
    "Firozabad": dict(launch="2025-04-13", population=842959, households=168592, tam=69976,
                       end_case_partner=28,
                       milestones=[(6, 2017, 2.88), (12, 3484, 4.98), (18, 4264, 6.09), (24, 4678, 6.69)]),
    "Moradabad": dict(launch="2025-03-21", population=1242261, households=248452, tam=103123,
                       end_case_partner=41,
                       milestones=[(6, 1285, 1.25), (12, 2784, 2.70), (18, 4161, 4.03), (24, 5722, 5.55)]),
    "Bareilly": dict(launch="2025-03-21", population=1225464, households=245093, tam=101728,
                      end_case_partner=40,
                      milestones=[(6, 1285, 1.26), (12, 2784, 2.74), (18, 4161, 4.09), (24, 5722, 5.62)]),
    "Meerut City": dict(launch="2025-01-01", population=1822506, households=364501, tam=151290,
                         end_case_partner=60,
                         milestones=[(6, 1754, 1.16), (12, 3498, 2.31), (18, 5349, 3.54), (24, 7488, 4.95)]),
    "Agra": dict(launch="2025-02-12", population=2213797, households=442759, tam=183772,
                 end_case_partner=66,
                 milestones=None),
}

# City name as it appears in each live Snowflake source
CITY_SQL_NAMES = {
    "Saharanpur": {"funnel": "Saharanpur", "device": "Saharanpur"},
    "Bijnor": {"funnel": "Bijnor", "device": "Bijnor"},
    "Muzaffarnagar": {"funnel": "Muzaffarnagar", "device": "Muzaffarnagar"},
    "Hapur": {"funnel": "Hapur", "device": "Hapur"},
    "Aligarh": {"funnel": "Aligarh", "device": "Aligarh"},
    "Mathura": {"funnel": "Mathura", "device": "Mathura"},
    "Firozabad": {"funnel": "Firozabad", "device": "Firozabad"},
    "Moradabad": {"funnel": "Moradabad", "device": "Moradabad"},
    "Bareilly": {"funnel": "Bareilly", "device": "Bareilly"},
    "Meerut City": {"funnel": "Meerut_City", "device": "Meerut"},
    "Agra": {"funnel": "Agra", "device": "Agra"},
}


@st.cache_data(ttl=3600)
def fetch_live_metrics(funnel_city, device_city):
    """Live actuals for one city: partners/leads/installs from the partner funnel,
    active customer base from the device table."""
    sql = f"""
    WITH funnel AS (
        SELECT
            COUNT(DISTINCT PARTNER_ID)                                            AS TOTAL_PARTNERS,
            COUNT(DISTINCT CASE WHEN INSTALLED_LEADS > 0 THEN PARTNER_ID END)     AS ACTIVE_PARTNERS,
            SUM(ASSIGNED_LEADS)                                                    AS BOOKED_LEADS,
            SUM(INSTALLED_LEADS)                                                   AS GROSS_INSTALLS
        FROM DBT.AGG_PARTNER_FUNNEL
        WHERE TRIM(PARTNER_CITY) = '{funnel_city}'
    ),
    device AS (
        SELECT
            COUNT(DISTINCT CASE WHEN CUSTOMER_STATUS = 'Active_Customer' THEN CUSTOMER_ID END) AS ACTIVE_BASE
        FROM PROD_DB.PUBLIC.WIOM_DEVICE_DATA
        WHERE TRIM(C_CITY) = '{device_city}'
    )
    SELECT * FROM funnel, device
    """
    try:
        df = run_sql(sql)
        if df.empty:
            return dict(total_partners=0, active_partners=0, booked_leads=0, gross_installs=0, active_base=0)
        r = df.iloc[0]
        return dict(
            total_partners=safe_int(r.get('TOTAL_PARTNERS')),
            active_partners=safe_int(r.get('ACTIVE_PARTNERS')),
            booked_leads=safe_int(r.get('BOOKED_LEADS')),
            gross_installs=safe_int(r.get('GROSS_INSTALLS')),
            active_base=safe_int(r.get('ACTIVE_BASE')),
        )
    except Exception as e:
        st.error(f"Live query failed: {e}")
        return dict(total_partners=0, active_partners=0, booked_leads=0, gross_installs=0, active_base=0)


def expected_at(months_elapsed, milestones):
    """Linear interpolation of a target curve through (0,0) and the M6/M12/M18/M24
    milestones; extrapolates past M24 using the M18->M24 slope."""
    if not milestones:
        return None
    pts = [(0, 0)] + [(m, v) for m, v, _ in milestones]
    if months_elapsed <= 0:
        return 0
    for i in range(len(pts) - 1):
        m0, v0 = pts[i]
        m1, v1 = pts[i + 1]
        if months_elapsed <= m1:
            frac = (months_elapsed - m0) / (m1 - m0)
            return v0 + frac * (v1 - v0)
    m0, v0 = pts[-2]
    m1, v1 = pts[-1]
    slope = (v1 - v0) / (m1 - m0)
    return v1 + slope * (months_elapsed - m1)


def pct_achieved(actual, expected):
    if expected is None or expected == 0:
        return None
    return (actual / expected) * 100


def fmt_pct(p):
    if p is None:
        return "—"
    return f"{p:.1f}%"


# ── UI ───────────────────────────────────────────────────────────────────
st.title("🏙️ City Launch Tracker")
st.caption(
    "Targets are the launch-plan figures from the New City Launch tracker sheet. "
    "Actuals (Active Base, Installs, Leads, Partners) are live from Snowflake, refreshed hourly."
)

city = st.selectbox("Select City", sorted(CITY_CONFIG.keys()))
cfg = CITY_CONFIG[city]
sql_names = CITY_SQL_NAMES[city]

launch_date = datetime.strptime(cfg["launch"], "%Y-%m-%d").date()
days_since_launch = (today - launch_date).days
months_since_launch = days_since_launch / 30.44

col1, col2, col3 = st.columns(3)
col1.metric("Launch Day", launch_date.strftime("%d %b %Y"))
col2.metric("Days Since Launch", days_since_launch)
col3.metric("Months Since Launch", f"{months_since_launch:.1f}")

with st.spinner("Loading live metrics..."):
    live = fetch_live_metrics(sql_names["funnel"], sql_names["device"])

st.divider()

st.subheader("📊 Reach")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Population", f"{cfg['population']:,}")
c2.metric("Households", f"{cfg['households']:,}")
c3.metric("Wiom TAM", f"{cfg['tam']:,}")
active_base_pen = (live['active_base'] / cfg['tam'] * 100) if cfg['tam'] else 0
c4.metric("Active Base", f"{live['active_base']:,}", f"{active_base_pen:.2f}% of TAM")

st.subheader("🔧 Installs & Leads")
expected_installs = expected_at(months_since_launch, cfg["milestones"])
expected_leads = None  # sheet doesn't separately target leads at milestones; installs is the primary target
c1, c2, c3 = st.columns(3)
c1.metric("Gross Installs (Actual)", f"{live['gross_installs']:,}")
if expected_installs is not None:
    c2.metric("Expected Installs (pacing)", f"{expected_installs:,.0f}")
    c3.metric("% Achieved", fmt_pct(pct_achieved(live['gross_installs'], expected_installs)))
else:
    c2.metric("Expected Installs (pacing)", "not set")
    c3.metric("% Achieved", "—")
st.metric("Booked Leads (Actual)", f"{live['booked_leads']:,}")

st.subheader("🤝 Partners")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Partners (Actual)", live['total_partners'])
c2.metric("Active Partners (Actual)", live['active_partners'])
c3.metric("End-Case Partner Target", cfg['end_case_partner'])
partner_pct = pct_achieved(live['total_partners'], cfg['end_case_partner'])
c4.metric("% Achieved", fmt_pct(partner_pct))
st.caption("Active Partners = partners with at least one completed install to date.")

st.divider()

st.subheader("🎯 Active Base Milestones (M6 / M12 / M18 / M24)")
if cfg["milestones"]:
    rows = []
    for m, target, pen in cfg["milestones"]:
        rows.append({
            "Milestone": f"M{m}",
            "Target Active Base": f"{target:,}",
            "Target Penetration": f"{pen:.2f}%",
            "Status": "✅ Passed" if months_since_launch >= m else ("🔵 Current" if months_since_launch >= m - 3 else "⚪ Upcoming"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    expected_active_base = expected_at(months_since_launch, cfg["milestones"])
    c1, c2, c3 = st.columns(3)
    c1.metric("Actual Active Base", f"{live['active_base']:,}")
    c2.metric("Expected Active Base (pacing)", f"{expected_active_base:,.0f}")
    c3.metric("% Achieved", fmt_pct(pct_achieved(live['active_base'], expected_active_base)))

    fig = go.Figure()
    months_x = [0] + [m for m, _, _ in cfg["milestones"]]
    target_y = [0] + [v for _, v, _ in cfg["milestones"]]
    fig.add_trace(go.Scatter(x=months_x, y=target_y, mode='lines+markers', name='Target',
                              line=dict(color='#636EFA', dash='dash')))
    fig.add_trace(go.Scatter(x=[months_since_launch], y=[live['active_base']], mode='markers',
                              name='Actual (today)', marker=dict(color='#EF553B', size=14, symbol='star')))
    fig.update_layout(title="Active Base Pacing vs Target", xaxis_title="Months Since Launch",
                       yaxis_title="Active Base", plot_bgcolor='rgba(0,0,0,0)', height=380)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info(f"No M6-M24 milestone targets are set for {city} in the source sheet.")

st.caption(f"Source: New City Launch tracker sheet (targets) · DBT.AGG_PARTNER_FUNNEL + WIOM_DEVICE_DATA (live actuals, Snowflake) · Last refreshed: {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
