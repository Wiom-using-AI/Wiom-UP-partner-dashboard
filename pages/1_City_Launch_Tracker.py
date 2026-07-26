"""
City Launch Tracker — new city launch scorecard for UP.
Run with: streamlit run up_partner_dashboard.py --server.port 8502
(this file lives in pages/ so Streamlit picks it up automatically as a second page)

Rebuilt (2026-07-26, Anurag): everything on this page is live from Snowflake — nothing
is pulled from the Google Sheet. Only the list of 11 launch cities is fixed
(Saharanpur, Bijnor, Muzaffarnagar, Hapur, Aligarh, Mathura, Firozabad, Moradabad,
Bareilly, Meerut City, Agra); every number shown is queried fresh.

Sources:
  - DBT.AGG_PARTNER_FUNNEL (PARTNER_CITY) — today's roster of partners in each city.
  - PROD_DB.DYNAMODB_READ.HOME_ROUTER_PLAN_INFO — per-customer recharge history, used to
    compute Active Base, day-by-day Installs, and each partner's Total Gross / Active /
    Live / R30+ / M1 Expired / M1 Renewed.
  - PROD_DB.DYNAMODB_READ.BOOKING — day-by-day Leads (by partner LCO_ACCOUNT_ID).
  - DBT.FCT_CX_SERVICE_TICKETS_BY_PARTNER_DAILY — tickets in the last 30 days.

  Fixed 2026-07-26 (Anurag reported: dropdown not switching city, Active Base wrong,
  installs stuck at 0, R30+ never updating):
  - Active Base was reading PROD_DB.PUBLIC.WIOM_DEVICE_DATA, which turned out to be a
    stale/frozen snapshot (no install activity recorded past Jan 2026). Switched to the
    same live HOME_ROUTER_PLAN_INFO-based Active/Live logic used for the partner table.
  - Installs was reading TASK_LOGS for an 'OTP_VERIFIED' event that doesn't exist in that
    table (it's a ticketing log, not an onboarding log) — installs were always 0. Switched
    to each customer's first-ever plan start date in HOME_ROUTER_PLAN_INFO.
  - Leads was filtered to mobiles that already had a plan record, which excludes most real
    leads (people who haven't installed yet). Switched to filtering BOOKING directly by the
    partner's LCO_ACCOUNT_ID.
  - The city dropdown didn't have a stable widget key, which could let Streamlit reuse a
    stale render — added an explicit key so the label and the data always match.

Definitions confirmed with Anurag:
  - Active   = plan currently running, OR within the 15-day grace window after expiry.
  - Live     = plan currently running (strictly not expired, not in grace).
  - R30+     = out of everyone ever installed for this partner, how many have NOT
               recharged in the last 30 days (i.e. it's been 30+ days since their most
               recent recharge, whether or not that plan is still technically running).
               Example: 100 customers installed to date, 70 haven't recharged in the
               last 30 days -> R30+ = 70.
  - M1 Expired = installed 30+ days ago and has NEVER recharged again since the first plan.
  - M1 Renewed = installed 30+ days ago and HAS recharged again at least once since.
  - Tickets / Resolved = last 30 days only.

Fixed 2026-07-26 (Anurag): R30+ previously required BOTH "still Live" AND "30+ days
since last recharge started" — with plans mostly 28-30 days long, that combination is
almost always false, so R30+ was stuck at 0. Removed the Live requirement per Anurag's
clarification: R30+ is simply "haven't recharged in the last 30 days," regardless of
whether their last plan has technically expired yet.
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


# City name as it appears in DBT.AGG_PARTNER_FUNNEL.PARTNER_CITY
CITY_SQL_NAMES = {
    "Saharanpur": {"funnel": "Saharanpur"},
    "Bijnor": {"funnel": "Bijnor"},
    "Muzaffarnagar": {"funnel": "Muzaffarnagar"},
    "Hapur": {"funnel": "Hapur"},
    "Aligarh": {"funnel": "Aligarh"},
    "Mathura": {"funnel": "Mathura"},
    "Firozabad": {"funnel": "Firozabad"},
    "Moradabad": {"funnel": "Moradabad"},
    "Bareilly": {"funnel": "Bareilly"},
    "Meerut City": {"funnel": "Meerut_City"},
    "Agra": {"funnel": "Agra"},
}


@st.cache_data(ttl=3600)
def fetch_active_base(funnel_city):
    sql = f"""
    WITH partner_list AS (
        SELECT DISTINCT PARTNER_ID FROM DBT.AGG_PARTNER_FUNNEL WHERE TRIM(PARTNER_CITY) = '{funnel_city}'
    ),
    plans AS (
        SELECT MOBILE, DATEADD('second', TIME_PLAN, PLAN_START_TIME) AS PLAN_EXPIRY
        FROM PROD_DB.DYNAMODB_READ.HOME_ROUTER_PLAN_INFO
        WHERE LCO_ACCOUNT_ID IN (SELECT PARTNER_ID FROM partner_list)
    ),
    last_plan AS (
        SELECT MOBILE, PLAN_EXPIRY AS LAST_EXPIRY
        FROM plans QUALIFY ROW_NUMBER() OVER (PARTITION BY MOBILE ORDER BY PLAN_EXPIRY DESC) = 1
    )
    SELECT COUNT(DISTINCT CASE WHEN LAST_EXPIRY >= CURRENT_TIMESTAMP
                                 OR DATEDIFF('day', LAST_EXPIRY, CURRENT_TIMESTAMP) <= 15
                            THEN MOBILE END) AS ACTIVE_BASE
    FROM last_plan
    """
    try:
        df = run_sql(sql)
        return safe_int(df.iloc[0]['ACTIVE_BASE']) if not df.empty else 0
    except Exception as e:
        st.error(f"Active Base query failed: {e}")
        return 0


@st.cache_data(ttl=3600)
def fetch_daily_leads_installs(funnel_city):
    sql = f"""
    WITH partner_list AS (
        SELECT DISTINCT PARTNER_ID FROM DBT.AGG_PARTNER_FUNNEL WHERE TRIM(PARTNER_CITY) = '{funnel_city}'
    ),
    plans AS (
        SELECT MOBILE, PLAN_START_TIME
        FROM PROD_DB.DYNAMODB_READ.HOME_ROUTER_PLAN_INFO
        WHERE LCO_ACCOUNT_ID IN (SELECT PARTNER_ID FROM partner_list)
    ),
    first_plan AS (
        SELECT MOBILE, MIN(PLAN_START_TIME) AS FIRST_START
        FROM plans GROUP BY 1
    ),
    installs AS (
        SELECT TO_DATE(FIRST_START) AS DAY, COUNT(DISTINCT MOBILE) AS INSTALLS
        FROM first_plan
        WHERE FIRST_START >= DATEADD('day', -30, CURRENT_DATE)
        GROUP BY 1
    ),
    leads AS (
        SELECT TO_DATE(DATEADD('minute', 330, ADDED_TIME)) AS DAY, COUNT(DISTINCT MOBILE) AS LEADS
        FROM PROD_DB.DYNAMODB_READ.BOOKING
        WHERE LCO_ACCOUNT_ID IN (SELECT PARTNER_ID FROM partner_list)
          AND ADDED_TIME >= DATEADD('day', -30, CURRENT_DATE)
        GROUP BY 1
    )
    SELECT COALESCE(i.DAY, l.DAY) AS DAY, COALESCE(i.INSTALLS, 0) AS INSTALLS, COALESCE(l.LEADS, 0) AS LEADS
    FROM installs i FULL OUTER JOIN leads l ON l.DAY = i.DAY
    ORDER BY 1
    """
    try:
        df = run_sql(sql)
        if df.empty:
            return pd.DataFrame(columns=['DAY', 'INSTALLS', 'LEADS'])
        df['DAY'] = pd.to_datetime(df['DAY'])
        return df
    except Exception as e:
        st.error(f"Daily leads/installs query failed: {e}")
        return pd.DataFrame(columns=['DAY', 'INSTALLS', 'LEADS'])


@st.cache_data(ttl=3600)
def fetch_partner_table(funnel_city):
    sql = f"""
    WITH partner_list AS (
        SELECT DISTINCT PARTNER_ID, PARTNER_NAME
        FROM DBT.AGG_PARTNER_FUNNEL
        WHERE TRIM(PARTNER_CITY) = '{funnel_city}'
    ),
    plans AS (
        SELECT
            LCO_ACCOUNT_ID AS PARTNER_ID,
            MOBILE,
            PLAN_START_TIME,
            DATEADD('second', TIME_PLAN, PLAN_START_TIME) AS PLAN_EXPIRY
        FROM PROD_DB.DYNAMODB_READ.HOME_ROUTER_PLAN_INFO
        WHERE LCO_ACCOUNT_ID IN (SELECT PARTNER_ID FROM partner_list)
    ),
    mobile_counts AS (
        SELECT PARTNER_ID, MOBILE, COUNT(*) AS RECHARGE_COUNT
        FROM plans GROUP BY 1, 2
    ),
    first_plan AS (
        SELECT PARTNER_ID, MOBILE, PLAN_START_TIME AS FIRST_START
        FROM plans QUALIFY ROW_NUMBER() OVER (PARTITION BY MOBILE ORDER BY PLAN_START_TIME ASC) = 1
    ),
    last_plan AS (
        SELECT PARTNER_ID, MOBILE, PLAN_START_TIME AS LAST_START, PLAN_EXPIRY AS LAST_EXPIRY
        FROM plans QUALIFY ROW_NUMBER() OVER (PARTITION BY MOBILE ORDER BY PLAN_START_TIME DESC) = 1
    ),
    mobile_summary AS (
        SELECT
            f.PARTNER_ID, f.MOBILE, f.FIRST_START,
            l.LAST_START, l.LAST_EXPIRY, mc.RECHARGE_COUNT
        FROM first_plan f
        JOIN last_plan l ON l.MOBILE = f.MOBILE
        JOIN mobile_counts mc ON mc.MOBILE = f.MOBILE
    ),
    partner_metrics AS (
        SELECT
            pl.PARTNER_ID, pl.PARTNER_NAME,
            COUNT(DISTINCT ms.MOBILE) AS TOTAL_GROSS,
            COUNT(DISTINCT CASE WHEN ms.LAST_EXPIRY >= CURRENT_TIMESTAMP
                                 OR DATEDIFF('day', ms.LAST_EXPIRY, CURRENT_TIMESTAMP) <= 15
                            THEN ms.MOBILE END) AS ACTIVE,
            COUNT(DISTINCT CASE WHEN ms.LAST_EXPIRY >= CURRENT_TIMESTAMP
                            THEN ms.MOBILE END) AS LIVE,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', ms.LAST_START, CURRENT_TIMESTAMP) >= 30
                            THEN ms.MOBILE END) AS R30_PLUS,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', ms.FIRST_START, CURRENT_TIMESTAMP) >= 30
                                 AND ms.RECHARGE_COUNT = 1
                            THEN ms.MOBILE END) AS M1_EXPIRED,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', ms.FIRST_START, CURRENT_TIMESTAMP) >= 30
                                 AND ms.RECHARGE_COUNT > 1
                            THEN ms.MOBILE END) AS M1_RENEWED
        FROM partner_list pl
        LEFT JOIN mobile_summary ms ON ms.PARTNER_ID = pl.PARTNER_ID
        GROUP BY 1, 2
    ),
    tickets AS (
        SELECT
            PARTNER_ACCOUNT_ID AS PARTNER_ID,
            SUM(TOTAL_TICKETS) AS TICKETS_1M,
            SUM(RESOLVED_TICKETS) AS RESOLVED_1M
        FROM DBT.FCT_CX_SERVICE_TICKETS_BY_PARTNER_DAILY
        WHERE TKT_DT >= DATEADD('day', -30, CURRENT_DATE)
          AND PARTNER_ACCOUNT_ID IN (SELECT PARTNER_ID FROM partner_list)
        GROUP BY 1
    )
    SELECT
        pm.*,
        COALESCE(t.TICKETS_1M, 0) AS TICKETS_1M,
        COALESCE(t.RESOLVED_1M, 0) AS RESOLVED_1M
    FROM partner_metrics pm
    LEFT JOIN tickets t ON t.PARTNER_ID = pm.PARTNER_ID
    ORDER BY pm.TOTAL_GROSS DESC
    """
    try:
        df = run_sql(sql)
        return df
    except Exception as e:
        st.error(f"Partner table query failed: {e}")
        return pd.DataFrame()


# ── UI ───────────────────────────────────────────────────────────────────
st.title("🏙️ City Launch Tracker")
st.caption("Everything on this page is live from Snowflake, refreshed hourly. Nothing is pulled from the launch-tracker Google Sheet.")

city = st.selectbox("Select City", sorted(CITY_SQL_NAMES.keys()), key="city_select")
sql_names = CITY_SQL_NAMES[city]

with st.spinner(f"Loading Active Base for {city}..."):
    active_base = fetch_active_base(sql_names["funnel"])

st.metric("Active Base (city-wide)", f"{active_base:,}")

st.divider()

st.subheader("📈 Leads & Installs — last 30 days")
with st.spinner("Loading daily leads & installs..."):
    daily_df = fetch_daily_leads_installs(sql_names["funnel"])

if not daily_df.empty:
    fig = go.Figure()
    fig.add_trace(go.Bar(x=daily_df['DAY'], y=daily_df['LEADS'], name='Leads', marker_color='#636EFA'))
    fig.add_trace(go.Bar(x=daily_df['DAY'], y=daily_df['INSTALLS'], name='Installs', marker_color='#00CC96'))
    fig.update_layout(barmode='group', plot_bgcolor='rgba(0,0,0,0)', height=360,
                       margin=dict(t=20, b=20), xaxis_title="Date", yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    c1.metric("Total Leads (30 days)", int(daily_df['LEADS'].sum()))
    c2.metric("Total Installs (30 days)", int(daily_df['INSTALLS'].sum()))
else:
    st.info(f"No leads or installs recorded for {city} in the last 30 days.")

st.divider()

st.subheader("🤝 Partners — as of today")
with st.spinner("Loading partner table..."):
    partner_df = fetch_partner_table(sql_names["funnel"])

if not partner_df.empty:
    disp = partner_df.copy()
    disp['ACTIVE_PCT'] = disp.apply(
        lambda r: f"{(safe_int(r['ACTIVE']) / safe_int(r['TOTAL_GROSS']) * 100):.0f}%" if safe_int(r['TOTAL_GROSS']) else "—",
        axis=1
    )
    disp['M1_PCT'] = disp.apply(
        lambda r: f"{(safe_int(r['M1_RENEWED']) / (safe_int(r['M1_EXPIRED']) + safe_int(r['M1_RENEWED'])) * 100):.0f}%"
        if (safe_int(r['M1_EXPIRED']) + safe_int(r['M1_RENEWED'])) else "—",
        axis=1
    )
    disp = disp[['PARTNER_NAME', 'TOTAL_GROSS', 'ACTIVE', 'ACTIVE_PCT', 'LIVE', 'R30_PLUS',
                  'M1_EXPIRED', 'M1_RENEWED', 'M1_PCT', 'TICKETS_1M', 'RESOLVED_1M']]
    disp.columns = ['Partner', 'Total Gross', 'Active', 'Active %', 'Live', 'R30+',
                     'M1 Expired', 'M1 Renewed', 'M1 %', 'Tickets (30d)', 'Resolved (30d)']
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.caption(f"{len(partner_df)} partners currently in {city}, per DBT.AGG_PARTNER_FUNNEL.")
else:
    st.info(f"No partners found for {city}.")

st.caption(f"Source: DBT.AGG_PARTNER_FUNNEL + HOME_ROUTER_PLAN_INFO + BOOKING + FCT_CX_SERVICE_TICKETS_BY_PARTNER_DAILY (Snowflake) · Last refreshed: {datetime.now().strftime('%d %b %Y, %I:%M %p')}")
