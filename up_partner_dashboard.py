"""
UP Partner Dashboard — powered by Metabase card 7876 (Partner wise earnings view)
Run with: streamlit run up_partner_dashboard.py --server.port 8502
"""

import os
import time
import requests
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv(r'C:\credentials\.env')

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

# Month labels for M3→M2→M1→M0
m3_label = (today.replace(day=1) - relativedelta(months=3)).strftime('%b %Y')
m2_label = (today.replace(day=1) - relativedelta(months=2)).strftime('%b %Y')
m1_label = (today.replace(day=1) - relativedelta(months=1)).strftime('%b %Y')
m0_label =  today.replace(day=1).strftime('%b %Y')

UP_ZONES = {'Agra','Agra++','Meerut','Meerut++','Bareilly','Bareilly++',
            'Gorakhpur','Prayagraj','Lucknow','Meerut City'}

st.set_page_config(page_title="UP Partner Dashboard", page_icon="📊", layout="wide")

# Auto-refresh every 24 hours
REFRESH_INTERVAL_S = 24 * 60 * 60
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()
if time.time() - st.session_state.last_refresh > REFRESH_INTERVAL_S:
    st.session_state.last_refresh = time.time()
    st.cache_data.clear()
    st.rerun()


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


@st.cache_data(ttl=86400)
def fetch_card7876_all():
    """Fetch all partners from card 7876 (Partner wise earnings view)."""
    if not API_KEY:
        raise RuntimeError("METABASE_API_KEY not set")
    r = requests.post(
        f"{BASE_URL}/api/card/7876/query",
        headers=HEADERS, json={}, timeout=120
    )
    data = r.json()
    if isinstance(data, dict) and data.get('status') == 'failed':
        raise RuntimeError(f"Card query failed: {data.get('error', 'unknown')}")
    result = data.get('data', data) if isinstance(data, dict) else {}
    cols = [c['name'] for c in result.get('cols', [])]
    rows = result.get('rows', [])
    df = pd.DataFrame(rows, columns=cols)
    # Filter UP zones
    return df[df['ZONE'].isin(UP_ZONES)].reset_index(drop=True)


@st.cache_data(ttl=86400)
def fetch_partner_growth_card(partner_id):
    """Supplement data from partner_growth_card_raw for tickets + Dec 15 snapshot."""
    sql = f'SELECT * FROM PUBLIC."partner_growth_card_raw" WHERE "partner_id" = {partner_id}'
    df = run_sql(sql)
    return df.iloc[0] if len(df) > 0 else None


@st.cache_data(ttl=3600)
def fetch_live_tickets_m0(partner_id):
    """Fetch current-month ticket counts from TICKETVANILLA_AUDIT (used when growth card m0 = 0)."""
    m0_start = today.replace(day=1).strftime('%Y-%m-01')
    sql = f"""
    SELECT
        TYPE,
        COUNT(DISTINCT TASK_ID) AS tickets
    FROM PUBLIC.TICKETVANILLA_AUDIT
    WHERE ASSIGNED_ACCOUNT_ID = {partner_id}
      AND EVENT_NAME = 'TICKET_CREATED'
      AND ADDED_TIME::TIMESTAMP >= '{m0_start}'
    GROUP BY 1
    """
    try:
        df = run_sql(sql)
        if df is None or len(df) == 0:
            return 0, 0
        type_map = {r['TYPE']: safe_int(r['TICKETS']) for _, r in df.iterrows()}
        svc = type_map.get('SERVICE', 0)
        dev = type_map.get('ROUTER_PICKUP', 0)
        return svc, dev
    except Exception:
        return 0, 0


@st.cache_data(ttl=3600)
def fetch_active_base_all_months(partner_id):
    """
    Returns (m0, m1, m2, m3) active base using the same PJK/ACTIVE_CUST methodology:
    - M0: PARTNER_JANAM_KUNDLI.ACTIVE_CUSTOMER (same source as PJK dashboard)
    - M1/M2/M3: CUSTOMER_METRICS SCD2 with PLAN_EXPIRY + 15-day grace at each month-end
    """
    from dateutil.relativedelta import relativedelta as rdelta

    # M0 from PJK (exact match with what PJK dashboard shows)
    m0 = 0
    try:
        df = run_sql(f"SELECT ACTIVE_CUSTOMER FROM PARTNER_JANAM_KUNDLI WHERE PARTNER_ID = {partner_id}")
        if df is not None and len(df) > 0:
            m0 = safe_int(df.iloc[0]['ACTIVE_CUSTOMER'])
    except Exception:
        pass

    # M1/M2/M3 from CUSTOMER_METRICS SCD2 — same 15-day grace logic as ACTIVE_CUST
    results = [m0]
    for i in range(1, 4):
        month_end = (today.replace(day=1) - rdelta(months=i) + rdelta(months=1) - rdelta(days=1))
        sql = f"""
        SELECT COUNT(DISTINCT CUSTOMER_NAS)
        FROM CUSTOMER_DB_CUSTOMER_PROFILE_SERVICE_PUBLIC.CUSTOMER_METRICS
        WHERE LCO_ACCOUNT_ID = {partner_id}
          AND _FIVETRAN_START::DATE <= '{month_end}'
          AND _FIVETRAN_END::DATE > '{month_end}'
          AND DATEADD(day, 15, PLAN_EXPIRY_DATE::DATE) >= '{month_end}'
        """
        try:
            df = run_sql(sql)
            results.append(safe_int(df.iloc[0][0]) if df is not None and len(df) > 0 else 0)
        except Exception:
            results.append(0)

    return tuple(results)  # (m0, m1, m2, m3)


@st.cache_data(ttl=86400)
def fetch_fixed_payout_monthly(partner_id):
    """Get actual monthly fixed per-renewal payout from PARTNER_BONUS_DISBURSEMENT (BONUS_STATUS=5 = paid)."""
    dec_start = (today.replace(day=1) - relativedelta(months=3)).strftime('%Y-%m-01')
    sql = f"""
    SELECT
        DATE_TRUNC('month', ADDED_TIME)::DATE AS month,
        SUM(AMOUNT) AS fixed_paid,
        COUNT(*) AS renewal_count
    FROM DYNAMODB.PARTNER_BONUS_DISBURSEMENT
    WHERE ACCOUNT_ID = {partner_id}
      AND BONUS_STATUS = 5
      AND _FIVETRAN_DELETED = FALSE
      AND ADDED_TIME >= '{dec_start}'
    GROUP BY 1
    ORDER BY 1 DESC
    """
    try:
        df = run_sql(sql)
        result = {}
        for _, r in df.iterrows():
            month_key = pd.to_datetime(r['MONTH']).strftime('%Y-%m')
            result[month_key] = safe_float(r['FIXED_PAID'])
        return result
    except Exception:
        return {}


@st.cache_data(ttl=86400)
def fetch_rohit_earnings(partner_id):
    dec_start = (today.replace(day=1) - relativedelta(months=3)).strftime('%Y-%m-01')
    sql = f"""
    SELECT
        DATE_TRUNC('month', CREATED_AT)::DATE AS month,
        SUM(CASE WHEN STATUS = 'CLAIMED'   THEN DISBURSEMENT_AMOUNT ELSE 0 END) AS claimed,
        SUM(CASE WHEN STATUS = 'UNCLAIMED' THEN DISBURSEMENT_AMOUNT ELSE 0 END) AS unclaimed,
        SUM(CASE WHEN STATUS = 'PROMISED'  THEN DISBURSEMENT_AMOUNT ELSE 0 END) AS promised,
        SUM(CASE WHEN STATUS = 'MISSED'    THEN DISBURSEMENT_AMOUNT ELSE 0 END) AS missed,
        SUM(CASE WHEN STATUS = 'EXPIRED'   THEN DISBURSEMENT_AMOUNT ELSE 0 END) AS expired,
        SUM(DISBURSEMENT_AMOUNT) AS total_earned,
        COUNT(DISTINCT PARTNER_ID) AS rohit_count
    FROM PARTNER_INCENTIVE_SERVICE_PUBLIC.PARTNER_INCENTIVES
    WHERE LCO_ACCOUNT_ID = {partner_id}
      AND USER_TYPE = 'ROHIT'
      AND _FIVETRAN_DELETED = FALSE
      AND CREATED_AT >= '{dec_start}'
    GROUP BY 1
    ORDER BY 1 DESC
    """
    return run_sql(sql)


# ── UI ────────────────────────────────────────────────────────────────────────
st.title("📊 UP Partner Dashboard")
st.caption(f"Last refreshed: {datetime.now().strftime('%d %b %Y, %I:%M %p')} · Auto-refreshes daily · Source: Partner Wise Earnings View")

try:
    with st.spinner("Loading UP partners from earnings view..."):
        all_df = fetch_card7876_all()
except Exception as e:
    st.error(f"Failed to load partner data: {e}")
    st.stop()

if all_df is None or len(all_df) == 0:
    st.error("Partner list returned empty.")
    st.stop()

partner_names = sorted(all_df['PARTNER_NAME'].dropna().tolist())
selected_name = st.selectbox(
    "🔍 Search Partner Name",
    options=[""] + partner_names,
    index=0,
    placeholder="Type to search..."
)

if not selected_name:
    st.info("👆 Select a partner above to view their performance data.")
    st.subheader(f"All UP Partners — Quick Overview ({len(all_df)} partners)")
    disp = all_df[['PARTNER_NAME', 'ZONE', 'ACTIVE_BASE', 'PARTNER_STATUS',
                    'LIFETIME_EARNING', 'TOTAL_M0_PAYOUT', 'TOTAL_M1_PAYOUT']].copy()
    disp['LIFETIME_EARNING'] = disp['LIFETIME_EARNING'].apply(lambda x: f"₹{safe_float(x):,.0f}")
    disp['TOTAL_M0_PAYOUT']  = disp['TOTAL_M0_PAYOUT'].apply(lambda x: f"₹{safe_float(x):,.0f}")
    disp['TOTAL_M1_PAYOUT']  = disp['TOTAL_M1_PAYOUT'].apply(lambda x: f"₹{safe_float(x):,.0f}")
    disp = disp.sort_values('ACTIVE_BASE', ascending=False).rename(columns={
        'PARTNER_NAME': 'Partner', 'ZONE': 'Zone', 'ACTIVE_BASE': 'Active Base',
        'PARTNER_STATUS': 'Status', 'LIFETIME_EARNING': 'Lifetime Earning',
        'TOTAL_M0_PAYOUT': f'{m0_label} Payout', 'TOTAL_M1_PAYOUT': f'{m1_label} Payout'
    })
    st.dataframe(disp, use_container_width=True, height=500, hide_index=True)
    st.stop()

# Get partner row from card 7876
row = all_df[all_df['PARTNER_NAME'] == selected_name].iloc[0]
partner_id = int(float(row['PARTNER_ACCOUNT_ID']))

# Load supplementary data (tickets + Dec 15)
try:
    pgc = fetch_partner_growth_card(partner_id)
except Exception:
    pgc = None

# ── Partner Header ─────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Zone", str(row.get('ZONE', '-')))
c2.metric("Status", str(row.get('PARTNER_STATUS', '-')))
c3.metric("Rating", f"{safe_float(row.get('CURRENT_RATING')):.2f}")
c4.metric("Device SLA", f"{safe_float(row.get('CURRENT_DEVICE_SLA'))*100:.1f}%")
c5.metric("Service SLA", f"{safe_float(row.get('CURRENT_SERVICE_SLA'))*100:.1f}%")

st.divider()

# ── Active Base ───────────────────────────────────────────────────────────────
st.subheader("👥 Active Customer Base")
st.caption(
    f"**Active Base** = customers with active recharge + those within 15-day grace window. "
    f"Shown month-end for {m3_label}–{m1_label}, and MTD for {m0_label}. "
    f"Rating & Device SLA bonus are calculated on renewals (not active base)."
)

# M0: PARTNER_JANAM_KUNDLI (matches PJK dashboard)
# M1/M2/M3: CUSTOMER_METRICS SCD2 with same 15-day grace logic as PJK/ACTIVE_CUST
with st.spinner("Loading active base..."):
    m0_active, m1_active, m2_active, m3_active = fetch_active_base_all_months(partner_id)

# Display metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric(f"Active Base {m3_label}", m3_active)
c2.metric(f"Active Base {m2_label}", m2_active, delta=m2_active - m3_active if m3_active else None)
c3.metric(f"Active Base {m1_label}", m1_active, delta=m1_active - m2_active if m2_active else None)
c4.metric(f"Active Base {m0_label}", m0_active, delta=m0_active - m1_active if m1_active else None,
          help="Customers with active recharge + those within 15-day grace window")

fig_ac = go.Figure()
fig_ac.add_trace(go.Bar(
    name='Active Base',
    x=[m3_label, m2_label, m1_label, m0_label],
    y=[m3_active, m2_active, m1_active, m0_active],
    marker_color='#636EFA',
    text=[m3_active, m2_active, m1_active, m0_active],
    textposition='outside'
))
fig_ac.update_layout(
    title="Monthly Active Base Trend (incl. 15-day grace window)",
    yaxis_title="Customers",
    plot_bgcolor='rgba(0,0,0,0)',
    height=300, margin=dict(t=40, b=20)
)
st.plotly_chart(fig_ac, use_container_width=True)

st.divider()

# ── Installs & Engagement ─────────────────────────────────────────────────────
st.subheader("📥 Installs & Engagement")

installs  = [safe_int(row.get('M2_INSTALLS')), safe_int(row.get('M1_INSTALLS')), safe_int(row.get('M0_INSTALLS_MTD'))]
notifs    = [safe_int(row.get('M2_NOTIFICATIONS')), safe_int(row.get('M1_NOTIFICATIONS')), safe_int(row.get('M0_NOTIFICATIONS_MTD'))]
interests = [safe_int(row.get('M2_INTERESTS')), safe_int(row.get('M1_INTERESTS')), safe_int(row.get('M0_INTERESTS_MTD'))]
month_3 = [m2_label, m1_label, m0_label]

c1, c2 = st.columns(2)
with c1:
    fig_inst = go.Figure()
    fig_inst.add_trace(go.Bar(name='Installs', x=month_3, y=installs,
                              marker_color='#00CC96', text=installs, textposition='outside'))
    fig_inst.update_layout(title="Monthly Installs", plot_bgcolor='rgba(0,0,0,0)',
                           height=280, margin=dict(t=40, b=20))
    st.plotly_chart(fig_inst, use_container_width=True)

with c2:
    fig_eng = go.Figure()
    fig_eng.add_trace(go.Scatter(x=month_3, y=notifs, mode='lines+markers+text',
                                  name='Notifications', text=notifs, textposition='top center',
                                  line=dict(color='#636EFA', width=2)))
    fig_eng.add_trace(go.Scatter(x=month_3, y=interests, mode='lines+markers+text',
                                  name='Interests', text=interests, textposition='top center',
                                  line=dict(color='#FFA15A', width=2)))
    fig_eng.update_layout(title="Notifications & Interests", plot_bgcolor='rgba(0,0,0,0)',
                           height=280, margin=dict(t=40, b=20))
    st.plotly_chart(fig_eng, use_container_width=True)

st.divider()

# ── Earnings ──────────────────────────────────────────────────────────────────
st.subheader("💰 Earnings")

lifetime = safe_float(row.get('LIFETIME_EARNING'))
partner_lottery = safe_float(row.get('PARTNER_LOTTERY_EARNING'))
rohit_lottery   = safe_float(row.get('ROHIT_LOTTERY_EARNING'))

# Month keys for lookup
m0_key = today.replace(day=1).strftime('%Y-%m')
m1_key = (today.replace(day=1) - relativedelta(months=1)).strftime('%Y-%m')
m2_key = (today.replace(day=1) - relativedelta(months=2)).strftime('%Y-%m')

# Fetch actual fixed payouts from PARTNER_BONUS_DISBURSEMENT
with st.spinner("Loading fixed payout data..."):
    fixed_payout_actual = fetch_fixed_payout_monthly(partner_id)

# Card 7876 fixed payout values (may be 0 for recent months)
card_fixed = [
    safe_float(row.get('M2_FIXED_PAYOUT')),
    safe_float(row.get('M1_FIXED_PAYOUT')),
    safe_float(row.get('M0_FIXED_PAYOUT')),
]
# Use PARTNER_BONUS_DISBURSEMENT when card 7876 shows 0
fixed_payouts = [
    card_fixed[i] if card_fixed[i] > 0 else fixed_payout_actual.get(k, 0)
    for i, k in enumerate([m2_key, m1_key, m0_key])
]

rating_payouts = [safe_float(row.get('M2_RATING_BONUS_PAYOUT')),   safe_float(row.get('M1_RATING_BONUS_PAYOUT')),   safe_float(row.get('M0_RATING_BONUS_PAYOUT'))]
svc_sla_payouts= [safe_float(row.get('M2_SERVICE_SLA_BONUS_PAYOUT')),safe_float(row.get('M1_SERVICE_SLA_BONUS_PAYOUT')),safe_float(row.get('M0_SERVICE_SLA_BONUS_PAYOUT'))]
dev_sla_payouts= [safe_float(row.get('M2_DEVICE_SLA_BONUS_PAYOUT')), safe_float(row.get('M1_DEVICE_SLA_BONUS_PAYOUT')), safe_float(row.get('M0_DEVICE_SLA_BONUS_PAYOUT'))]

# Recompute total: card 7876 total + fixed correction where card showed 0
total_payouts = [
    safe_float(row.get(f'TOTAL_M{2-i}_PAYOUT')) + (fixed_payouts[i] if card_fixed[i] == 0 else 0)
    for i in range(3)
]

avg_3m = sum(total_payouts[:3]) / 3 if any(total_payouts) else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Lifetime Earning", f"₹{lifetime:,.0f}")
c2.metric("Avg Monthly Earning (3M)", f"₹{avg_3m:,.0f}")
c3.metric(f"{m0_label} Payout (MTD)", f"₹{total_payouts[2]:,.0f}")
c4.metric("Partner Lottery (Lifetime)", f"₹{partner_lottery:,.0f}",
          help=f"Rohit Lottery: ₹{rohit_lottery:,.0f} · Both are lifetime totals from earnings view")

fig_earn = go.Figure()
fig_earn.add_trace(go.Bar(name='Fixed Payout (per renewal)', x=month_3, y=fixed_payouts,   marker_color='#FFA15A'))
fig_earn.add_trace(go.Bar(name='Rating Bonus',               x=month_3, y=rating_payouts,  marker_color='#636EFA'))
fig_earn.add_trace(go.Bar(name='Service SLA Bonus',          x=month_3, y=svc_sla_payouts, marker_color='#00CC96'))
fig_earn.add_trace(go.Bar(name='Device SLA Bonus',           x=month_3, y=dev_sla_payouts, marker_color='#EF553B'))
fig_earn.update_layout(
    barmode='stack',
    title="Monthly Earnings Breakdown (Fixed + Rating + Service SLA + Device SLA)",
    yaxis_title="₹",
    plot_bgcolor='rgba(0,0,0,0)', height=340, margin=dict(t=40, b=20)
)
# Total labels on top of each stack
fig_earn.add_trace(go.Scatter(
    x=month_3, y=[v + max(total_payouts) * 0.02 for v in total_payouts],
    mode='text',
    text=[f"₹{v:,.0f}" for v in total_payouts],
    textposition='top center', showlegend=False,
    textfont=dict(size=11, color='#FFFFFF')
))
st.plotly_chart(fig_earn, use_container_width=True)

# Note about data sources
fixed_source_note = []
for i, (key, label) in enumerate([(m2_key, m2_label), (m1_key, m1_label), (m0_key, m0_label)]):
    if card_fixed[i] == 0 and fixed_payout_actual.get(key, 0) > 0:
        fixed_source_note.append(f"**{label}** fixed payout pulled from `PARTNER_BONUS_DISBURSEMENT` (card 7876 showed ₹0)")
if fixed_source_note:
    st.caption("ℹ️ " + " · ".join(fixed_source_note))

# Earnings breakdown table
with st.expander("📋 Detailed Earnings Breakdown"):
    earn_detail = pd.DataFrame({
        'Month': month_3,
        'Fixed Payout': [f"₹{v:,.0f}" for v in fixed_payouts],
        'Rating Bonus': [f"₹{v:,.0f}" for v in rating_payouts],
        'Service SLA Bonus': [f"₹{v:,.0f}" for v in svc_sla_payouts],
        'Device SLA Bonus': [f"₹{v:,.0f}" for v in dev_sla_payouts],
        'Total Payout': [f"₹{v:,.0f}" for v in total_payouts],
        'Rating Score': [f"{safe_float(row.get(f'M{2-i}_RATING_AT_PAYOUT')):.2f}" for i in range(3)],
        'Renewals': [safe_int(row.get(f'M{2-i}_RATING_BONUS_RENEWALS')) for i in range(3)],
    })
    st.dataframe(earn_detail, use_container_width=True, hide_index=True)

    if partner_lottery > 0 or rohit_lottery > 0:
        st.markdown(f"**Lottery Earnings (Lifetime):** Partner ₹{partner_lottery:,.0f} &nbsp;|&nbsp; Rohit ₹{rohit_lottery:,.0f}")

st.divider()

# ── Tickets (from partner_growth_card_raw) ────────────────────────────────────
st.subheader("🎫 Service & Device Tickets")

if pgc is not None:
    svc_tickets = [safe_int(pgc.get(f'service_ticket_m{i}')) for i in [2, 1, 0]]
    svc_sla_cnt = [safe_int(pgc.get(f'service_ticket_sla_m{i}')) for i in [2, 1, 0]]
    dev_tickets = [safe_int(pgc.get(f'device_ticket_m{i}')) for i in [2, 1, 0]]
    dev_sla_cnt = [safe_int(pgc.get(f'device_ticket_sla_m{i}')) for i in [2, 1, 0]]

    # If current month (m0) is 0 in growth card, pull live data from TICKETVANILLA_AUDIT
    if svc_tickets[2] == 0 and dev_tickets[2] == 0:
        live_svc, live_dev = fetch_live_tickets_m0(partner_id)
        if live_svc > 0 or live_dev > 0:
            svc_tickets[2] = live_svc
            dev_tickets[2] = live_dev
            st.caption(f"ℹ️ **{m0_label}** ticket counts are MTD (live from TICKETVANILLA · SLA calculated at month-end)")

    c1, c2 = st.columns(2)
    with c1:
        fig_svc = go.Figure()
        fig_svc.add_trace(go.Bar(name='Tickets', x=month_3, y=svc_tickets, marker_color='#19D3F3'))
        fig_svc.add_trace(go.Bar(name='SLA Met', x=month_3, y=svc_sla_cnt, marker_color='#00CC96'))
        fig_svc.update_layout(barmode='group', title="Service Tickets",
                               plot_bgcolor='rgba(0,0,0,0)', height=280, margin=dict(t=40, b=20))
        st.plotly_chart(fig_svc, use_container_width=True)

    with c2:
        fig_dev = go.Figure()
        fig_dev.add_trace(go.Bar(name='Tickets', x=month_3, y=dev_tickets, marker_color='#FF6692'))
        fig_dev.add_trace(go.Bar(name='SLA Met', x=month_3, y=dev_sla_cnt, marker_color='#B6E880'))
        fig_dev.update_layout(barmode='group', title="Device Tickets",
                               plot_bgcolor='rgba(0,0,0,0)', height=280, margin=dict(t=40, b=20))
        st.plotly_chart(fig_dev, use_container_width=True)
else:
    st.info("Ticket data not available for this partner.")

st.divider()

# ── Rohit Earnings ────────────────────────────────────────────────────────────
st.subheader("🔧 Rohit (Field Technician) Earnings — Month Wise")

with st.spinner("Loading Rohit earnings..."):
    try:
        rohit_df = fetch_rohit_earnings(partner_id)
    except Exception as e:
        rohit_df = None
        st.error(f"Could not load Rohit earnings: {e}")

if rohit_df is not None and len(rohit_df) > 0:
    rohit_df = rohit_df.copy()
    rohit_df['month_label'] = pd.to_datetime(rohit_df['MONTH']).dt.strftime('%b %Y')
    rohit_df = rohit_df.sort_values('MONTH')

    latest = rohit_df.iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active Rohits (latest month)", safe_int(latest.get('ROHIT_COUNT')))
    c2.metric("Claimed (latest month)", f"₹{safe_float(latest.get('CLAIMED')):,.0f}")
    c3.metric("Missed (latest month)",  f"₹{safe_float(latest.get('MISSED')):,.0f}")
    c4.metric("Total (latest month)",   f"₹{safe_float(latest.get('TOTAL_EARNED')):,.0f}")

    fig_rohit = go.Figure()
    fig_rohit.add_trace(go.Bar(name='Claimed',   x=rohit_df['month_label'], y=rohit_df['CLAIMED'].astype(float),   marker_color='#00CC96'))
    fig_rohit.add_trace(go.Bar(name='Unclaimed', x=rohit_df['month_label'], y=rohit_df['UNCLAIMED'].astype(float), marker_color='#636EFA'))
    fig_rohit.add_trace(go.Bar(name='Promised',  x=rohit_df['month_label'], y=rohit_df['PROMISED'].astype(float),  marker_color='#FFA15A'))
    fig_rohit.add_trace(go.Bar(name='Missed',    x=rohit_df['month_label'], y=rohit_df['MISSED'].astype(float),    marker_color='#EF553B'))
    fig_rohit.add_trace(go.Bar(name='Expired',   x=rohit_df['month_label'], y=rohit_df['EXPIRED'].astype(float),   marker_color='#BEBEBE'))
    fig_rohit.update_layout(
        barmode='stack', title="Rohit Incentives by Month (₹)",
        yaxis_title="₹", plot_bgcolor='rgba(0,0,0,0)',
        height=320, margin=dict(t=40, b=20)
    )
    st.plotly_chart(fig_rohit, use_container_width=True)

    disp_rohit = rohit_df[['month_label', 'ROHIT_COUNT', 'CLAIMED', 'UNCLAIMED', 'PROMISED', 'MISSED', 'EXPIRED', 'TOTAL_EARNED']].copy()
    for col in ['CLAIMED', 'UNCLAIMED', 'PROMISED', 'MISSED', 'EXPIRED', 'TOTAL_EARNED']:
        disp_rohit[col] = disp_rohit[col].apply(lambda x: f"₹{safe_float(x):,.0f}")
    disp_rohit.columns = ['Month', 'Rohits', 'Claimed', 'Unclaimed', 'Promised', 'Missed', 'Expired', 'Total Earned']
    st.dataframe(disp_rohit, use_container_width=True, hide_index=True)
else:
    st.info("No Rohit earning data found for this partner in the selected period.")

# ── Raw Data ──────────────────────────────────────────────────────────────────
with st.expander("📋 View All Partner Fields"):
    raw = pd.DataFrame([dict(row)]).T.rename(columns={0: 'Value'})
    st.dataframe(raw, use_container_width=True)

st.caption(f"Source: Metabase card 7876 (Partner Wise Earnings View) · Partner ID: {partner_id}")
