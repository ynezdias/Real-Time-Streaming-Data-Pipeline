"""
app.py — StreamPulse: Real-Time Streaming Analytics Dashboard

A production-grade Streamlit dashboard backed by PostgreSQL.
Visualises live pipeline output: active users, event distribution,
top countries, purchase trends, and a live activity feed.

Run with:
    streamlit run dashboard/app.py
"""

import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── must be first Streamlit call ──────────────────────────────────────────
st.set_page_config(
    page_title="StreamPulse — Real-Time Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

from queries import (
    get_event_type_distribution,
    get_events_over_time,
    get_filter_options,
    get_kpi_summary,
    get_live_feed,
    get_purchase_trends,
    get_top_countries,
)

# ── Design tokens ─────────────────────────────────────────────────────────
ACCENT      = "#6C63FF"   # electric violet
ACCENT2     = "#00D4AA"   # teal
ACCENT3     = "#FF6B6B"   # coral
ACCENT4     = "#FFD93D"   # amber
BG_CARD     = "#1A1D2E"
BG_PAGE     = "#0F1120"
TEXT_MUTED  = "#8B8FA8"

PALETTE = [ACCENT, ACCENT2, ACCENT3, ACCENT4, "#FF9F43", "#54A0FF"]

# ── Custom CSS ────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
/* ── Base ── */
[data-testid="stAppViewContainer"] {{
    background: {BG_PAGE};
}}
[data-testid="stSidebar"] {{
    background: {BG_CARD};
    border-right: 1px solid #252840;
}}
[data-testid="stSidebar"] * {{
    color: #E2E4F0 !important;
}}

/* ── Hide default Streamlit chrome ── */
#MainMenu, footer, header {{ visibility: hidden; }}

/* ── Top header bar ── */
.header-bar {{
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 16px 6px 8px;
    margin-bottom: 4px;
    border-bottom: 1px solid #252840;
}}
.header-logo {{
    font-size: 28px;
    line-height: 1;
}}
.header-title {{
    font-size: 22px;
    font-weight: 700;
    color: #FFFFFF;
    letter-spacing: -0.3px;
}}
.header-sub {{
    font-size: 12px;
    color: {TEXT_MUTED};
    margin-top: 2px;
}}
.pill {{
    display: inline-block;
    background: rgba(108,99,255,0.18);
    color: {ACCENT};
    border: 1px solid rgba(108,99,255,0.35);
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}}

/* ── KPI cards ── */
.kpi-card {{
    background: {BG_CARD};
    border: 1px solid #252840;
    border-radius: 14px;
    padding: 18px 22px;
    position: relative;
    overflow: hidden;
}}
.kpi-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, {ACCENT}, {ACCENT2});
    border-radius: 14px 14px 0 0;
}}
.kpi-label {{
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: {TEXT_MUTED};
    margin-bottom: 6px;
}}
.kpi-value {{
    font-size: 32px;
    font-weight: 800;
    color: #FFFFFF;
    line-height: 1;
    letter-spacing: -0.5px;
}}
.kpi-icon {{
    position: absolute;
    top: 16px; right: 18px;
    font-size: 26px;
    opacity: 0.25;
}}

/* ── Chart containers ── */
.chart-card {{
    background: {BG_CARD};
    border: 1px solid #252840;
    border-radius: 14px;
    padding: 18px 20px 8px;
    margin-bottom: 16px;
}}
.chart-title {{
    font-size: 14px;
    font-weight: 600;
    color: #FFFFFF;
    margin-bottom: 4px;
}}
.chart-sub {{
    font-size: 11px;
    color: {TEXT_MUTED};
    margin-bottom: 12px;
}}

/* ── Live feed table ── */
.feed-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 0;
    border-bottom: 1px solid #252840;
    font-size: 12px;
    color: #C5C8E0;
}}
.feed-badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 10px;
    font-weight: 600;
}}

/* ── Sidebar inputs ── */
div[data-testid="stSelectbox"] label,
div[data-testid="stSlider"] label {{
    font-size: 11px !important;
    font-weight: 600 !important;
    letter-spacing: 0.8px !important;
    text-transform: uppercase !important;
    color: {TEXT_MUTED} !important;
}}

/* ── Plotly chart background override ── */
.js-plotly-plot .plotly .main-svg {{
    background: transparent !important;
}}

/* ── No-data notice ── */
.empty-state {{
    text-align: center;
    padding: 40px 20px;
    color: {TEXT_MUTED};
    font-size: 13px;
}}
.empty-state .icon {{
    font-size: 36px;
    margin-bottom: 8px;
}}

/* ── Status dot ── */
.status-live {{
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    background: {ACCENT2};
    box-shadow: 0 0 6px {ACCENT2};
    animation: pulse 2s infinite;
    margin-right: 5px;
}}
@keyframes pulse {{
    0%   {{ opacity: 1; }}
    50%  {{ opacity: 0.4; }}
    100% {{ opacity: 1; }}
}}

/* ── Streamlit metric override ── */
[data-testid="metric-container"] {{
    display: none;
}}
</style>
""", unsafe_allow_html=True)

# ── Plotly base theme ─────────────────────────────────────────────────────
CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color="#C5C8E0", size=11),
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis=dict(
        gridcolor="#1F2235",
        zerolinecolor="#1F2235",
        tickfont=dict(size=10),
    ),
    yaxis=dict(
        gridcolor="#1F2235",
        zerolinecolor="#1F2235",
        tickfont=dict(size=10),
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(size=10),
    ),
    hoverlabel=dict(
        bgcolor=BG_CARD,
        bordercolor="#252840",
        font=dict(color="#E2E4F0", size=11),
    ),
)


# ── Helpers ───────────────────────────────────────────────────────────────
def fmt_number(n, prefix="", suffix="") -> str:
    if n is None:
        return "—"
    if n >= 1_000_000:
        return f"{prefix}{n/1_000_000:.1f}M{suffix}"
    if n >= 1_000:
        return f"{prefix}{n/1_000:.1f}K{suffix}"
    return f"{prefix}{n:,.0f}{suffix}"


def empty_state(msg="No data for this selection"):
    st.markdown(
        f'<div class="empty-state"><div class="icon">📭</div>{msg}</div>',
        unsafe_allow_html=True,
    )


def badge_color(event_type: str) -> str:
    colours = {
        "page_view":   "rgba(108,99,255,0.3)",
        "search":      "rgba(0,212,170,0.3)",
        "add_to_cart": "rgba(255,159,67,0.3)",
        "purchase":    "rgba(255,107,107,0.3)",
        "logout":      "rgba(84,160,255,0.3)",
    }
    return colours.get(event_type, "rgba(139,143,168,0.3)")


def badge_text_color(event_type: str) -> str:
    colours = {
        "page_view":   "#9B96FF",
        "search":      "#00D4AA",
        "add_to_cart": "#FFB347",
        "purchase":    "#FF8A8A",
        "logout":      "#80BAFF",
    }
    return colours.get(event_type, TEXT_MUTED)


# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style="text-align:center; padding: 10px 0 20px;">
            <div style="font-size:38px;">⚡</div>
            <div style="font-size:18px; font-weight:800; color:#FFF;
                        letter-spacing:-0.3px;">StreamPulse</div>
            <div style="font-size:11px; color:#8B8FA8; margin-top:4px;">
                Real-Time Analytics
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.divider()

    # Load filter options
    with st.spinner("Loading filters…"):
        event_types, countries = get_filter_options()

    st.markdown('<p style="font-size:11px;font-weight:600;letter-spacing:0.8px;'
                'text-transform:uppercase;color:#8B8FA8;margin-bottom:6px;">'
                'TIME WINDOW</p>', unsafe_allow_html=True)
    time_window = st.selectbox(
        "Time window",
        ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Last 30 Days"],
        index=2,
        label_visibility="collapsed",
    )

    st.markdown('<p style="font-size:11px;font-weight:600;letter-spacing:0.8px;'
                'text-transform:uppercase;color:#8B8FA8;margin-bottom:6px;margin-top:12px;">'
                'EVENT TYPE</p>', unsafe_allow_html=True)
    selected_event = st.selectbox(
        "Event type", event_types, label_visibility="collapsed"
    )

    st.markdown('<p style="font-size:11px;font-weight:600;letter-spacing:0.8px;'
                'text-transform:uppercase;color:#8B8FA8;margin-bottom:6px;margin-top:12px;">'
                'COUNTRY</p>', unsafe_allow_html=True)
    selected_country = st.selectbox(
        "Country", countries, label_visibility="collapsed"
    )

    st.markdown('<p style="font-size:11px;font-weight:600;letter-spacing:0.8px;'
                'text-transform:uppercase;color:#8B8FA8;margin-bottom:6px;margin-top:12px;">'
                'TOP COUNTRIES</p>', unsafe_allow_html=True)
    top_n = st.slider("Top N countries", 3, 20, 10, label_visibility="collapsed")

    st.divider()

    st.markdown('<p style="font-size:11px;font-weight:600;letter-spacing:0.8px;'
                'text-transform:uppercase;color:#8B8FA8;margin-bottom:6px;">'
                'AUTO-REFRESH (seconds)</p>', unsafe_allow_html=True)
    refresh_rate = st.slider("Refresh", 10, 120, 30, step=5, label_visibility="collapsed")

    refresh_toggle = st.toggle("Enable auto-refresh", value=True)

    st.divider()
    st.markdown(
        f'<div style="font-size:10px; color:#8B8FA8; text-align:center;">'
        f'Last updated<br>'
        f'<span style="color:#E2E4F0; font-weight:600;">'
        f'{datetime.utcnow().strftime("%H:%M:%S")} UTC</span></div>',
        unsafe_allow_html=True,
    )

# ── Page header ───────────────────────────────────────────────────────────
st.markdown(f"""
<div class="header-bar">
    <div class="header-logo">⚡</div>
    <div>
        <div class="header-title">StreamPulse Analytics</div>
        <div class="header-sub">
            <span class="status-live"></span>
            Live · {time_window} · {selected_event} · {selected_country}
        </div>
    </div>
    <div style="margin-left:auto;">
        <span class="pill">🔴 Live</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Fetch all data ────────────────────────────────────────────────────────
with st.spinner("Fetching data…"):
    kpi_df    = get_kpi_summary(time_window, selected_event, selected_country)
    ts_df     = get_events_over_time(time_window, selected_event, selected_country)
    dist_df   = get_event_type_distribution(time_window, selected_country)
    ctry_df   = get_top_countries(time_window, selected_event, top_n)
    purch_df  = get_purchase_trends(time_window, selected_country)
    feed_df   = get_live_feed(20)

# ── KPI Row ───────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)

def kpi_card(col, label, value, icon):
    col.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-icon">{icon}</div>
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

if not kpi_df.empty and kpi_df.iloc[0]["total_events"] > 0:
    row = kpi_df.iloc[0]
    kpi_card(k1, "Total Events",       fmt_number(row["total_events"]),          "📊")
    kpi_card(k2, "Unique Users",        fmt_number(row["total_unique_users"]),    "👥")
    kpi_card(k3, "Revenue Generated",   fmt_number(row["total_revenue"], "$"),    "💰")
    kpi_card(k4, "Active Countries",    fmt_number(row["active_countries"]),      "🌍")
else:
    for col, label, icon in [
        (k1, "Total Events",     "📊"),
        (k2, "Unique Users",     "👥"),
        (k3, "Revenue",          "💰"),
        (k4, "Active Countries", "🌍"),
    ]:
        kpi_card(col, label, "—", icon)

st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

# ── Row 2: Time-series + Event distribution ───────────────────────────────
col_ts, col_dist = st.columns([3, 2], gap="medium")

with col_ts:
    st.markdown(
        '<div class="chart-card">'
        '<div class="chart-title">Activity Over Time</div>'
        '<div class="chart-sub">Events and unique users per time bucket</div>',
        unsafe_allow_html=True,
    )
    if ts_df.empty or len(ts_df) < 2:
        empty_state("No time-series data for the selected filters.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=ts_df["bucket"], y=ts_df["event_count"],
            name="Events",
            mode="lines",
            line=dict(color=ACCENT, width=2.5),
            fill="tozeroy",
            fillcolor="rgba(108,99,255,0.08)",
            hovertemplate="%{x|%b %d %H:%M}<br><b>%{y:,} events</b><extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=ts_df["bucket"], y=ts_df["unique_users"],
            name="Unique Users",
            mode="lines",
            line=dict(color=ACCENT2, width=2, dash="dot"),
            hovertemplate="%{x|%b %d %H:%M}<br><b>%{y:,} users</b><extra></extra>",
        ))
        fig.update_layout(height=260, **CHART_LAYOUT)
        fig.update_xaxes(tickformat="%H:%M" if "Hour" in time_window else "%b %d")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

with col_dist:
    st.markdown(
        '<div class="chart-card">'
        '<div class="chart-title">Event Distribution</div>'
        '<div class="chart-sub">Share by event type</div>',
        unsafe_allow_html=True,
    )
    if dist_df.empty:
        empty_state()
    else:
        fig = px.pie(
            dist_df,
            names="event_type",
            values="event_count",
            color_discrete_sequence=PALETTE,
            hole=0.55,
        )
        fig.update_traces(
            textposition="outside",
            textfont_size=10,
            hovertemplate="<b>%{label}</b><br>%{value:,} events<br>%{percent}<extra></extra>",
        )
        fig.update_layout(
            height=260,
            showlegend=True,
            legend=dict(
                orientation="v",
                x=1.0, y=0.5,
                font=dict(size=10),
            ),
            **{k: v for k, v in CHART_LAYOUT.items() if k != "legend"},
        )
        # Centre annotation
        fig.add_annotation(
            text=f"<b>{fmt_number(dist_df['event_count'].sum())}</b><br><span style='font-size:9px'>events</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=13, color="#FFFFFF"),
            xref="paper", yref="paper",
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

# ── Row 3: Top countries + Purchase revenue ───────────────────────────────
col_ctry, col_rev = st.columns([2, 3], gap="medium")

with col_ctry:
    st.markdown(
        '<div class="chart-card">'
        f'<div class="chart-title">Top {top_n} Countries</div>'
        '<div class="chart-sub">By total event volume</div>',
        unsafe_allow_html=True,
    )
    if ctry_df.empty:
        empty_state()
    else:
        fig = px.bar(
            ctry_df.sort_values("event_count"),
            x="event_count",
            y="country",
            orientation="h",
            color="event_count",
            color_continuous_scale=[[0, "#1A1D2E"], [0.4, ACCENT], [1, ACCENT2]],
            text="event_count",
        )
        fig.update_traces(
            texttemplate="%{text:,}",
            textposition="outside",
            textfont=dict(size=9, color="#C5C8E0"),
            hovertemplate="<b>%{y}</b><br>%{x:,} events<extra></extra>",
            marker_line_width=0,
        )
        fig.update_layout(
            height=320,
            coloraxis_showscale=False,
            **CHART_LAYOUT,
        )
        fig.update_xaxes(title=None)
        fig.update_yaxes(title=None)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

with col_rev:
    st.markdown(
        '<div class="chart-card">'
        '<div class="chart-title">Purchase Revenue Trend</div>'
        '<div class="chart-sub">Hourly revenue from purchase events</div>',
        unsafe_allow_html=True,
    )
    if purch_df.empty or len(purch_df) < 2:
        empty_state("No purchase data in this window.")
    else:
        fig = go.Figure()
        # Revenue area
        fig.add_trace(go.Scatter(
            x=purch_df["hour"], y=purch_df["revenue"],
            name="Revenue ($)",
            mode="lines",
            line=dict(color=ACCENT3, width=2.5),
            fill="tozeroy",
            fillcolor="rgba(255,107,107,0.08)",
            hovertemplate="%{x|%b %d %H:%M}<br><b>$%{y:,.0f}</b><extra></extra>",
        ))
        # Purchase count on secondary axis
        fig.add_trace(go.Bar(
            x=purch_df["hour"], y=purch_df["purchases"],
            name="Purchases",
            marker_color="rgba(108,99,255,0.3)",
            yaxis="y2",
            hovertemplate="%{x|%b %d %H:%M}<br><b>%{y:,} purchases</b><extra></extra>",
        ))
        fig.update_layout(
            height=320,
            yaxis=dict(title="Revenue ($)", titlefont=dict(size=10), gridcolor="#1F2235"),
            yaxis2=dict(
                title="Purchase Count",
                titlefont=dict(size=10),
                overlaying="y",
                side="right",
                gridcolor="rgba(0,0,0,0)",
            ),
            **{k: v for k, v in CHART_LAYOUT.items() if k not in ("yaxis",)},
        )
        fig.update_xaxes(tickformat="%H:%M" if "Hour" in time_window else "%b %d")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

# ── Row 4: Event-type revenue breakdown ──────────────────────────────────
if not dist_df.empty and "revenue" in dist_df.columns:
    st.markdown(
        '<div class="chart-card">'
        '<div class="chart-title">Revenue by Event Type & Country</div>'
        '<div class="chart-sub">Breakdown of where revenue is generated</div>',
        unsafe_allow_html=True,
    )
    rev_df = dist_df[dist_df["revenue"] > 0]
    if rev_df.empty:
        empty_state("No revenue data for this selection.")
    else:
        # Horizontal bar with gradient
        fig = px.bar(
            rev_df,
            x="revenue", y="event_type",
            orientation="h",
            color="event_type",
            color_discrete_sequence=PALETTE,
            text="revenue",
        )
        fig.update_traces(
            texttemplate="$%{text:,.0f}",
            textposition="outside",
            textfont=dict(size=10, color="#C5C8E0"),
            hovertemplate="<b>%{y}</b><br>$%{x:,.2f}<extra></extra>",
            marker_line_width=0,
        )
        fig.update_layout(height=180, showlegend=False, **CHART_LAYOUT)
        fig.update_xaxes(title=None, tickprefix="$")
        fig.update_yaxes(title=None)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

# ── Row 5: Live activity feed ─────────────────────────────────────────────
st.markdown(
    '<div class="chart-card">'
    '<div class="chart-title">🔴 Live Activity Feed</div>'
    '<div class="chart-sub">Most recent aggregation windows from the pipeline</div>',
    unsafe_allow_html=True,
)
if feed_df.empty:
    empty_state("No live events yet. Make sure the pipeline is running.")
else:
    # Render each row as styled HTML
    rows_html = ""
    for _, r in feed_df.iterrows():
        ts = pd.to_datetime(r["window_start"]).strftime("%H:%M:%S")
        bg   = badge_color(r["event_type"])
        tc   = badge_text_color(r["event_type"])
        rev  = f'&nbsp;·&nbsp; <span style="color:{ACCENT4}">${r["total_revenue"]:,.0f}</span>' \
               if r["total_revenue"] > 0 else ""
        rows_html += f"""
        <div class="feed-row">
            <span style="color:{TEXT_MUTED};width:70px;flex-shrink:0;">{ts}</span>
            <span class="feed-badge" style="background:{bg};color:{tc};margin:0 8px;">
                {r['event_type']}
            </span>
            <span style="flex:1;">{r['country']}</span>
            <span style="color:#C5C8E0;">{int(r['event_count']):,} events
                &nbsp;·&nbsp; {int(r['unique_users']):,} users{rev}</span>
        </div>
        """
    st.markdown(rows_html, unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="text-align:center;padding:24px 0 8px;color:{TEXT_MUTED};font-size:11px;">
    StreamPulse &nbsp;·&nbsp; Real-Time Streaming Data Pipeline
    &nbsp;·&nbsp; Powered by Kafka · PySpark · PostgreSQL
    &nbsp;·&nbsp; <span style="color:{ACCENT2};">Auto-refreshes every {refresh_rate}s</span>
</div>
""", unsafe_allow_html=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────
if refresh_toggle:
    time.sleep(refresh_rate)
    st.rerun()
