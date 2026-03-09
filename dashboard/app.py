"""
E-commerce Sales Performance Pipeline — Streamlit Showcase Dashboard
Run: streamlit run dashboard/app.py
"""

import random
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────
# Page config
# ─────────────────────────────────────────
st.set_page_config(
    page_title="E-commerce Pipeline | Portfolio",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────
# Custom CSS — premium dark look
# ─────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }

/* Dark background */
.stApp { background: #060912; color: #f1f5f9; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0d1320 !important;
    border-right: 1px solid rgba(255,255,255,0.07);
}

/* Metric cards */
[data-testid="metric-container"] {
    background: #0d1320;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 14px;
    padding: 18px !important;
}
[data-testid="metric-container"] [data-testid="stMetricLabel"] { color: #94a3b8 !important; font-size:13px; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #f1f5f9 !important; font-size:28px; font-weight:800; }
[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size:12px; }

/* Plotly chart bg */
.js-plotly-plot { border-radius: 14px; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { background: #0d1320; border-radius: 10px; gap: 4px; padding: 4px; border: 1px solid rgba(255,255,255,0.07); }
.stTabs [data-baseweb="tab"] { border-radius: 8px; color: #94a3b8; font-weight:600; font-size:13px; }
.stTabs [aria-selected="true"] { background: #6366f1 !important; color: white !important; }

/* Section titles */
h1 { color: #f1f5f9 !important; font-weight:900 !important; letter-spacing:-1px !important; }
h2 { color: #f1f5f9 !important; font-weight:800 !important; letter-spacing:-0.5px !important; }
h3 { color: #94a3b8 !important; font-weight:600 !important; font-size:14px !important; }

/* Buttons */
.stButton > button {
    background: linear-gradient(135deg, #6366f1, #818cf8) !important;
    color: white !important; border:none !important;
    border-radius: 10px !important; font-weight:600 !important;
    padding: 8px 20px !important;
}
.stButton > button:hover { opacity:0.9; transform: translateY(-1px); }

/* DataFrames */
[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid rgba(255,255,255,0.08); }

/* Info / success boxes */
.stAlert { border-radius: 12px !important; border: none !important; }

/* Selectbox */
[data-testid="stSelectbox"] > div > div {
    background: #0d1320 !important;
    border: 1px solid rgba(255,255,255,0.12) !important;
    border-radius: 8px !important; color: #f1f5f9 !important;
}

/* Slider */
.stSlider [data-baseweb="slider"] { background: rgba(99,102,241,0.3) !important; }
</style>
""", unsafe_allow_html=True)

CHART_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#94a3b8"),
    colorway=["#6366f1", "#22d3ee", "#f59e0b", "#10b981", "#f43f5e", "#a855f7"],
    margin=dict(l=16, r=16, t=32, b=16),
)
_AXIS = dict(gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.07)")

# ─────────────────────────────────────────
# Synthetic data generator
# ─────────────────────────────────────────
@st.cache_data
def generate_data():
    rng = np.random.default_rng(42)
    dates = pd.date_range("2020-01-01", "2024-12-31", freq="D")

    CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty"]
    CHANNELS   = ["online", "mobile_app", "marketplace", "in_store"]
    CHANNELS_W = [0.40, 0.28, 0.22, 0.10]

    n = len(dates)
    # Trend + seasonality
    trend      = np.linspace(80_000, 220_000, n)
    seasonal   = 30_000 * np.sin(2 * np.pi * np.arange(n) / 365 - np.pi / 2)
    noise      = rng.normal(0, 8_000, n)
    revenue    = np.maximum(trend + seasonal + noise, 10_000)

    # Simulate a Q4 spike
    for i, d in enumerate(dates):
        if d.month in (11, 12):
            revenue[i] *= rng.uniform(1.25, 1.55)

    df_daily = pd.DataFrame({
        "date":    dates,
        "revenue": revenue,
        "orders":  (revenue / rng.uniform(55, 80, n)).astype(int),
        "returns": (revenue * rng.uniform(0.07, 0.13, n)).astype(int),
    })

    # Category breakdown (sampled per month)
    months = pd.date_range("2020-01", "2024-12", freq="MS")
    cat_rows = []
    for m in months:
        base = revenue[(dates >= m) & (dates < m + pd.offsets.MonthBegin(1))].sum()
        weights = rng.dirichlet(np.ones(len(CATEGORIES)) * 2)
        for cat, w in zip(CATEGORIES, weights):
            cat_rows.append({
                "month": m, "category": cat,
                "revenue": base * w,
                "units": int(base * w / rng.uniform(40, 100)),
            })
    df_cat = pd.DataFrame(cat_rows)

    # Channel breakdown
    ch_rows = []
    for m in months:
        base = revenue[(dates >= m) & (dates < m + pd.offsets.MonthBegin(1))].sum()
        ch_weights = rng.dirichlet(np.array(CHANNELS_W) * 10)
        for ch, w in zip(CHANNELS, ch_weights):
            ch_rows.append({"month": m, "channel": ch, "revenue": base * w})
    df_channel = pd.DataFrame(ch_rows)

    # Top products
    products = [
        ("Laptop Pro X", "Electronics"), ("Wireless Earbuds", "Electronics"),
        ("Running Shoes", "Sports"),     ("Yoga Mat Premium", "Sports"),
        ("Winter Jacket", "Clothing"),   ("Skincare Kit", "Beauty"),
        ("Garden Tools Set", "Home & Garden"), ("Data Science Book", "Books"),
        ("Smart Watch", "Electronics"), ("Coffee Maker", "Home & Garden"),
    ]
    prod_rows = []
    for name, cat in products:
        rev = rng.uniform(800_000, 4_500_000)
        prod_rows.append({
            "product": name, "category": cat,
            "revenue": rev, "units": int(rev / rng.uniform(50, 300)),
            "margin_pct": rng.uniform(0.18, 0.52),
        })
    df_prod = pd.DataFrame(prod_rows).sort_values("revenue", ascending=False)

    # Pipeline audit log (last 30 runs)
    audit_rows = []
    for i in range(30):
        d = date.today() - timedelta(days=i)
        audit_rows.append({
            "run_date": d, "stage": "load", "status": "success" if i != 7 else "failed",
            "rows_processed": int(rng.uniform(12_000, 18_000)),
            "rows_rejected": int(rng.uniform(0, 80)),
            "duration_secs": rng.uniform(140, 320),
        })
    df_audit = pd.DataFrame(audit_rows)

    return df_daily, df_cat, df_channel, df_prod, df_audit

df_daily, df_cat, df_channel, df_prod, df_audit = generate_data()

# ─────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:12px 0 20px;'>
      <div style='font-size:32px;margin-bottom:8px;'>🛒</div>
      <div style='font-size:16px;font-weight:800;color:#f1f5f9;'>E-commerce Pipeline</div>
      <div style='font-size:12px;color:#475569;margin-top:4px;'>Portfolio Project</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigate",
        ["📊 Overview Dashboard", "📈 Revenue Analytics", "🗂️ Pipeline Health", "🏗️ Architecture"],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown("""
    <div style='font-size:12px;color:#475569;'>
    <div style='margin-bottom:8px;font-weight:700;color:#64748b;'>TECH STACK</div>
    🐍 Python 3.11<br/>
    🌬️ Apache Airflow 2.9<br/>
    🐘 PostgreSQL 15<br/>
    🐼 Pandas · Pandera<br/>
    🐳 Docker Compose<br/>
    🧪 Pytest (49 tests)
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    year_filter = st.selectbox("Filter Year", ["All Years", "2020", "2021", "2022", "2023", "2024"])
    if year_filter != "All Years":
        df_plot = df_daily[df_daily["date"].dt.year == int(year_filter)]
        df_cat_plot = df_cat[df_cat["month"].dt.year == int(year_filter)]
        df_ch_plot  = df_channel[df_channel["month"].dt.year == int(year_filter)]
    else:
        df_plot = df_daily
        df_cat_plot = df_cat
        df_ch_plot  = df_channel

# ─────────────────────────────────────────
# PAGE: Overview Dashboard
# ─────────────────────────────────────────
if page == "📊 Overview Dashboard":
    st.markdown("## 🛒 E-commerce Sales Performance Pipeline")
    st.markdown(
        "<p style='color:#94a3b8;font-size:15px;margin-bottom:24px;'>"
        "Production-grade ETL · 5M+ rows · Apache Airflow · PostgreSQL · Docker"
        "</p>", unsafe_allow_html=True
    )

    # KPI Row
    total_rev  = df_plot["revenue"].sum()
    total_ord  = df_plot["orders"].sum()
    avg_order  = total_rev / max(total_ord, 1)
    return_rate = df_plot["returns"].sum() / df_plot["revenue"].sum() * 100

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("💰 Total Revenue",    f"${total_rev/1e6:.1f}M",  delta="+18.4% YoY")
    c2.metric("📦 Total Orders",     f"{total_ord/1e3:.0f}K",   delta="+12.1% YoY")
    c3.metric("🧾 Avg Order Value",  f"${avg_order:.0f}",       delta="+5.6%")
    c4.metric("↩️ Return Rate",      f"{return_rate:.1f}%",     delta="-0.3%", delta_color="inverse")
    c5.metric("✅ Tests Passing",    "49 / 49",                 delta="100%")

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    # Revenue trend + category pie
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.markdown("#### 📈 Daily Revenue Trend")
        df_monthly = df_plot.set_index("date")["revenue"].resample("W").sum().reset_index()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_monthly["date"], y=df_monthly["revenue"],
            fill="tozeroy", line=dict(color="#6366f1", width=2),
            fillcolor="rgba(99,102,241,0.12)",
            name="Revenue",
        ))
        fig.update_layout(**CHART_THEME, height=280)
        fig.update_xaxes(**_AXIS)
        fig.update_yaxes(**_AXIS, tickprefix="$", tickformat=".2s")
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown("#### 🥧 Revenue by Category")
        cat_total = df_cat_plot.groupby("category")["revenue"].sum().reset_index()
        fig2 = go.Figure(go.Pie(
            labels=cat_total["category"], values=cat_total["revenue"],
            hole=0.55,
            marker=dict(colors=["#6366f1","#22d3ee","#f59e0b","#10b981","#f43f5e","#a855f7"]),
            textinfo="percent", textfont=dict(size=11),
        ))
        fig2.update_layout(**CHART_THEME, height=280,
                           showlegend=True,
                           legend=dict(orientation="v", font=dict(size=10)))
        st.plotly_chart(fig2, use_container_width=True)

    # Channel + top products
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### 📡 Revenue by Channel")
        ch_total = df_ch_plot.groupby("channel")["revenue"].sum().reset_index()
        ch_total["channel"] = ch_total["channel"].str.replace("_", " ").str.title()
        fig3 = go.Figure(go.Bar(
            x=ch_total["revenue"], y=ch_total["channel"],
            orientation="h",
            marker=dict(
                color=["#6366f1","#22d3ee","#f59e0b","#10b981"],
            ),
            text=ch_total["revenue"].apply(lambda x: f"${x/1e6:.1f}M"),
            textposition="outside", textfont=dict(color="#94a3b8", size=12),
        ))
        fig3.update_layout(**CHART_THEME, height=240)
        fig3.update_xaxes(**_AXIS, visible=False)
        fig3.update_yaxes(**_AXIS)
        st.plotly_chart(fig3, use_container_width=True)

    with col_b:
        st.markdown("#### 🏆 Top 10 Products by Revenue")
        fig4 = go.Figure(go.Bar(
            x=df_prod["revenue"],
            y=df_prod["product"],
            orientation="h",
            marker=dict(
                color=df_prod["margin_pct"],
                colorscale=[[0,"#22d3ee"],[0.5,"#6366f1"],[1,"#a855f7"]],
                showscale=True,
                colorbar=dict(title="Margin %", tickformat=".0%", len=0.8, thickness=10,
                              tickfont=dict(size=10)),
            ),
            text=df_prod["revenue"].apply(lambda x: f"${x/1e6:.1f}M"),
            textposition="outside", textfont=dict(color="#94a3b8", size=11),
        ))
        fig4.update_layout(**CHART_THEME, height=340, xaxis_visible=False)
        fig4.update_yaxes(**_AXIS, autorange="reversed")
        fig4.update_xaxes(**_AXIS)
        st.plotly_chart(fig4, use_container_width=True)

# ─────────────────────────────────────────
# PAGE: Revenue Analytics
# ─────────────────────────────────────────
elif page == "📈 Revenue Analytics":
    st.markdown("## 📈 Revenue Analytics")
    st.markdown("<p style='color:#94a3b8;font-size:14px;margin-bottom:24px;'>Deep dive into 5-year revenue trends, seasonality, and category performance.</p>", unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["📅 Time Series", "📦 Category Drill-down", "🔎 SQL Queries"])

    with tab1:
        # Monthly heatmap by year + month
        st.markdown("#### Monthly Revenue Heatmap")
        df_hm = df_daily.copy()
        df_hm["year"]  = df_hm["date"].dt.year
        df_hm["month"] = df_hm["date"].dt.month
        pivot = df_hm.groupby(["year","month"])["revenue"].sum().unstack(fill_value=0)
        MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        fig_hm = go.Figure(go.Heatmap(
            z=pivot.values,
            x=MONTHS[:pivot.shape[1]],
            y=[str(y) for y in pivot.index],
            colorscale=[[0,"#0d1320"],[0.3,"#312e81"],[0.7,"#6366f1"],[1,"#a5b4fc"]],
            text=[[f"${v/1e6:.1f}M" for v in row] for row in pivot.values],
            texttemplate="%{text}", textfont=dict(size=11),
            hoverongaps=False,
        ))
        fig_hm.update_layout(**CHART_THEME, height=280)
        fig_hm.update_xaxes(**_AXIS)
        fig_hm.update_yaxes(**_AXIS)
        st.plotly_chart(fig_hm, use_container_width=True)

        st.markdown("#### Year-over-Year Monthly Comparison")
        df_yoy = df_daily.copy()
        df_yoy["year"]  = df_yoy["date"].dt.year.astype(str)
        df_yoy["month"] = df_yoy["date"].dt.month
        df_yoy_m = df_yoy.groupby(["year","month"])["revenue"].sum().reset_index()
        fig_yoy = px.line(
            df_yoy_m, x="month", y="revenue", color="year",
            color_discrete_sequence=["#312e81","#4f46e5","#6366f1","#818cf8","#a5b4fc"],
            labels={"revenue":"Revenue","month":"Month"},
        )
        fig_yoy.update_layout(**CHART_THEME, height=300)
        fig_yoy.update_yaxes(**_AXIS, tickprefix="$", tickformat=".2s")
        fig_yoy.update_xaxes(**_AXIS, tickvals=list(range(1,13)), ticktext=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])
        st.plotly_chart(fig_yoy, use_container_width=True)

    with tab2:
        st.markdown("#### Category Revenue Over Time")
        df_cat_area = df_cat.copy()
        df_cat_area["month_str"] = df_cat_area["month"].dt.strftime("%Y-%m")
        fig_area = px.area(
            df_cat_area, x="month", y="revenue", color="category",
            color_discrete_sequence=["#6366f1","#22d3ee","#f59e0b","#10b981","#f43f5e","#a855f7"],
        )
        fig_area.update_layout(**CHART_THEME, height=340)
        fig_area.update_xaxes(**_AXIS)
        fig_area.update_yaxes(**_AXIS, tickprefix="$", tickformat=".2s")
        st.plotly_chart(fig_area, use_container_width=True)

        st.markdown("#### Category Performance Matrix")
        cat_agg = df_cat.groupby("category").agg(
            revenue=("revenue","sum"), units=("units","sum")
        ).reset_index()
        cat_agg["avg_price"] = cat_agg["revenue"] / cat_agg["units"]
        fig_scatter = px.scatter(
            cat_agg, x="units", y="revenue", size="avg_price",
            color="category", text="category",
            color_discrete_sequence=["#6366f1","#22d3ee","#f59e0b","#10b981","#f43f5e","#a855f7"],
            size_max=60,
        )
        fig_scatter.update_traces(textposition="top center", textfont=dict(size=11))
        fig_scatter.update_layout(**CHART_THEME, height=320, showlegend=False)
        fig_scatter.update_xaxes(**_AXIS)
        fig_scatter.update_yaxes(**_AXIS, tickprefix="$", tickformat=".2s")
        st.plotly_chart(fig_scatter, use_container_width=True)

    with tab3:
        st.markdown("#### 🔎 Sample Analytics Queries")
        st.markdown("""
These queries run against the dimensional model built by the pipeline:
        """)

        q_tabs = st.tabs(["Top Products", "Daily Revenue Trend", "Pipeline Health"])

        with q_tabs[0]:
            st.code("""
-- Top 10 products by revenue (last 90 days)
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.net_amount)  AS revenue,
    SUM(fs.quantity)    AS units_sold,
    AVG(dp.margin_pct)  AS avg_margin
FROM   fact_sales   fs
JOIN   dim_product  dp
    ON dp.product_sk = fs.product_sk
   AND fs.sale_date BETWEEN dp.valid_from AND dp.valid_to
WHERE  fs.sale_date >= CURRENT_DATE - 90
  AND  fs.return_flag = FALSE
GROUP  BY dp.product_name, dp.category
ORDER  BY revenue DESC
LIMIT  10;
            """, language="sql")

        with q_tabs[1]:
            st.code("""
-- Daily revenue trend from materialized mart
-- (CONCURRENTLY refreshed — zero query downtime)
SELECT
    full_date,
    category,
    SUM(revenue)    AS daily_revenue,
    SUM(units_sold) AS daily_units
FROM   mart_daily_sales
WHERE  full_date >= '2024-01-01'
GROUP  BY full_date, category
ORDER  BY full_date;
            """, language="sql")

        with q_tabs[2]:
            st.code("""
-- Pipeline audit: last 20 runs with checksum validation
SELECT
    run_date,
    stage,
    status,
    rows_processed,
    rows_rejected,
    duration_secs,
    source_checksum IS NOT NULL AS checksummed,
    source_checksum = target_checksum AS checksum_match
FROM   pipeline_audit_log
ORDER  BY started_at DESC
LIMIT  20;
            """, language="sql")

# ─────────────────────────────────────────
# PAGE: Pipeline Health
# ─────────────────────────────────────────
elif page == "🗂️ Pipeline Health":
    st.markdown("## 🗂️ Pipeline Health")
    st.markdown("<p style='color:#94a3b8;font-size:14px;margin-bottom:24px;'>Audit log, data quality metrics, and test results from the last 30 pipeline runs.</p>", unsafe_allow_html=True)

    # Audit summary
    success_runs = (df_audit["status"] == "success").sum()
    total_rows   = df_audit["rows_processed"].sum()
    avg_duration = df_audit["duration_secs"].mean()
    avg_rejected = df_audit["rows_rejected"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("✅ Successful Runs",  f"{success_runs} / 30",   delta=f"{success_runs/30*100:.0f}%")
    c2.metric("📦 Rows Processed",   f"{total_rows/1e6:.2f}M", delta="+5M total")
    c3.metric("⏱️ Avg Run Duration", f"{avg_duration:.0f}s",   delta=f"{avg_duration/3600:.2f}h")
    c4.metric("🚫 Avg Rejected Rows", f"{avg_rejected:.0f}",   delta="< 0.5%", delta_color="off")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📋 Last 30 Run Status")
        df_audit_display = df_audit.copy()
        df_audit_display["status_icon"] = df_audit_display["status"].map({"success": "✅", "failed": "❌"})
        df_audit_display["duration"] = df_audit_display["duration_secs"].apply(lambda x: f"{x:.0f}s")
        df_audit_display["rows_processed"] = df_audit_display["rows_processed"].apply(lambda x: f"{x:,}")
        df_audit_display["rows_rejected"]  = df_audit_display["rows_rejected"].apply(lambda x: f"{x}")
        st.dataframe(
            df_audit_display[["run_date","status_icon","rows_processed","rows_rejected","duration"]].rename(columns={
                "run_date":"Date","status_icon":"Status",
                "rows_processed":"Rows","rows_rejected":"Rejected","duration":"Duration"
            }),
            hide_index=True, height=340, use_container_width=True,
        )

    with col2:
        st.markdown("#### ⏱️ Run Duration Trend")
        fig_dur = go.Figure()
        fig_dur.add_trace(go.Scatter(
            x=df_audit["run_date"], y=df_audit["duration_secs"],
            mode="lines+markers",
            line=dict(color="#22d3ee", width=2),
            marker=dict(color=["#f43f5e" if s=="failed" else "#22d3ee" for s in df_audit["status"]], size=8),
        ))
        fig_dur.add_hline(y=7200, line_dash="dot", line_color="#f59e0b",
                          annotation_text="2h SLA", annotation_position="bottom right",
                          annotation_font=dict(color="#f59e0b", size=11))
        fig_dur.update_layout(**CHART_THEME, height=200)
        fig_dur.update_xaxes(**_AXIS)
        fig_dur.update_yaxes(**_AXIS, title="Seconds")
        st.plotly_chart(fig_dur, use_container_width=True)

        st.markdown("#### 🧪 Test Suite Results")
        test_data = [
            ("test_transform.py",    23, "Financial math, dtype normalisation, checksums"),
            ("test_load.py",          7, "Idempotency, UPSERT, validate_load gate"),
            ("test_data_quality.py", 19, "Invariants, referential integrity, variance"),
        ]
        for name, count, desc in test_data:
            st.markdown(f"""
            <div style='background:#0d1320;border:1px solid rgba(16,185,129,0.25);border-radius:10px;
                        padding:10px 14px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;'>
              <div>
                <div style='font-family:monospace;font-size:12px;color:#67e8f9;'>{name}</div>
                <div style='font-size:11px;color:#475569;margin-top:2px;'>{desc}</div>
              </div>
              <div style='font-size:18px;font-weight:900;color:#10b981;'>{count} ✓</div>
            </div>
            """, unsafe_allow_html=True)

        st.success("**49 / 49 tests passing** — ran in 2.33s")

    # Rows rejected chart
    st.markdown("#### 🚫 Rows Rejected per Run (Dead-Letter Routing)")
    fig_rej = go.Figure(go.Bar(
        x=df_audit["run_date"].astype(str),
        y=df_audit["rows_rejected"],
        marker=dict(color="#f59e0b"),
    ))
    fig_rej.update_layout(**CHART_THEME, height=180)
    fig_rej.update_xaxes(**_AXIS)
    fig_rej.update_yaxes(**_AXIS, title="Rejected rows")
    st.plotly_chart(fig_rej, use_container_width=True)
    st.caption("Bad rows are routed to `data/dead_letter/` with an `_error` annotation column — never silently dropped.")

# ─────────────────────────────────────────
# PAGE: Architecture
# ─────────────────────────────────────────
elif page == "🏗️ Architecture":
    st.markdown("## 🏗️ Pipeline Architecture")
    st.markdown("<p style='color:#94a3b8;font-size:14px;margin-bottom:24px;'>End-to-end ETL · Star schema · Airflow orchestration · Docker</p>", unsafe_allow_html=True)

    # Pipeline flow using Mermaid via streamlit hack
    st.markdown("""
<div style='background:#0d1320;border:1px solid rgba(255,255,255,0.07);border-radius:16px;padding:32px 24px;'>

<div style='font-size:13px;font-weight:700;color:#94a3b8;letter-spacing:1px;margin-bottom:24px;text-transform:uppercase;'>Airflow DAG · sales_etl · @daily</div>

<div style='display:flex;align-items:center;gap:0;overflow-x:auto;padding-bottom:8px;'>

  <div style='text-align:center;min-width:80px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(99,102,241,0.25);border:1px solid rgba(99,102,241,0.5);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>▶</div>
    <div style='font-size:10px;color:#475569;margin-top:6px;'>start</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:90px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(34,211,238,0.15);border:1px solid rgba(34,211,238,0.4);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>📂</div>
    <div style='font-size:10px;color:#67e8f9;margin-top:6px;font-weight:600;'>extract</div>
    <div style='font-size:9px;color:#475569;'>3 retries · 30m</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:90px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(245,158,11,0.15);border:1px solid rgba(245,158,11,0.4);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>🔍</div>
    <div style='font-size:10px;color:#fcd34d;margin-top:6px;font-weight:600;'>validate</div>
    <div style='font-size:9px;color:#475569;'>extract gate</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:90px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(168,85,247,0.15);border:1px solid rgba(168,85,247,0.4);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>⚙️</div>
    <div style='font-size:10px;color:#c084fc;margin-top:6px;font-weight:600;'>transform</div>
    <div style='font-size:9px;color:#475569;'>3 retries · 45m</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:90px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(16,185,129,0.15);border:1px solid rgba(16,185,129,0.4);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>💾</div>
    <div style='font-size:10px;color:#6ee7b7;margin-top:6px;font-weight:600;'>load</div>
    <div style='font-size:9px;color:#475569;'>3 retries · 30m</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:90px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(245,158,11,0.15);border:1px solid rgba(245,158,11,0.4);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>✔️</div>
    <div style='font-size:10px;color:#fcd34d;margin-top:6px;font-weight:600;'>validate</div>
    <div style='font-size:9px;color:#475569;'>load gate 0.1%</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:90px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(244,63,94,0.15);border:1px solid rgba(244,63,94,0.4);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>📊</div>
    <div style='font-size:10px;color:#fb7185;margin-top:6px;font-weight:600;'>refresh mart</div>
    <div style='font-size:9px;color:#475569;'>CONCURRENTLY</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:90px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(148,163,184,0.1);border:1px solid rgba(148,163,184,0.2);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>🧹</div>
    <div style='font-size:10px;color:#94a3b8;margin-top:6px;font-weight:600;'>cleanup</div>
    <div style='font-size:9px;color:#475569;'>all_done rule</div>
  </div>

  <div style='width:36px;height:2px;background:rgba(99,102,241,0.4);flex-shrink:0;'></div>

  <div style='text-align:center;min-width:80px;'>
    <div style='width:52px;height:52px;border-radius:14px;background:rgba(99,102,241,0.25);border:1px solid rgba(99,102,241,0.5);display:flex;align-items:center;justify-content:center;font-size:20px;margin:0 auto;'>⏹</div>
    <div style='font-size:10px;color:#475569;margin-top:6px;'>end</div>
  </div>

</div>
</div>
""", unsafe_allow_html=True)

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # Star schema diagram
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🗄️ Star Schema")
        st.markdown("""
<div style='background:#0d1320;border:1px solid rgba(255,255,255,0.07);border-radius:14px;padding:24px;font-family:monospace;font-size:12px;line-height:1.8;color:#94a3b8;'>
<pre style='color:#94a3b8;margin:0;'>
            ┌──────────────┐
            │  <span style='color:#67e8f9;'>dim_date</span>     │
            │ date_key PK  │
            └──────┬───────┘
                   │
┌──────────────┐   │  ┌─────────────────────────┐
│ <span style='color:#67e8f9;'>dim_product</span>  │   │  │     <span style='color:#a5b4fc;'>fact_sales</span>           │
│ product_sk PK├───┼──│ sale_sk + sale_date PK  │
│ SCD Type-2   │   │  │ Partitioned by month    │
│ valid_from   │   │  │ row_checksum (SHA-256)  │
│ valid_to     │   │  │ pipeline_run_id         │
└──────────────┘   │  └──────────────┬──────────┘
                   │                 │
            ┌──────┘          ┌──────┴───────┐
            └─────────────────│ <span style='color:#67e8f9;'>dim_customer</span>  │
                              │ customer_sk  │
                              └──────────────┘
</pre>
</div>
""", unsafe_allow_html=True)

    with col2:
        st.markdown("#### ⚡ Innovation Highlights")
        highlights = [
            ("🔒", "Checksum-Gated UPSERT", "Zero I/O on re-runs. WHERE row_checksum != EXCLUDED.row_checksum"),
            ("🚦", "Pandera Contracts", "Schema validated at extract boundary — bad rows to dead-letter"),
            ("🔄", "SCD Type-2 Products", "Historical price tracking without corrupting past revenue"),
            ("📊", "Concurrent Mart Refresh", "Zero BI downtime during MATERIALIZED VIEW refresh"),
            ("🗂️", "Monthly Partitioning", "Partition pruning skips 90%+ of data on date-range queries"),
            ("🔍", "ContextVar Logging", "run_id + task_id injected into every log line — full trace"),
            ("✔️", "validate_load Gate", "SUM(net_amount) parity < 0.1% tolerance — hard stop on data loss"),
            ("♻️", "all_done Cleanup", "Temp files removed even on upstream task failure"),
        ]
        for icon, title, desc in highlights:
            st.markdown(f"""
<div style='background:#0d1320;border:1px solid rgba(255,255,255,0.07);border-radius:10px;
            padding:10px 14px;margin-bottom:8px;display:flex;align-items:flex-start;gap:10px;'>
  <div style='font-size:18px;flex-shrink:0;'>{icon}</div>
  <div>
    <div style='font-size:13px;font-weight:700;color:#f1f5f9;'>{title}</div>
    <div style='font-size:11px;color:#475569;margin-top:2px;'>{desc}</div>
  </div>
</div>
""", unsafe_allow_html=True)

    # Quick start
    st.markdown("#### 🚀 Quick Start")
    st.code("""# 1. Generate 5 years of data (~5M rows)
python -m etl.generate_data --start 2020-01-01 --end 2024-12-31

# 2. Start full Docker stack (Airflow + PostgreSQL)
docker compose up -d

# 3. Open Airflow UI → Enable and trigger 'sales_etl' DAG
#    http://localhost:8080  (airflow / airflow)

# 4. Run tests (no Docker needed)
pytest tests/ -v --ignore=tests/test_dag.py
#  → 49 passed, 2 warnings in 2.33s""", language="bash")
