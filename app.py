"""
STP/FTA Duty Savings Simulator — POC
Nike Foundation Data Technology

Powered by published_domain.trade_customs (Databricks published views).
Goods value from commodity_tariff_v with CUSTOMS_AMT_USD fallback.
STP eligibility from non_published_domain.trade_customs (raw tables).
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from real_data_loader import PROGRAM_NAMES, COUNTRY_NAMES, PSC_WINDOW_DAYS, get_filing_list_for_lane
from fta_rules import (
    load_all_ftas, enrich_lanes_with_fta, get_fta_info_for_origin,
    find_agreements_for_lane, _summarize_rule_for_lane,
)

def _select_loader():
    """Prefer live Databricks connection; fall back to CSV files."""
    _loader_msg = None
    try:
        import databricks_loader
        if databricks_loader.is_configured():
            ok, msg = databricks_loader.test_connection()
            if ok:
                return databricks_loader.load_all, "databricks", msg
            else:
                _loader_msg = f"Databricks connection failed: {msg}"
        else:
            _loader_msg = "Databricks not configured — falling back to CSV"
    except Exception as e:
        import traceback
        _loader_msg = f"Databricks loader error: {e}\n{traceback.format_exc()}"
    from real_data_loader import load_all
    return load_all, "csv", _loader_msg

_load_fn, _data_source, _loader_msg = _select_loader()

st.set_page_config(
    page_title="STP/FTA Duty Savings Simulator",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)

if _loader_msg and _data_source == "csv":
    st.error(f"⚠️ **Data Source Issue** — Could not connect to Databricks. Details below:")
    st.code(_loader_msg, language="text")
    st.info("The app will attempt to load from local CSV files as a fallback.")

NIKE_ORANGE = "#FA5400"
NIKE_BLACK = "#111111"
NIKE_GREEN = "#00A94F"
NIKE_RED = "#D43B2E"
NIKE_GRAY = "#757575"
SAVINGS_GREEN = "#00A94F"
GAP_RED = "#D43B2E"
NEUTRAL_BLUE = "#0077C8"

st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background-color: #FAFAFA; }
    .block-container { padding-top: 1rem; max-width: 1400px; }
    .metric-card {
        background: white; border-radius: 12px; padding: 20px;
        border: 1px solid #E5E5E5; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    .metric-label { font-size: 13px; color: #757575; margin-bottom: 4px; }
    .metric-value { font-size: 28px; font-weight: 700; color: #111; }
    .metric-delta { font-size: 13px; margin-top: 4px; }
    .metric-delta.positive { color: #00A94F; }
    .metric-delta.negative { color: #D43B2E; }
    .section-header {
        font-size: 18px; font-weight: 600; color: #111;
        margin-top: 24px; margin-bottom: 12px;
        padding-bottom: 8px; border-bottom: 2px solid #FA5400;
    }
    .poc-banner {
        background: linear-gradient(135deg, #111 0%, #333 100%);
        color: white; padding: 20px 28px; border-radius: 12px; margin-bottom: 24px;
    }
    .poc-banner h1 { margin: 0; font-size: 26px; font-weight: 700; }
    .poc-banner p { margin: 6px 0 0 0; font-size: 14px; color: #AAA; }
    .poc-badge {
        display: inline-block; background: #FA5400; color: white;
        font-size: 11px; font-weight: 600; padding: 3px 10px;
        border-radius: 12px; margin-left: 12px; vertical-align: middle;
    }
    .live-badge {
        display: inline-block; background: #00A94F; color: white;
        font-size: 11px; font-weight: 600; padding: 3px 10px;
        border-radius: 12px; margin-left: 8px; vertical-align: middle;
    }
    .insight-box {
        background: #FFF8F0; border-left: 4px solid #FA5400;
        padding: 16px 20px; border-radius: 0 8px 8px 0;
        margin: 16px 0; font-size: 14px; color: #333;
    }
    .gap-highlight {
        background: #FFF0F0; border-left: 4px solid #D43B2E;
        padding: 12px 16px; border-radius: 0 8px 8px 0;
        margin: 8px 0; font-size: 14px;
    }
    .savings-highlight {
        background: #F0FFF4; border-left: 4px solid #00A94F;
        padding: 12px 16px; border-radius: 0 8px 8px 0;
        margin: 8px 0; font-size: 14px;
    }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        border-bottom-color: #FA5400 !important; color: #FA5400 !important;
    }
</style>
""", unsafe_allow_html=True)


def fmt_usd(val: float, decimals: int = 1) -> str:
    """Format a dollar amount with auto-scaling: $1.2B, $345.6M, $12.3K."""
    av = abs(val)
    if av >= 1e9:
        return f"${val / 1e9:,.{decimals}f}B"
    if av >= 1e6:
        return f"${val / 1e6:,.{decimals}f}M"
    if av >= 1e3:
        return f"${val / 1e3:,.{decimals}f}K"
    return f"${val:,.{decimals}f}"


def metric_card(label, value, delta="", delta_type="positive"):
    delta_html = f'<div class="metric-delta {delta_type}">{delta}</div>' if delta else ""
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """


_DATA_VERSION = 10  # bump: savings realized vs left on table cards

@st.cache_data(ttl=3600, show_spinner="Loading data from Databricks...")
def load_data(_version=_DATA_VERSION):
    return _load_fn()


with st.sidebar:
    if st.button("Refresh Data", help="Clear cached data and reload from Databricks"):
        st.cache_data.clear()
        st.rerun()

data = load_data()

if data is None:
    if _data_source == "csv":
        st.error("No data available. Databricks connection failed and no local CSV files found.")
        if _loader_msg:
            st.code(_loader_msg, language="text")
    else:
        st.error("Failed to load data from Databricks.")
    st.stop()

lanes = data["lanes"]
stp_df = data["stp_eligibility"]
duties_df = data["duties_paid"]
programs_df = data["programs"]
recovery_df = data["recovery"]
claim_packages = data["claim_packages"]
claims_detail = data["claims_detail"]
gen_pref_df = data["gen_pref_df"]
meta = data["meta"]

# ── Load FTA rules and enrich lanes ──────────────────────────────────
@st.cache_data(ttl=3600, show_spinner="Loading FTA rules...")
def _cached_ftas():
    return load_all_ftas()

_fta_agreements = _cached_ftas()
if _fta_agreements:
    enrich_lanes_with_fta(lanes, _fta_agreements)

# ── Header ───────────────────────────────────────────────────────────
_is_live = meta.get("data_source") == "databricks_live"
_source_badge = '<span style="background:#00A94F;color:white;font-size:10px;font-weight:600;padding:3px 8px;border-radius:12px;margin-left:8px;">LIVE DATABRICKS</span>' if _is_live else ""
sample_warning = ' <span style="background:#FFCC00;color:#111;font-size:10px;font-weight:600;padding:3px 8px;border-radius:12px;margin-left:8px;">500K ROW SAMPLE</span>' if meta.get("duties_sample_limited") else ""

_sample_note = " &middot; <strong style='color:#FFCC00;'>Sample — full dataset is significantly larger</strong>" if meta.get("duties_sample_limited") else ""
_completeness_note = meta.get("data_completeness_note", "")
_completeness_html = (
    f'<p style="margin-top:6px; font-size:11px; color:#FFCC00; background:rgba(255,204,0,0.1); '
    f'padding:4px 8px; border-radius:4px; display:inline-block;">'
    f'&#9888; Data Coverage: {_completeness_note}</p>'
) if _completeness_note else ""

st.markdown(
    f'<div class="poc-banner">'
    f'<h1>STP/FTA Duty Savings Simulator'
    f' <span class="poc-badge">POC</span>'
    f' <span class="live-badge">REAL DATA</span>{_source_badge}{sample_warning}'
    f'</h1>'
    f'<p><strong>FY26</strong> ({meta["duties_date_range"]}) &middot;'
    f' <strong>{meta["stp_total_records"]:,}</strong> STP eligibility records across'
    f' <strong>{meta["unique_programs"]}</strong> trade programs &middot;'
    f' <strong>{meta["duties_total_records"]:,}</strong> duty declaration line items</p>'
    f'<p style="margin-top:8px; font-size:12px; color:#666;">'
    f'Source: {meta.get("source", "published_domain.trade_customs")} &middot;'
    f' Last ETL: {meta["last_etl"]}{_sample_note}</p>'
    f'{_completeness_html}'
    f'</div>',
    unsafe_allow_html=True,
)

# ── Tabs ─────────────────────────────────────────────────────────────
tab_gap, tab_recovery, tab_sim, tab_util, tab_programs, tab_pipeline = st.tabs([
    "💰 Duty Savings Gap (FY26)",
    "💵 Retroactive Recovery",
    "🔄 Sourcing Shift Simulator",
    "📊 Utilization by Lane",
    "🔍 Program Deep Dive",
    "🔗 Data Pipeline & Landscape",
])


# ======================================================================
# TAB 1: DUTY SAVINGS GAP (REAL DATA)
# ======================================================================
with tab_gap:

    stp_lanes = lanes[lanes["has_stp"]]
    effective_stp_lanes = lanes[(lanes["has_stp"]) & (lanes["eligible_pct"] >= 0.01)]
    gap_lanes = lanes[lanes["is_gap_lane"] & (lanes["eligible_pct"] >= 0.01)]

    total_goods_value = meta["total_goods_value"]
    total_gen_duty = meta["total_gen_duty"]
    total_stp_duty = meta["total_stp_duty"]
    total_savings = meta["total_savings_potential"]
    total_savings_realized = meta.get("total_savings_realized", 0)
    total_excess = lanes[lanes["has_stp"] & (lanes["eligible_pct"] >= 0.01)]["excess_duty_usd"].sum()
    gap_lane_count = len(gap_lanes)
    total_lanes_with_stp = len(effective_stp_lanes)

    overall_pref_y = lanes["pref_y_count"].sum()
    overall_pref_n = lanes["pref_n_count"].sum()
    overall_util = overall_pref_y / (overall_pref_y + overall_pref_n) if (overall_pref_y + overall_pref_n) > 0 else 0

    total_line_items = lanes["total_rows"].sum()

    cols = st.columns(5)
    with cols[0]:
        gv_display = fmt_usd(total_goods_value, 0)
        duty_display = fmt_usd(total_gen_duty, 0)
        st.markdown(metric_card(
            "FY26 US Customs Goods Value",
            gv_display,
            f"Est. actual duty: {duty_display}",
            "neutral"
        ), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(metric_card(
            "Duty Savings Realized",
            fmt_usd(total_savings_realized),
            f"Saved by claiming STPs across {total_lanes_with_stp} eligible lanes",
            "positive"
        ), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(metric_card(
            "Overall STP Utilization",
            f"{overall_util:.1%}",
            f"{overall_pref_y:,.0f} preferential / {overall_pref_y + overall_pref_n:,.0f} total",
            "positive" if overall_util > 0.5 else "negative"
        ), unsafe_allow_html=True)
    with cols[3]:
        st.markdown(metric_card(
            "Gap Lanes (<50% utilization)",
            f"{gap_lane_count}",
            f"of {total_lanes_with_stp} STP-eligible lanes",
            "negative"
        ), unsafe_allow_html=True)
    with cols[4]:
        st.markdown(metric_card(
            "Savings Left on the Table",
            fmt_usd(total_excess),
            f"Not claimed across {total_lanes_with_stp} STP-eligible lanes",
            "negative"
        ), unsafe_allow_html=True)

    st.markdown(f"""
    <div class="insight-box">
        <strong>FY26 US Imports ({meta['duties_date_range']}):</strong>
        Across <strong>{fmt_usd(total_goods_value, 0)}</strong> in customs goods value
        ({total_line_items:,.0f} line items), <strong>{overall_util:.1%}</strong> received preferential treatment.
        STP claims saved <strong>{fmt_usd(total_savings_realized)}</strong> in duty,
        while <strong>{fmt_usd(total_excess)}</strong> in savings was left on the table
        across {total_lanes_with_stp} STP-eligible lanes.
        <em>Rates: MFN base from Databricks tariff schedule + FY26 surcharges
        (IEEPA reciprocal Apr 2025&ndash;Feb 2026, Section 122 15% Feb 24 2026+).</em>
    </div>
    """, unsafe_allow_html=True)

    fq_counts = meta.get("fiscal_quarter_counts", {})
    if fq_counts:
        st.markdown('<div class="section-header">Declaration Volume by Fiscal Quarter</div>', unsafe_allow_html=True)
        fq_df = pd.DataFrame([
            {"quarter": k, "declarations": v}
            for k, v in sorted(fq_counts.items()) if k != "Unknown"
        ])
        if not fq_df.empty:
            fig_fq = px.bar(
                fq_df, x="quarter", y="declarations",
                color_discrete_sequence=[NEUTRAL_BLUE],
                text=fq_df["declarations"].apply(lambda x: f"{x:,.0f}"),
            )
            fig_fq.update_layout(
                height=250, margin=dict(t=10, b=30),
                xaxis_title="Nike Fiscal Quarter",
                yaxis_title="GEN/PREF Line Items",
                plot_bgcolor="white", showlegend=False,
            )
            fig_fq.update_traces(textposition="outside")
            st.plotly_chart(fig_fq, use_container_width=True)

    # Dynamic insight: highlight lanes with the most excess duty (real savings opportunity)
    _top_excess = lanes[
        (lanes["has_stp"]) & (lanes["excess_duty_usd"] > 10_000)
    ].nlargest(3, "excess_duty_usd")

    if not _top_excess.empty:
        _gap_parts = []
        for _, r in _top_excess.iterrows():
            _prog = ", ".join(r["stp_programs"][:2]) if r["stp_programs"] else "STP"
            _diff = r.get("fta_difficulty", "—")
            _gap_parts.append(
                f"{r['origin_name']} &rarr; {r['dest_name']}: "
                f"<strong>{fmt_usd(r['excess_duty_usd'])}</strong> savings "
                f"({r['gen_rate']:.1%} GEN &rarr; "
                f"{r['stp_rate']:.1%} under {_prog}), "
                f"ROO difficulty: {_diff}, utilization: <strong>{r['utilization_pct']:.1%}</strong>."
            )
        st.markdown(f"""
        <div class="gap-highlight">
            <strong>Largest savings opportunities:</strong> {" ".join(_gap_parts)}
        </div>
        """, unsafe_allow_html=True)

    # Informational note about excluded products
    _excluded_lanes = lanes[
        (lanes["has_stp"]) & (lanes["eligible_pct"] < 0.01)
    ]
    if not _excluded_lanes.empty:
        _excluded_names = ", ".join(_excluded_lanes["origin_name"].tolist()[:5])
        st.markdown(f"""
        <div class="insight-box">
            <strong>Note:</strong> {len(_excluded_lanes)} lane(s) have STP program data
            but 0% product eligibility due to HS chapter exclusions
            ({_excluded_names}). US GSP excludes textiles/apparel (Ch 50-63)
            and footwear (Ch 64) — Nike's primary product categories for these origins.
        </div>
        """, unsafe_allow_html=True)

    # ── Gap lanes table ──
    st.markdown('<div class="section-header">Trade Lanes: STP Eligibility vs Actual Utilization</div>', unsafe_allow_html=True)

    display_lanes = lanes[lanes["goods_value_usd"] > 0].copy()
    display_lanes["programs_str"] = display_lanes["stp_programs"].apply(
        lambda x: ", ".join(x[:3]) if x else "None"
    )

    _has_fta = "fta_name" in display_lanes.columns

    _base_cols = [
        "origin_name", "dest_name", "goods_value_usd", "eligible_goods_value_usd",
        "has_stp", "stp_qualification_rate", "utilization_pct", "gen_duty_usd",
        "stp_duty_usd", "excess_duty_usd", "programs_str",
    ]
    if _has_fta:
        _base_cols += ["fta_name", "fta_difficulty", "fta_rule_summary"]

    display_table = display_lanes[_base_cols].copy()
    _has_stp_raw = display_table["has_stp"].copy()
    _eligible_pct_raw = display_lanes["eligible_pct"].values

    _col_names = [
        "Origin", "Destination", "Goods Value ($)", "Eligible Value ($)",
        "STP Status", "Qualification Rate", "Utilization at Customs",
        "Est. GEN Duty ($)", "Est. STP Duty ($)", "Excess Duty ($)",
        "Programs",
    ]
    if _has_fta:
        _col_names += ["Best FTA", "Difficulty", "Key Rule"]
    display_table.columns = _col_names

    display_table["Goods Value ($)"] = display_table["Goods Value ($)"].apply(lambda x: f"${x:,.0f}")
    display_table["Eligible Value ($)"] = [
        f"${v:,.0f} ({p:.0%})" if stp else "N/A"
        for v, p, stp in zip(display_lanes["eligible_goods_value_usd"], _eligible_pct_raw, _has_stp_raw)
    ]

    # Potential US trade programs by origin — programs that exist but may not
    # be in Nike's STP eligibility data yet.
    _POTENTIAL_US_PROGRAMS = {
        "PK": "US GSP (beneficiary — not assessed)",
        "IN": "US GSP (beneficiary — not assessed)",
        "PH": "US GSP (beneficiary — not assessed)",
        "KR": "KORUS FTA (not assessed)",
        "SG": "US-Singapore FTA (not assessed)",
        "JP": "US-Japan Trade Agreement (limited — not assessed)",
        "IL": "US-Israel FTA (not assessed)",
    }

    _stp_status = []
    for idx, (stp, ep) in enumerate(zip(_has_stp_raw, _eligible_pct_raw)):
        origin_cd = display_lanes.iloc[idx]["country_of_origin_cd"]

        if not stp:
            potential = _POTENTIAL_US_PROGRAMS.get(origin_cd)
            if potential:
                _stp_status.append("Not Assessed")
            else:
                _stp_status.append("No Program")
        elif ep < 0.01:
            _stp_status.append("Products Excluded")
        elif ep < 0.50:
            _stp_status.append(f"Partial ({ep:.0%} eligible)")
        elif ep < 1.0:
            _stp_status.append(f"Eligible ({ep:.0%})")
        else:
            _stp_status.append("Eligible")
    display_table["STP Status"] = _stp_status

    display_table["Qualification Rate"] = [
        f"{q:.1%}" if stp and ep >= 0.01 else "N/A"
        for q, stp, ep in zip(display_lanes["stp_qualification_rate"], _has_stp_raw, _eligible_pct_raw)
    ]
    display_table["Utilization at Customs"] = display_table["Utilization at Customs"].apply(lambda x: f"{x:.1%}")
    display_table["Est. GEN Duty ($)"] = display_table["Est. GEN Duty ($)"].apply(lambda x: f"${x:,.0f}")
    display_table["Est. STP Duty ($)"] = [
        f"${v:,.0f}" if stp and ep >= 0.01 else "N/A"
        for v, stp, ep in zip(display_lanes["stp_duty_usd"], _has_stp_raw, _eligible_pct_raw)
    ]
    display_table["Excess Duty ($)"] = [
        f"${v:,.0f}" if stp and ep >= 0.01 else "N/A"
        for v, stp, ep in zip(display_lanes["excess_duty_usd"], _has_stp_raw, _eligible_pct_raw)
    ]
    # For non-STP lanes: show FTA info if there's a potential program (awareness),
    # suppress if there's genuinely no program available.
    if _has_fta:
        _has_potential = [
            display_lanes.iloc[i]["country_of_origin_cd"] in _POTENTIAL_US_PROGRAMS
            for i in range(len(display_lanes))
        ]
        display_table["Best FTA"] = [
            name if (stp or pot) else "—"
            for name, stp, pot in zip(display_table["Best FTA"], _has_stp_raw, _has_potential)
        ]
        display_table["Difficulty"] = [
            d if (stp or pot) else "—"
            for d, stp, pot in zip(display_table["Difficulty"], _has_stp_raw, _has_potential)
        ]
        display_table["Key Rule"] = [
            r if (stp or pot) else "—"
            for r, stp, pot in zip(display_table["Key Rule"], _has_stp_raw, _has_potential)
        ]

    st.dataframe(display_table, use_container_width=True, hide_index=True, height=500)

    # ── Excess duty by origin ──
    st.markdown('<div class="section-header">Savings Opportunity by Sourcing Country</div>', unsafe_allow_html=True)

    excess_by_origin = (
        gap_lanes.groupby("origin_name").agg(
            excess_duty_usd=("excess_duty_usd", "sum"),
        )
        .reset_index()
        .sort_values("excess_duty_usd", ascending=False)
    )
    if not excess_by_origin.empty:
        fig_origin = px.bar(
            excess_by_origin,
            x="origin_name",
            y="excess_duty_usd",
            color_discrete_sequence=[NIKE_ORANGE],
            text=excess_by_origin["excess_duty_usd"].apply(fmt_usd),
        )
        fig_origin.update_layout(
            height=350, margin=dict(t=20, b=40),
            xaxis_title="Sourcing Country",
            yaxis_title="Excess Duty ($)",
            yaxis_tickprefix="$", yaxis_tickformat=",",
            plot_bgcolor="white", showlegend=False,
        )
        fig_origin.update_traces(textposition="outside")
        st.plotly_chart(fig_origin, use_container_width=True)


# ======================================================================
# TAB 2: RETROACTIVE RECOVERY
# ======================================================================
with tab_recovery:
    st.markdown(f'<div class="section-header">Retroactive Duty Recovery — {PSC_WINDOW_DAYS}-Day PSC Window</div>', unsafe_allow_html=True)

    psc_window_start = (pd.Timestamp.now() - pd.Timedelta(days=PSC_WINDOW_DAYS)).strftime("%b %d, %Y")

    if recovery_df is not None and not recovery_df.empty:
        total_recovery = recovery_df["estimated_recovery"].sum()
        total_declarations = recovery_df["declaration_count"].sum()
        total_gen_paid = recovery_df["gen_duty_paid"].sum()
        total_duty_savings = recovery_df["duty_savings"].sum()

        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
        col_r1.metric("Estimated Recovery", fmt_usd(total_recovery))
        col_r2.metric("Declarations in Window", f"{total_declarations:,.0f}")
        col_r3.metric("Full Duty Savings (if all qualify)", fmt_usd(total_duty_savings))
        col_r4.metric("PSC Window Start", psc_window_start)

        st.markdown(f"""
        <div class="insight-box" style="border-left-color: #E8B130;">
            <strong>Immediate Opportunity:</strong> Within the last {PSC_WINDOW_DAYS} days, <strong>{total_declarations:,.0f}</strong>
            declarations paid General duty rates on lanes where STP eligibility exists.
            The estimated GEN duty on these entries totals <strong>{fmt_usd(total_gen_paid)}</strong> vs
            <strong>{fmt_usd(total_gen_paid - total_duty_savings)}</strong> under STP rates.
            Discounted by qualification rate, the estimated recovery is <strong>{fmt_usd(total_recovery)}</strong>.
            Under 19 USC 1514, importers have 180 days from liquidation to file a PSC.
            <br><br>
            <em>Duty amounts estimated using MFN base rates (from HTS tariff schedule) + FY26 tariff surcharges
            (IEEPA reciprocal + Section 122). Accuracy improves as FY26 actuals become available.</em>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="section-header">Recovery by Trade Lane</div>', unsafe_allow_html=True)

        recovery_display = recovery_df[[
            "origin_name", "dest_name", "declaration_count",
            "gen_duty_paid", "stp_qualification_rate", "estimated_recovery",
            "earliest", "latest",
        ]].copy()

        earliest_dt = pd.to_datetime(recovery_display["earliest"])
        days_remaining = PSC_WINDOW_DAYS - (pd.Timestamp.now() - earliest_dt).dt.days
        days_remaining = days_remaining.clip(lower=0)

        recovery_display.columns = [
            "Origin", "Destination", "Declarations",
            "Est. GEN Duty ($)", "STP Qual. Rate", "Est. Recovery ($)",
            "First Declaration", "Last Declaration",
        ]
        recovery_display["Est. GEN Duty ($)"] = recovery_display["Est. GEN Duty ($)"].apply(lambda x: f"${x:,.0f}")
        recovery_display["STP Qual. Rate"] = recovery_display["STP Qual. Rate"].apply(lambda x: f"{x:.1%}")
        recovery_display["Est. Recovery ($)"] = recovery_display["Est. Recovery ($)"].apply(lambda x: f"${x:,.0f}")
        recovery_display["First Declaration"] = pd.to_datetime(recovery_display["First Declaration"]).dt.strftime("%Y-%m-%d")
        recovery_display["Last Declaration"] = pd.to_datetime(recovery_display["Last Declaration"]).dt.strftime("%Y-%m-%d")
        recovery_display["Days Remaining"] = days_remaining.apply(
            lambda d: f"{d} days" if d > 30 else f"{d} days ⚠️" if d > 0 else "EXPIRED"
        )

        st.dataframe(recovery_display.head(20), use_container_width=True, hide_index=True)

        fig_rec = px.bar(
            recovery_df.head(10),
            x="origin_name", y="estimated_recovery",
            text=recovery_df.head(10)["estimated_recovery"].apply(fmt_usd),
            color_discrete_sequence=["#E8B130"],
            labels={"origin_name": "Origin Country", "estimated_recovery": "Estimated Recovery ($)"},
        )
        fig_rec.update_layout(
            height=350, margin=dict(t=20, b=30),
            plot_bgcolor="white", showlegend=False,
        )
        fig_rec.update_traces(textposition="outside")
        st.plotly_chart(fig_rec, use_container_width=True)

        st.markdown("""
        <div class="insight-box">
            <strong>How PSC Recovery Works:</strong>
            <ol style="margin-top:8px; margin-bottom:0;">
                <li><strong>Identify entries</strong> — declarations where GEN duty was paid but STP eligibility existed</li>
                <li><strong>Validate CoO</strong> — ensure Certificate of Origin can be obtained or already exists in E2Open/Amber Road</li>
                <li><strong>File PSC</strong> — submit Post-Summary Correction to CBP within 180-day window</li>
                <li><strong>Receive refund</strong> — CBP processes refund of excess duty (typically 30-90 days)</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)

        # ── Claim Package Generator ──
        st.markdown('<div class="section-header">PSC Claim Package Generator</div>', unsafe_allow_html=True)
        st.markdown("""
        <div class="insight-box" style="border-left-color: #0077C8;">
            <strong>Select a trade lane below</strong> to generate the claim package — including the applicable STP,
            required documents, HTS codes to amend, filing mechanism, and estimated recovery per lane.
            This is the information your broker needs to file the Post-Summary Correction.
        </div>
        """, unsafe_allow_html=True)

        if claim_packages:
            lane_options = {
                f"{pkg['origin_name']} -> {pkg['dest_name']} (${pkg['estimated_recovery']:,.0f} est. recovery)": i
                for i, pkg in enumerate(claim_packages)
                if pkg["estimated_recovery"] > 0
            }

            if lane_options:
                selected_lane = st.selectbox(
                    "Select trade lane for claim package",
                    options=list(lane_options.keys()),
                    key="claim_lane_select",
                )
                pkg = claim_packages[lane_options[selected_lane]]

                claim_col1, claim_col2 = st.columns(2)

                with claim_col1:
                    st.markdown(f"""
                    <div class="metric-card" style="border-top: 4px solid {NIKE_ORANGE};">
                        <div class="metric-label">Claim Summary</div>
                        <table style="width:100%; font-size:13px; margin-top:8px;">
                            <tr><td style="color:#757575;">Origin</td><td style="font-weight:600;">{pkg['origin_name']} ({pkg['origin_cd']})</td></tr>
                            <tr><td style="color:#757575;">Destination</td><td style="font-weight:600;">{pkg['dest_name']} ({pkg['dest_cd']})</td></tr>
                            <tr><td style="color:#757575;">Applicable STP(s)</td><td style="font-weight:600;">{', '.join(pkg['applicable_stp_names'][:3])}</td></tr>
                            <tr><td style="color:#757575;">STP Qualification Rate</td><td style="font-weight:600;">{pkg['qualification_rate']:.1%}</td></tr>
                            <tr><td style="color:#757575;">Declarations to Amend</td><td style="font-weight:600;">{pkg['declaration_count']:,}</td></tr>
                            <tr><td style="color:#757575;">Goods Value</td><td style="font-weight:600;">${pkg['goods_value']:,.0f}</td></tr>
                            <tr><td style="color:#757575;">Est. GEN Duty</td><td style="font-weight:600; color:{GAP_RED};">${pkg['gen_duty_paid']:,.0f}</td></tr>
                            <tr><td style="color:#757575;">Est. Duty Savings</td><td style="font-weight:600;">${pkg['duty_savings']:,.0f}</td></tr>
                            <tr><td style="color:#757575;">Estimated Recovery</td><td style="font-weight:600; color:{SAVINGS_GREEN};">${pkg['estimated_recovery']:,.0f}</td></tr>
                            <tr><td style="color:#757575;">Declaration Window</td><td style="font-weight:600;">{pkg['window_start']} to {pkg['window_end']}</td></tr>
                        </table>
                    </div>
                    """, unsafe_allow_html=True)

                with claim_col2:
                    filing = pkg["filing_mechanism"]
                    st.markdown(f"""
                    <div class="metric-card" style="border-top: 4px solid {NEUTRAL_BLUE};">
                        <div class="metric-label">Filing Mechanism</div>
                        <table style="width:100%; font-size:13px; margin-top:8px;">
                            <tr><td style="color:#757575;">Method</td><td style="font-weight:600;">{filing.get('method', 'N/A')}</td></tr>
                            <tr><td style="color:#757575;">Authority</td><td style="font-weight:600;">{filing.get('authority', 'N/A')}</td></tr>
                            <tr><td style="color:#757575;">Portal</td><td style="font-weight:600;">{filing.get('portal', 'N/A')}</td></tr>
                            <tr><td style="color:#757575;">Deadline</td><td style="font-weight:600; color:{GAP_RED};">{filing.get('deadline', 'N/A')}</td></tr>
                            <tr><td style="color:#757575;">Legal Basis</td><td style="font-weight:600;">{filing.get('legal_basis', 'N/A')}</td></tr>
                            <tr><td style="color:#757575;">Processing Time</td><td style="font-weight:600;">{filing.get('typical_processing', 'N/A')}</td></tr>
                        </table>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown('<div class="section-header">Required Documents</div>', unsafe_allow_html=True)
                for doc in pkg["required_documents"]:
                    status_color = SAVINGS_GREEN if "Available" in doc["status"] else (GAP_RED if "To file" in doc["status"] else NIKE_ORANGE)
                    st.markdown(f"""
                    <div style="background:white; border-radius:8px; padding:12px 16px; margin-bottom:6px;
                                border:1px solid #E5E5E5; border-left:4px solid {status_color};
                                display:flex; justify-content:space-between; align-items:flex-start;">
                        <div style="flex:1;">
                            <span style="font-weight:600; font-size:13px;">{doc['document']}</span>
                            <div style="font-size:12px; color:#757575; margin-top:2px;">{doc['description']}</div>
                        </div>
                        <div style="text-align:right; min-width:140px;">
                            <div style="font-size:11px; color:#757575;">Source: {doc['source']}</div>
                            <div style="background:{status_color}20; color:{status_color}; font-size:10px; font-weight:600;
                                        padding:2px 8px; border-radius:8px; margin-top:4px; display:inline-block;">
                                {doc['status']}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                if pkg.get("hts_detail"):
                    st.markdown('<div class="section-header">Top HTS Codes to Amend</div>', unsafe_allow_html=True)
                    hts_df = pd.DataFrame(pkg["hts_detail"])
                    hts_df.columns = ["HTS Code", "Declarations", "Est. Duty ($)", "GEN Rate (%)"]
                    hts_df["Est. Duty ($)"] = hts_df["Est. Duty ($)"].apply(lambda x: f"${x:,.0f}")
                    hts_df["GEN Rate (%)"] = hts_df["GEN Rate (%)"].apply(lambda x: f"{x:.1f}%")
                    st.dataframe(hts_df, use_container_width=True, hide_index=True)

                # ── Filing-Ready Entry List ──
                if claims_detail is not None:
                    filing_list = get_filing_list_for_lane(
                        claims_detail, pkg["origin_cd"], pkg["dest_cd"],
                        lane_programs=pkg.get("applicable_stps", []),
                    )

                    if not filing_list.empty:
                        st.markdown('<div class="section-header">Filing-Ready Entry List</div>', unsafe_allow_html=True)

                        urgent = filing_list[filing_list["days_remaining"] <= 30]
                        if not urgent.empty:
                            st.markdown(f"""
                            <div class="gap-highlight" style="border-left-color: {GAP_RED};">
                                <strong>URGENT:</strong> {len(urgent)} entries have <strong>30 days or fewer</strong>
                                remaining in the PSC window. These must be filed immediately to avoid losing
                                <strong>${urgent['total_duty'].sum():,.0f}</strong> in recoverable duty.
                            </div>
                            """, unsafe_allow_html=True)

                        fl_display = filing_list[[
                            "filing_reference_nbr", "acceptance_date", "line_items",
                            "total_duty", "hts_codes", "days_remaining", "declaration_status",
                        ]].copy()
                        fl_display.columns = [
                            "Filing Reference", "Acceptance Date", "Line Items",
                            "Total Duty ($)", "HTS Codes", "Days Remaining", "Status",
                        ]
                        fl_display["Acceptance Date"] = pd.to_datetime(fl_display["Acceptance Date"]).dt.strftime("%Y-%m-%d")
                        fl_display["Total Duty ($)"] = fl_display["Total Duty ($)"].apply(lambda x: f"${x:,.0f}")
                        fl_display["Days Remaining"] = fl_display["Days Remaining"].apply(
                            lambda d: f"{d} days" if d > 30 else f"{d} days !!!" if d > 0 else "EXPIRED"
                        )

                        filing_container = st.container(height=500)
                        filing_container.dataframe(fl_display, use_container_width=True, hide_index=True)

                        csv_data = filing_list.to_csv(index=False)
                        st.download_button(
                            label=f"Download Filing List — {pkg['origin_name']} to {pkg['dest_name']} ({len(filing_list)} entries)",
                            data=csv_data,
                            file_name=f"PSC_Filing_List_{pkg['origin_cd']}_{pkg['dest_cd']}.csv",
                            mime="text/csv",
                        )

                        st.markdown(f"""
                        <div class="insight-box" style="border-left-color: {SAVINGS_GREEN};">
                            <strong>This list is filing-ready.</strong> Each row is a customs entry with its filing
                            reference number, line item count, HTS codes, duty paid, and days remaining.
                            Download the CSV and hand it to the broker — they have everything needed to
                            file Post-Summary Corrections in ACE.
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.info(f"No entry-level detail available for {pkg['origin_name']} -> {pkg['dest_name']}. "
                                f"The claims detail query covers Oct 2025 data — this lane may need a broader date range.")

    else:
        st.info("No recovery data available. Ensure FY26 duties data with acceptance_tmst is loaded.")


# ======================================================================
# TAB 3: SOURCING SHIFT SIMULATOR
# ======================================================================
with tab_sim:
    st.markdown('<div class="section-header">Sourcing Shift Simulator — Duty Impact Analysis</div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="insight-box">
        <strong>What-if analysis:</strong> Model the duty impact of shifting production volume from one
        sourcing country to another. See how STP eligibility and preferential rates change across destinations,
        and whether the shift unlocks new trade programs or increases exposure.
    </div>
    """, unsafe_allow_html=True)

    HTS_PRODUCT_LINE = {
        "6401": "Footwear", "6402": "Footwear", "6403": "Footwear", "6404": "Footwear", "6405": "Footwear", "6406": "Footwear",
        "6101": "Apparel", "6102": "Apparel", "6103": "Apparel", "6104": "Apparel", "6105": "Apparel", "6106": "Apparel",
        "6107": "Apparel", "6108": "Apparel", "6109": "Apparel", "6110": "Apparel", "6111": "Apparel", "6112": "Apparel",
        "6113": "Apparel", "6114": "Apparel", "6115": "Apparel", "6116": "Apparel", "6117": "Apparel",
        "6201": "Apparel", "6202": "Apparel", "6203": "Apparel", "6204": "Apparel", "6205": "Apparel", "6206": "Apparel",
        "6207": "Apparel", "6208": "Apparel", "6209": "Apparel", "6210": "Apparel", "6211": "Apparel", "6212": "Apparel",
        "6213": "Apparel", "6214": "Apparel", "6215": "Apparel", "6216": "Apparel", "6217": "Apparel",
        "4202": "Accessories", "4203": "Accessories", "6505": "Accessories", "6506": "Accessories",
        "9506": "Accessories", "6307": "Accessories", "4911": "Accessories", "9603": "Accessories",
    }

    duties_df["product_line"] = duties_df["hts_chapter"].map(HTS_PRODUCT_LINE).fillna("Other")
    _product_lines = ["All"] + sorted(duties_df["product_line"].unique().tolist())

    sim_col1, sim_col2, sim_col3, sim_col4 = st.columns(4)
    with sim_col1:
        sim_product_line = st.selectbox("Product Line", options=_product_lines, key="sim_product")
    with sim_col2:
        _sim_duties = duties_df if sim_product_line == "All" else duties_df[duties_df["product_line"] == sim_product_line]
        _sim_gen = _sim_duties[_sim_duties["tariff_type_cd"] == "GEN"]
        _sim_lane_values = _sim_gen.groupby(["country_of_origin_cd", "country_of_destination_cd"]).agg(
            goods_value_usd=("goods_value_usd", "sum"),
        ).reset_index()
        _sim_lane_values["origin_name"] = _sim_lane_values["country_of_origin_cd"].map(
            dict(zip(lanes["country_of_origin_cd"], lanes["origin_name"]))
        ).fillna(_sim_lane_values["country_of_origin_cd"])
        _sim_active = _sim_lane_values[_sim_lane_values["goods_value_usd"] > 0].sort_values("goods_value_usd", ascending=False)
        origin_options = _sim_active["origin_name"].unique().tolist() if not _sim_active.empty else lanes["origin_name"].unique().tolist()
        all_origins_with_stp = stp_df["origin_name"].unique().tolist()
        from_origin = st.selectbox("Shift FROM", options=origin_options, key="sim_from")
    with sim_col3:
        to_options = [o for o in sorted(set(origin_options + all_origins_with_stp)) if o != from_origin]
        to_origin = st.selectbox("Shift TO", options=to_options, key="sim_to")
    with sim_col4:
        shift_pct = st.slider("% of Volume to Shift", min_value=5, max_value=100, value=25, step=5, key="sim_pct") / 100.0

    if sim_product_line != "All":
        _pl_value = _sim_gen["goods_value_usd"].sum()
        _pl_rows = int(_sim_gen["row_count"].sum()) if "row_count" in _sim_gen.columns else len(_sim_gen)
        st.caption(f"Filtered to **{sim_product_line}**: {_pl_rows:,} GEN line items, {fmt_usd(_pl_value)} goods value")

    from_cd = {v: k for k, v in COUNTRY_NAMES.items()}.get(from_origin, from_origin)
    to_cd = {v: k for k, v in COUNTRY_NAMES.items()}.get(to_origin, to_origin)

    if sim_product_line != "All":
        _sim_from_lanes = _sim_lane_values[_sim_lane_values["country_of_origin_cd"] == from_cd].copy()
        if not _sim_from_lanes.empty:
            _sim_from_lanes["dest_name"] = _sim_from_lanes["country_of_destination_cd"].map(
                dict(zip(lanes["country_of_destination_cd"], lanes["dest_name"]))
            ).fillna(_sim_from_lanes["country_of_destination_cd"])
            from_lanes = _sim_from_lanes
        else:
            from_lanes = pd.DataFrame()
    else:
        from_lanes = lanes[lanes["country_of_origin_cd"] == from_cd].copy()

    if from_lanes.empty:
        st.warning(f"No duty data found for {from_origin}{' in ' + sim_product_line if sim_product_line != 'All' else ''}.")
    else:
        from real_data_loader import GEN_DUTY_RATES, STP_DUTY_RATES

        # Build a lookup of rates from the computed lanes dataframe (has Databricks-live rates)
        _lane_rate_lookup = {
            r["country_of_origin_cd"]: (r["gen_rate"], r["stp_rate"], r["has_stp"])
            for _, r in lanes.iterrows()
        }

        sim_results = []
        for _, lane in from_lanes.iterrows():
            dest_cd = lane["country_of_destination_cd"]
            dest_name = lane["dest_name"]
            shifted_value = lane["goods_value_usd"] * shift_pct

            _full_lane = lanes[(lanes["country_of_origin_cd"] == from_cd) & (lanes["country_of_destination_cd"] == dest_cd)]
            before_util = _full_lane["utilization_pct"].iloc[0] if not _full_lane.empty and "utilization_pct" in _full_lane.columns else 0.0
            _before_progs = _full_lane["stp_programs"].iloc[0] if not _full_lane.empty and "stp_programs" in _full_lane.columns else []
            before_stp = ", ".join(_before_progs[:2]) if _before_progs else "None"

            # Prefer rates from lanes df (Databricks-live), fall back to dict
            _from_rates = _lane_rate_lookup.get(from_cd)
            before_gen_rate = _from_rates[0] if _from_rates else GEN_DUTY_RATES.get(from_cd, 0.25)
            _from_has_stp = _from_rates[2] if _from_rates else not stp_df[
                (stp_df["country_of_origin_cd"] == from_cd) & (stp_df["country_of_destination_cd"] == dest_cd)
            ].empty
            before_stp_rate = (_from_rates[1] if _from_rates else STP_DUTY_RATES.get(from_cd, before_gen_rate)) if _from_has_stp else before_gen_rate

            to_stp = stp_df[
                (stp_df["country_of_origin_cd"] == to_cd)
                & (stp_df["country_of_destination_cd"] == dest_cd)
            ]
            _to_rates = _lane_rate_lookup.get(to_cd)
            after_gen_rate = _to_rates[0] if _to_rates else GEN_DUTY_RATES.get(to_cd, 0.25)
            after_stp_rate = (_to_rates[1] if _to_rates else STP_DUTY_RATES.get(to_cd, after_gen_rate)) if not to_stp.empty else after_gen_rate
            if not to_stp.empty:
                after_stp = ", ".join(to_stp["stp_cd"].unique()[:2])
                after_qual = to_stp["qualification_rate"].mean()
                to_lane = lanes[
                    (lanes["country_of_origin_cd"] == to_cd)
                    & (lanes["country_of_destination_cd"] == dest_cd)
                ]
                after_util = to_lane["utilization_pct"].iloc[0] if not to_lane.empty else 0.0
            else:
                after_stp = "None"
                after_qual = 0.0
                after_util = 0.0

            # FTA rule intelligence — use lane's actual program for "Before",
            # theoretical coverage for "After" (simulating a shift)
            _before_fta_name = "—"
            _before_fta_diff = "—"
            if _before_progs and _fta_agreements:
                for _bp in _before_progs:
                    if _bp in _fta_agreements:
                        _binfo = _summarize_rule_for_lane(_fta_agreements[_bp])
                        _before_fta_name = _fta_agreements[_bp].name
                        _before_fta_diff = _binfo["difficulty"]
                        break
                if _before_fta_name == "—" and _before_progs:
                    _before_fta_name = _before_progs[0]

            _to_fta = get_fta_info_for_origin(to_cd, dest_cd, _fta_agreements) if _fta_agreements else {}
            _after_fta_diff = _to_fta.get("difficulty", "—")


            # Blended before duty: portion already at STP rate + remainder at GEN rate
            before_effective_rate = (before_util * before_stp_rate) + ((1 - before_util) * before_gen_rate)
            before_duty = shifted_value * before_effective_rate

            # After duty (theoretical max): apply qualification rate
            after_effective_rate = (after_qual * after_stp_rate) + ((1 - after_qual) * after_gen_rate)
            after_duty_max = shifted_value * after_effective_rate

            duty_delta = after_duty_max - before_duty

            sim_results.append({
                "Destination": dest_name,
                "Shifted Value": shifted_value,
                "Before STP": before_stp,
                "Before Utilization": before_util,
                "Before FTA": _before_fta_name,
                "Before Difficulty": _before_fta_diff,
                "After STP": after_stp,
                "After Qualification": after_qual,
                "After Utilization": after_util,
                "After FTA": _to_fta.get("fta_name", "—"),
                "After Difficulty": _after_fta_diff,
                "After Key Rule": _to_fta.get("rule_summary", "—"),
                "Before Duty": before_duty,
                "After Duty": after_duty_max,
                "Duty Delta": duty_delta,
            })

        sim_df = pd.DataFrame(sim_results)

        if not sim_df.empty:
            total_delta = sim_df["Duty Delta"].sum()
            total_shifted = sim_df["Shifted Value"].sum()

            sim_m1, sim_m2, sim_m3, sim_m4 = st.columns(4)
            sim_m1.metric("Volume Shifted", fmt_usd(total_shifted))
            sim_m2.metric("Net Duty Impact", fmt_usd(total_delta))
            new_programs = sim_df[sim_df["After STP"] != "None"]["After STP"].nunique()
            lost_programs = sim_df[(sim_df["Before STP"] != "None") & (sim_df["After STP"] == "None")].shape[0]
            sim_m3.metric("New STP Access", f"{new_programs} programs")
            sim_m4.metric("STP Exposure Risk", f"{lost_programs} lanes lose STP")

            display_sim = sim_df.copy()
            display_sim["Shifted Value"] = display_sim["Shifted Value"].apply(lambda x: f"${x:,.0f}")
            display_sim["Before Utilization"] = display_sim["Before Utilization"].apply(lambda x: f"{x:.1%}")
            display_sim["After Qualification"] = display_sim["After Qualification"].apply(lambda x: f"{x:.1%}")
            display_sim["After Utilization"] = display_sim["After Utilization"].apply(lambda x: f"{x:.1%}")
            display_sim["Before Duty"] = display_sim["Before Duty"].apply(lambda x: f"${x:,.0f}")
            display_sim["After Duty"] = display_sim["After Duty"].apply(lambda x: f"${x:,.0f}")
            display_sim["Duty Delta"] = display_sim["Duty Delta"].apply(
                lambda x: f"-${abs(x):,.0f}" if x < 0 else f"+${x:,.0f}"
            )

            _sim_display_cols = [
                "Destination", "Shifted Value",
                "Before STP", "Before FTA", "Before Difficulty", "Before Utilization",
                "After STP", "After FTA", "After Difficulty", "After Key Rule", "After Qualification",
                "Before Duty", "After Duty", "Duty Delta",
            ]
            _sim_display_cols = [c for c in _sim_display_cols if c in display_sim.columns]

            st.dataframe(display_sim[_sim_display_cols], use_container_width=True, hide_index=True)

            _sorted_sim = sim_df.sort_values("Duty Delta")
            fig_sim = px.bar(
                _sorted_sim,
                x="Destination", y="Duty Delta",
                color=_sorted_sim["Duty Delta"].apply(
                    lambda x: "Savings" if x < 0 else "Increase"
                ),
                color_discrete_map={"Savings": SAVINGS_GREEN, "Increase": GAP_RED},
                text=_sorted_sim["Duty Delta"].apply(
                    lambda x: f"-{fmt_usd(abs(x))}" if x < 0 else f"+{fmt_usd(x)}"
                ),
            )
            fig_sim.update_layout(
                height=350, margin=dict(t=20, b=30),
                plot_bgcolor="white", yaxis_title="Duty Impact ($)",
                legend_title="", showlegend=True,
            )
            fig_sim.update_traces(textposition="outside")
            st.plotly_chart(fig_sim, use_container_width=True)

            _to_fta_overall = get_fta_info_for_origin(to_cd, "US", _fta_agreements) if _fta_agreements else {}
            _to_diff = _to_fta_overall.get("difficulty", "—")
            _to_rule = _to_fta_overall.get("rule_summary", "")
            _to_fta_name = _to_fta_overall.get("fta_name", "None")
            _fta_context = ""
            if _to_fta_name != "None" and _to_rule:
                _diff_color = SAVINGS_GREEN if _to_diff == "Easy" else (NIKE_ORANGE if _to_diff == "Moderate" else GAP_RED)
                _fta_context = (
                    f'<br><strong>FTA qualification at {to_origin}:</strong> {_to_fta_name} — '
                    f'<span style="color:{_diff_color};font-weight:600;">{_to_diff}</span> '
                    f'({_to_rule})'
                )
            elif _to_fta_name == "None":
                _fta_context = f'<br><strong>FTA availability at {to_origin} &rarr; US:</strong> <span style="color:{GAP_RED};">No FTA available</span> — MFN rates only.'

            st.markdown(f"""
            <div class="insight-box">
                <strong>Interpretation:</strong> Shifting {shift_pct:.0%} of volume from {from_origin} to {to_origin}
                would {"<strong style='color:{0}'>save {1}/year</strong> by gaining access to preferential STP rates".format(SAVINGS_GREEN, fmt_usd(abs(total_delta))) if total_delta < 0 else "<strong style='color:{0}'>increase duties by {1}/year</strong> — the destination STP coverage is weaker".format(GAP_RED, fmt_usd(abs(total_delta)))}.
                {_fta_context}
            </div>
            """, unsafe_allow_html=True)


# ======================================================================
# TAB 4: UTILIZATION BY LANE
# ======================================================================
with tab_util:

    st.markdown('<div class="section-header">STP Utilization Heatmap: Origin x Destination</div>', unsafe_allow_html=True)

    heatmap_lanes = stp_lanes[(stp_lanes["goods_value_usd"] > 0) & (stp_lanes["eligible_pct"] >= 0.01)].copy()
    if not heatmap_lanes.empty:
        heat_pivot = heatmap_lanes.pivot_table(
            index="origin_name",
            columns="dest_name",
            values="utilization_pct",
            aggfunc="mean",
        ).fillna(-1) * 100

        mask_text = heat_pivot.copy()
        mask_text = mask_text.applymap(lambda x: f"{x:.0f}%" if x >= 0 else "")

        fig_heat = px.imshow(
            heat_pivot.values,
            x=heat_pivot.columns.tolist(),
            y=heat_pivot.index.tolist(),
            color_continuous_scale=[[0, GAP_RED], [0.5, NIKE_ORANGE], [1.0, SAVINGS_GREEN]],
            aspect="auto",
            zmin=0, zmax=100,
        )
        fig_heat.update_traces(
            text=mask_text.values,
            texttemplate="%{text}",
        )
        fig_heat.update_layout(
            height=max(400, len(heat_pivot) * 40),
            margin=dict(t=20, b=20),
            xaxis_title="Destination",
            yaxis_title="Origin",
            coloraxis_colorbar_title="Utilization %",
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    # ── Utilization ranking ──
    st.markdown('<div class="section-header">Lane Utilization Ranking</div>', unsafe_allow_html=True)

    ranking = stp_lanes[stp_lanes["goods_value_usd"] > 0].sort_values("utilization_pct").copy()
    if not ranking.empty:
        # Only count gap lanes with actual eligible products for the summary
        ranking_effective = ranking[ranking["eligible_pct"] >= 0.01]
        ranking_gap = ranking_effective[ranking_effective["is_gap_lane"]]
        ranking_gap_excess = ranking_gap["excess_duty_usd"].sum()

        # Separate lanes into eligible vs product-excluded for clearer display
        _eligible_ranking = ranking[ranking["eligible_pct"] >= 0.01]
        _excluded_ranking = ranking[ranking["eligible_pct"] < 0.01]

        if not _eligible_ranking.empty:
            for _, lane in _eligible_ranking.iterrows():
                util = lane["utilization_pct"] * 100
                if util >= 75:
                    color = SAVINGS_GREEN
                    status = "High"
                elif util >= 25:
                    color = NIKE_ORANGE
                    status = "Moderate"
                else:
                    color = GAP_RED
                    status = "Low/None"

                progs = ", ".join(lane["stp_programs"][:3]) if lane["stp_programs"] else "—"
                qual = lane["stp_qualification_rate"]

                _lane_fta = lane.get("fta_name", "—") if "fta_name" in lane.index else "—"
                _lane_diff = lane.get("fta_difficulty", "—") if "fta_difficulty" in lane.index else "—"
                _lane_rule = lane.get("fta_rule_summary", "") if "fta_rule_summary" in lane.index else ""
                _diff_color_map = {"Easy": SAVINGS_GREEN, "Moderate": NIKE_ORANGE, "Hard": GAP_RED}
                _dc = _diff_color_map.get(_lane_diff, NIKE_GRAY)

                _fta_line = ""
                if _lane_fta and _lane_fta != "—" and _lane_fta != "None":
                    _fta_line = (
                        f' &middot; <span style="color:{_dc};font-weight:600;">{_lane_diff}</span>'
                        f' ({_lane_fta}: {_lane_rule})'
                    )
                elif _lane_fta == "None":
                    _fta_line = f' &middot; <span style="color:{GAP_RED};">No FTA</span>'

                _elig_note = ""
                if lane["eligible_pct"] < 1.0:
                    _elig_note = f' &middot; <span style="color:{NIKE_ORANGE};">{lane["eligible_pct"]:.0%} products eligible</span>'

                st.markdown(f"""
                <div style="background:white; border-radius:8px; padding:14px 18px; margin-bottom:6px;
                            border:1px solid #E5E5E5; border-left:4px solid {color};
                            display:flex; justify-content:space-between; align-items:center;">
                    <div style="flex:2;">
                        <span style="font-weight:600;">{lane['origin_name']} &rarr; {lane['dest_name']}</span>
                        <div style="font-size:12px; color:#757575; margin-top:2px;">
                            {progs} &middot; {lane['stp_total_products']:,.0f} products &middot;
                            Qualification: {qual:.0%} &middot;
                            Goods Value: {fmt_usd(lane['goods_value_usd'], 0)}{_elig_note}{_fta_line}
                        </div>
                    </div>
                    <div style="text-align:right; min-width:120px;">
                        <div style="font-size:22px; font-weight:700; color:{color};">{util:.1f}%</div>
                        <div style="font-size:11px; color:#757575;">{status} utilization</div>
                    </div>
                    <div style="text-align:right; min-width:140px;">
                        <div style="font-size:16px; font-weight:600; color:{GAP_RED if lane['excess_duty_usd'] > 0 else NIKE_GRAY};">
                            {fmt_usd(lane['excess_duty_usd'], 0)}
                        </div>
                        <div style="font-size:11px; color:#757575;">excess duty</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown(f"""
        <div style="background:#F5F5F5; border-radius:8px; padding:14px 18px; margin-top:4px;
                    border:2px solid #333; display:flex; justify-content:space-between; align-items:center;">
            <div style="flex:2;">
                <span style="font-weight:700; font-size:15px;">Total — {len(ranking_gap)} Gap Lanes (&lt;50% utilization, eligible products)</span>
            </div>
            <div style="text-align:right; min-width:120px;"></div>
            <div style="text-align:right; min-width:140px;">
                <div style="font-size:18px; font-weight:700; color:{GAP_RED};">
                    {fmt_usd(ranking_gap_excess, 0)}
                </div>
                <div style="font-size:11px; color:#757575;">excess duty on gap lanes</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        if not _excluded_ranking.empty:
            st.markdown('<div class="section-header" style="margin-top:24px;">Product-Excluded Lanes (HS Chapter Exclusions)</div>', unsafe_allow_html=True)
            st.markdown("""
            <div class="insight-box" style="margin-bottom:12px;">
                These lanes have STP/FTA program eligibility data, but <strong>0% of current products qualify</strong>
                because they fall into excluded HS chapters (e.g., US GSP excludes textiles Ch 50-63 and footwear Ch 64).
                Factory qualification rates are shown for reference but no duty savings are available under the current product mix.
            </div>
            """, unsafe_allow_html=True)

            for _, lane in _excluded_ranking.iterrows():
                progs = ", ".join(lane["stp_programs"][:3]) if lane["stp_programs"] else "—"
                qual = lane["stp_qualification_rate"]
                st.markdown(f"""
                <div style="background:#FAFAFA; border-radius:8px; padding:14px 18px; margin-bottom:6px;
                            border:1px solid #E0E0E0; border-left:4px solid {NIKE_GRAY};
                            display:flex; justify-content:space-between; align-items:center; opacity:0.75;">
                    <div style="flex:2;">
                        <span style="font-weight:600;">{lane['origin_name']} &rarr; {lane['dest_name']}</span>
                        <div style="font-size:12px; color:#757575; margin-top:2px;">
                            {progs} &middot; {lane['stp_total_products']:,.0f} products &middot;
                            Qualification: {qual:.0%} &middot;
                            Goods Value: {fmt_usd(lane['goods_value_usd'], 0)} &middot;
                            <span style="color:{GAP_RED};">0% products eligible</span>
                        </div>
                    </div>
                    <div style="text-align:right; min-width:120px;">
                        <div style="font-size:16px; font-weight:600; color:{NIKE_GRAY};">N/A</div>
                        <div style="font-size:11px; color:#757575;">products excluded</div>
                    </div>
                    <div style="text-align:right; min-width:120px;">
                        <div style="font-size:16px; font-weight:600; color:{NIKE_GRAY};">$0</div>
                        <div style="font-size:11px; color:#757575;">excess duty</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)


# ======================================================================
# TAB 5: PROGRAM DEEP DIVE
# ======================================================================
with tab_programs:

    st.markdown(f'<div class="section-header">{meta["unique_programs"]} Trade Programs in STP Eligibility Data</div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="insight-box">
        <strong>From the real data:</strong> The STP eligibility extract from <code>trade_customs</code> contains
        <strong>{meta["unique_programs"]} distinct trade programs</strong> across {meta["unique_origins"]} origin countries
        and {meta["unique_destinations"]} destination countries. These programs have product-lane eligibility records —
        the qualification rate reflects what percentage of products meet Rules of Origin criteria in the data.
        <br><br>
        <em>Note: "Active" means the program has eligibility records in the data extract. It does not confirm
        the program is currently being claimed at customs or actively managed in Trade Automation.</em>
    </div>
    """, unsafe_allow_html=True)

    sorted_programs = programs_df.sort_values("total_products", ascending=False)

    _prog_show_all = st.checkbox("Show all programs", value=False, key="prog_show_all")
    _prog_display = sorted_programs if _prog_show_all else sorted_programs.head(25)

    if not _prog_show_all and len(sorted_programs) > 25:
        st.caption(f"Showing top 25 of {len(sorted_programs)} programs by product count. Check the box above to see all.")

    # Map STP program codes to FTA codes for rule lookup
    _STP_TO_FTA = {
        "CAFTA_DR": "CAFTA_DR", "CAFTA-DR": "CAFTA_DR",
        "USMCA": "USMCA", "US_GSP": "US_GSP",
        "CPTPP": "CPTPP", "JO_US_FTA": "JO_US_FTA",
        "EU_GSP": "EU_GSP", "EU_VN": "EU_VN",
        "EU_JP": "EU_JP", "GB_GSP": "GB_GSP",
        "GB_VN": "GB_VN", "ACFTA": "ACFTA",
        "AIFTA": "AIFTA", "AKFTA": "AKFTA",
        "AJCEP": "AJCEP", "ASEAN": "ASEAN",
        "AANZFTA": "AANZFTA", "APTA": "APTA",
        "IJEPA": "IJEPA", "JP_GSP": "JP_GSP",
    }

    for _, prog in _prog_display.iterrows():
        qual_rate = prog["avg_qualification_rate"] * 100
        if qual_rate >= 90:
            color = SAVINGS_GREEN
            status = "High Qualification"
        elif qual_rate >= 70:
            color = NIKE_ORANGE
            status = "Moderate Qualification"
        else:
            color = GAP_RED
            status = "Low Qualification"

        not_met_pct = prog["not_met"] / (prog["met"] + prog["not_met"]) * 100 if (prog["met"] + prog["not_met"]) > 0 else 0

        # FTA rule enrichment
        _fta_code = _STP_TO_FTA.get(prog["stp_cd"], prog["stp_cd"])
        _fta_obj = _fta_agreements.get(_fta_code) if _fta_agreements else None
        _roo_html = ""
        if _fta_obj:
            _info = _summarize_rule_for_lane(_fta_obj)
            _dc = {"Easy": SAVINGS_GREEN, "Moderate": NIKE_ORANGE, "Hard": GAP_RED}.get(_info["difficulty"], NIKE_GRAY)
            _ch_list = ", ".join(f"Ch {c}" for c in sorted(_fta_obj.chapters.keys()))
            _roo_html = (
                f'<div style="margin-top:10px; padding:8px 12px; background:#F8F9FA; border-radius:6px; font-size:12px;">'
                f'<strong>Rules of Origin:</strong> '
                f'<span style="color:{_dc};font-weight:600;">{_info["difficulty"]}</span> — '
                f'{_info["rule_summary"]} &middot; '
                f'{_ch_list} &middot; {_fta_obj.total_spec_count} rule specs'
                f'</div>'
            )

        st.markdown(f"""
        <div style="background:white; border-radius:12px; padding:20px; margin-bottom:10px;
                    border:1px solid #E5E5E5; border-left:4px solid {color};">
            <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                <div style="flex:2;">
                    <span style="font-size:16px; font-weight:600;">{prog['program_name']}</span>
                    <span style="background:{color}20; color:{color}; font-size:11px;
                                 padding:2px 8px; border-radius:8px; margin-left:8px;">{status}</span>
                    <span style="background:#EEE; font-size:11px; padding:2px 8px;
                                 border-radius:8px; margin-left:4px;">{prog['stp_cd']}</span>
                    <div style="font-size:13px; color:#757575; margin-top:4px;">
                        {prog['origins']} origin countries &middot;
                        {prog['destinations']} destinations &middot;
                        {prog['lane_count']} lane configurations
                    </div>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:24px; font-weight:700; color:{color};">{qual_rate:.0f}%</div>
                    <div style="font-size:12px; color:#757575;">qualification rate</div>
                </div>
            </div>
            <div style="display:flex; gap:32px; margin-top:12px;">
                <div>
                    <div style="font-size:12px; color:#757575;">Products</div>
                    <div style="font-size:16px; font-weight:600;">{prog['total_products']:,}</div>
                </div>
                <div>
                    <div style="font-size:12px; color:#757575;">Factories</div>
                    <div style="font-size:16px; font-weight:600;">{prog['total_factories']:,}</div>
                </div>
                <div>
                    <div style="font-size:12px; color:#757575;">Qualified (Met)</div>
                    <div style="font-size:16px; font-weight:600; color:{SAVINGS_GREEN};">{prog['met']:,}</div>
                </div>
                <div>
                    <div style="font-size:12px; color:#757575;">Not Met</div>
                    <div style="font-size:16px; font-weight:600; color:{GAP_RED};">{prog['not_met']:,}</div>
                </div>
                <div>
                    <div style="font-size:12px; color:#757575;">Incomplete</div>
                    <div style="font-size:16px; font-weight:600; color:{NIKE_ORANGE};">{prog['incomplete']:,}</div>
                </div>
            </div>
            {_roo_html}
        </div>
        """, unsafe_allow_html=True)

    # Program qualification chart
    st.markdown('<div class="section-header">Qualification Rate by Program</div>', unsafe_allow_html=True)

    top_progs = sorted_programs.head(20).copy()
    fig_prog = go.Figure()
    fig_prog.add_trace(go.Bar(
        y=top_progs["program_name"],
        x=top_progs["met"],
        name="Qualified (Met)",
        orientation="h",
        marker_color=SAVINGS_GREEN,
    ))
    fig_prog.add_trace(go.Bar(
        y=top_progs["program_name"],
        x=top_progs["not_met"],
        name="Not Met",
        orientation="h",
        marker_color=GAP_RED,
    ))
    fig_prog.add_trace(go.Bar(
        y=top_progs["program_name"],
        x=top_progs["incomplete"],
        name="Incomplete",
        orientation="h",
        marker_color=NIKE_ORANGE,
    ))
    fig_prog.update_layout(
        barmode="stack",
        height=max(400, len(top_progs) * 35),
        margin=dict(t=20, b=20, l=280),
        xaxis_title="Product-Lane Records",
        plot_bgcolor="white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig_prog, use_container_width=True)


# ======================================================================
# TAB 6: DATA PIPELINE & LANDSCAPE
# ======================================================================
with tab_pipeline:

    from data import DATA_PIPELINE_STAGES, COMPANION_PROGRAMS

    st.markdown('<div class="section-header">STP Data Pipeline (Trade Automation &rarr; Broker)</div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="insight-box">
        <strong>Where Foundation Data fits:</strong> Our team owns <strong>two key stages</strong>:
        <strong>Trade Automation (Amber Road/E2Open)</strong> data — which is also an input into ILM — and
        the <strong>NDF (Databricks/SOLE)</strong> foundational data layer. This POC is built entirely
        from data in our domain: <code>non_published_domain.trade_customs</code>.
    </div>
    """, unsafe_allow_html=True)

    pipeline_cols = st.columns(len(DATA_PIPELINE_STAGES))
    for i, stage in enumerate(DATA_PIPELINE_STAGES):
        is_ours = stage["stage"] in ("NDF", "Trade Automation")
        border_color = NIKE_ORANGE if is_ours else "#E5E5E5"
        bg_color = "#FFF8F0" if is_ours else "white"
        badge = ' <span style="background:#FA5400;color:white;font-size:10px;padding:2px 6px;border-radius:8px;">OUR DOMAIN</span>' if is_ours else ""

        with pipeline_cols[i]:
            st.markdown(f"""
            <div style="background:{bg_color}; border:2px solid {border_color}; border-radius:12px;
                        padding:16px; text-align:center; min-height:180px;">
                <div style="font-size:14px; font-weight:700; color:#111; margin-bottom:8px;">
                    {stage['stage']}{badge}
                </div>
                <div style="font-size:11px; color:#757575; margin-bottom:8px;">
                    {stage['system']}
                </div>
                <div style="font-size:12px; color:#555;">
                    {stage['description']}
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("")

    st.markdown(f"""
    <div class="gap-highlight">
        <strong>Pipeline leakage points (confirmed by real data):</strong>
        <ul style="margin:8px 0 0 0; padding-left:20px; font-size:13px;">
            <li><strong>Trade Automation (our data):</strong> {meta['stp_total_records']:,} eligibility records show {stp_df['not_met'].sum():,} product-lanes that don't meet Rules of Origin</li>
            <li><strong>NDF (our layer):</strong> Data exists in <code>non_published_domain.trade_customs</code> but wasn't assembled to show the gap until this POC</li>
            <li><strong>CCH &rarr; Broker:</strong> Even when products qualify (97% for KH&rarr;US), only 0.1% of declarations claim preferential treatment</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

    # ── Companion Programs Landscape ──
    st.markdown('<div class="section-header">Complete Duty Savings Landscape</div>', unsafe_allow_html=True)

    _stp_gap_total = meta["total_savings_potential"]

    landscape_cols = st.columns(4)
    with landscape_cols[0]:
        st.markdown(f"""
        <div class="metric-card" style="border-top: 4px solid {GAP_RED};">
            <div class="metric-label">STP/FTA Utilization Gap</div>
            <div class="metric-value" style="color:{GAP_RED};">{fmt_usd(_stp_gap_total)}</div>
            <div class="metric-delta negative">Estimated from FY26 customs data</div>
            <div style="font-size:12px; color:#757575; margin-top:8px;">
                FY27 target: $50M savings &middot; <strong>This tool's focus</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with landscape_cols[1]:
        fs = COMPANION_PROGRAMS["first_sale"]
        st.markdown(f"""
        <div class="metric-card" style="border-top: 4px solid {SAVINGS_GREEN};">
            <div class="metric-label">{fs['name']}</div>
            <div class="metric-value" style="color:{SAVINGS_GREEN};">{fmt_usd(fs['current_savings'], 0)}</div>
            <div class="metric-delta positive">Currently saving &middot; expanding to {fmt_usd(fs['expansion_target'], 0)}</div>
        </div>
        """, unsafe_allow_html=True)

    with landscape_cols[2]:
        dd = COMPANION_PROGRAMS["duty_drawback"]
        st.markdown(f"""
        <div class="metric-card" style="border-top: 4px solid {NIKE_ORANGE};">
            <div class="metric-label">{dd['name']}</div>
            <div class="metric-value" style="color:{NIKE_ORANGE};">{fmt_usd(dd['current_savings'], 0)}</div>
            <div class="metric-delta">Small scale &middot; returns only</div>
        </div>
        """, unsafe_allow_html=True)

    with landscape_cols[3]:
        ftz = COMPANION_PROGRAMS["ftz"]
        st.markdown(f"""
        <div class="metric-card" style="border-top: 4px solid {NIKE_GRAY};">
            <div class="metric-label">{ftz['name']}</div>
            <div class="metric-value" style="color:{NIKE_GRAY};">$0M</div>
            <div class="metric-delta">Not currently pursued</div>
        </div>
        """, unsafe_allow_html=True)

    # ── Path to Production ──
    st.markdown('<div class="section-header">Path to Production</div>', unsafe_allow_html=True)

    roadmap = [
        ("Phase 1: POC Validation", "Share with Taruna, Chris McCollister (SC Analytics), James (Digital Twin)", NIKE_ORANGE, "Current"),
        ("Phase 2: Full Data Pull", "Remove LIMIT, add EU/EMEA declarations, add date-filtered duty data", NEUTRAL_BLUE, "Next"),
        ("Phase 3: Root Cause Layer", "Join with trade_document_reference for CoO filing status per declaration", NEUTRAL_BLUE, "Next"),
        ("Phase 4: Embed in S.C.O.P.E.", "Position as the STP/FTA layer that S.C.O.P.E.'s scenario builder lacks", NIKE_GRAY, "Future"),
        ("Phase 5: Gold Layer KPIs", "Surface STP utilization, excess duty, CoO compliance as governed KPIs", NIKE_GRAY, "Future"),
    ]

    for title, desc, color, timing in roadmap:
        st.markdown(f"""
        <div style="background:white; border-radius:8px; padding:14px 18px; margin-bottom:8px;
                    border:1px solid #E5E5E5; border-left:4px solid {color};
                    display:flex; justify-content:space-between; align-items:center;">
            <div>
                <span style="font-weight:600;">{title}</span>
                <div style="font-size:13px; color:#757575; margin-top:2px;">{desc}</div>
            </div>
            <div style="background:{color}20; color:{color}; font-size:11px; font-weight:600;
                        padding:3px 10px; border-radius:8px;">{timing}</div>
        </div>
        """, unsafe_allow_html=True)


# ── Footer ───────────────────────────────────────────────────────────
st.divider()
st.caption(
    f"**STP/FTA Duty Savings Simulator** · POC · Foundation Data Technology · "
    f"Real data from non_published_domain.trade_customs · "
    f"{meta['stp_total_records']:,} STP records · {meta['duties_total_records']:,} duty declarations · "
    f"Last ETL: {meta['last_etl']}"
)
