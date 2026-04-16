"""
Live data loader — queries Databricks SQL directly.
Optimised for performance: all heavy aggregation runs server-side so we
transfer small result-sets instead of millions of raw rows.

Uses published_domain.trade_customs views (_v suffix) which provide broader
data coverage than the raw non_published tables. Goods value sourced from
commodity_tariff_v.PAYABLE_PRC (GEN tariff type) with CUSTOMS_AMT_USD fallback.

Workspace: nike-sole-react.cloud.databricks.com
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# Fix Windows stdout encoding for Streamlit
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

# Published views (broader coverage, UPPERCASE columns, UUID join keys)
PUB = "published_domain.trade_customs"
# Raw tables (used only for STP eligibility which has no published _v view)
TC = "non_published_domain.trade_customs"
PSC_WINDOW_DAYS = 180

FY_START = "2025-06-01"
FY_END = "2026-05-31"

# Databricks Apps service principal auth (auto-injected by the runtime)
_USE_SERVICE_PRINCIPAL = not DATABRICKS_TOKEN


# ── connection helpers ───────────────────────────────────────────────

def is_configured() -> bool:
    if _USE_SERVICE_PRINCIPAL:
        return bool(DATABRICKS_HTTP_PATH)
    return bool(DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN)


def _get_connection():
    from databricks import sql as dbsql

    if _USE_SERVICE_PRINCIPAL:
        from databricks.sdk.core import Config
        cfg = Config()
        hostname = cfg.host
        if hostname.startswith("https://"):
            hostname = hostname[len("https://"):]
        elif hostname.startswith("http://"):
            hostname = hostname[len("http://"):]
        return dbsql.connect(
            server_hostname=hostname,
            http_path=DATABRICKS_HTTP_PATH,
            credentials_provider=lambda: cfg.authenticate,
        )

    return dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def _run_query(sql: str) -> pd.DataFrame:
    with _get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
    return pd.DataFrame(rows, columns=cols)


def test_connection() -> tuple[bool, str]:
    if not is_configured():
        return False, "Databricks not configured — set DATABRICKS_TOKEN (local) or deploy as Databricks App"
    try:
        _run_query("SELECT 1 AS ok")
        mode = "service principal" if _USE_SERVICE_PRINCIPAL else "PAT"
        return True, f"Connected to nike-sole-react ({mode} auth)"
    except Exception as e:
        import traceback
        return False, f"{e}\n{traceback.format_exc()}"


from real_data_loader import (
    PROGRAM_NAMES, COUNTRY_NAMES, GEO_REGIONS,
    GEN_DUTY_RATES, STP_DUTY_RATES,
    build_program_summary,
    _to_nike_fy, _to_nike_fq,
)


# ── 1. Duty rates (server-side GROUP BY — ~3 s) ─────────────────────

# FY26 tariff surcharges layered on top of MFN base rates.
# Sources: White House executive orders, Supreme Court IEEPA ruling,
#          Section 122 (15% universal, effective Feb 24 2026, expires Jul 24 2026),
#          Section 301 (7.5% List 4A for China apparel/footwear),
#          IEEPA reciprocal tariffs (Apr 2025 – Feb 2026, struck down by SCOTUS).
#
# FY26 timeline:
#   Jun–Aug 2025: 10% baseline reciprocal tariff (IEEPA)
#   Aug 2025–Feb 2026: country-specific reciprocal rates (IEEPA)
#     VN 46%, CN 34%+145%→30%, KH 49%, ID 32%, TH 36%, IN 26%, etc.
#   Feb 24 2026+: IEEPA struck down → 15% universal Section 122
#
# Since our FY26 data spans Jun 2025–May 2026, the effective surcharge
# is a blend of the IEEPA period and Section 122 period.
# Anupama's Trade Pulse confirms US blended rate: Jan 35.9%, Feb 30.5%.
# We use live MFN base (trade_estimated_duty_item Origin rates) +
# country-specific surcharges that produce rates consistent with public sources.
FY26_SURCHARGES = {
    "VN": 0.200,    # IEEPA 46% → Section 122 15%; effective avg ~+20pp
    "CN": 0.225,    # Section 301 7.5% + IEEPA 30% → Section 122 15%; avg ~+22.5pp
    "ID": 0.180,    # IEEPA 32% → Section 122 15%; avg ~+18pp
    "KH": 0.220,    # IEEPA 49% → Section 122 15%; avg ~+22pp
    "TH": 0.180,    # IEEPA 36% → Section 122 15%; avg ~+18pp
    "IN": 0.170,    # IEEPA 26% → Section 122 15%; avg ~+17pp
    "PK": 0.170,    # IEEPA 29% → Section 122 15%; avg ~+17pp
    "LK": 0.200,    # IEEPA 44% → Section 122 15%; avg ~+20pp
    "MY": 0.170,    # IEEPA 24% → Section 122 15%; avg ~+17pp
    "BD": 0.200,    # IEEPA 37% → Section 122 15%; avg ~+20pp
    "PH": 0.150,    # IEEPA 17% → Section 122 15%; avg ~+15pp
    "TR": 0.150,    # Section 122 15%
    "JO": 0.150,    # IEEPA 20% → Section 122 15%; avg ~+15pp
    "EG": 0.150,    # Section 122 15%
    "HN": 0.100,    # CAFTA-DR FTA + Section 122 15%; net ~+10pp
    "SV": 0.100,    # CAFTA-DR + Section 122 15%; net ~+10pp
    "GT": 0.100,    # CAFTA-DR + Section 122 15%; net ~+10pp
    "TW": 0.175,    # IEEPA 32% → Section 122 15%; avg ~+17.5pp
    "GE": 0.150,    # Section 122 15%
    "IT": 0.150,    # EU; Section 122 15%
    "NI": 0.150,    # IEEPA 18% → Section 122 15%; avg ~+15pp
    "SG": 0.150,    # Section 122 15% (Singapore FTA but Section 122 still applies)
    "HK": 0.150,    # Section 122 15%
    "JP": 0.150,    # IEEPA 24% → Section 122 15%; avg ~+15pp
    "BA": 0.150,    # IEEPA 35% → Section 122 15%; avg ~+15pp
    "KR": 0.150,    # IEEPA 25% → Section 122 15%; avg ~+15pp
    "MX": 0.000,    # USMCA; exempt from Section 122
}


def load_duty_rates() -> tuple[dict, dict]:
    """Average GEN and STP duty rates per origin country.

    GEN rates = live MFN base (from trade_estimated_duty_item Origin rates)
                + country-specific FY26 tariff surcharges.
    STP rates = live preferential rates from trade_estimated_duty_item STP rates.

    The Origin rates in the table are MFN tariff-schedule averages across
    Nike's HTS mix (~18-22% for most countries). Surcharges reflect IEEPA
    reciprocal tariffs (Jun-Feb FY26) and Section 122 (15%, Feb 24 2026+)."""
    sql = f"""
    SELECT
        country_of_origin_cd,
        duty_condition_type_nm,
        AVG(CAST(duty_rate AS DOUBLE)) AS avg_duty_rate
    FROM {TC}.trade_estimated_duty_item
    WHERE duty_rate IS NOT NULL
      AND duty_rate > 0
      AND country_of_origin_cd IS NOT NULL
    GROUP BY country_of_origin_cd, duty_condition_type_nm
    """
    df = _run_query(sql)
    df["avg_duty_rate"] = pd.to_numeric(df["avg_duty_rate"], errors="coerce").fillna(0)

    origin_base_rates = {}
    stp_rates = {}
    for _, row in df.iterrows():
        origin = row["country_of_origin_cd"]
        rate = float(row["avg_duty_rate"]) / 100.0
        if row["duty_condition_type_nm"] == "Origin":
            origin_base_rates[origin] = rate
        elif row["duty_condition_type_nm"] == "STP":
            stp_rates[origin] = rate

    gen_rates = {}
    for origin, mfn_base in origin_base_rates.items():
        surcharge = FY26_SURCHARGES.get(origin, 0.15)
        gen_rates[origin] = mfn_base + surcharge

    print(f"  [Rate Build] FY26 GEN = live MFN base (trade_estimated_duty_item Origin) + surcharges")
    for top_o in ["VN", "CN", "ID", "KH", "IN", "TH", "JO", "GT", "HN"]:
        base = origin_base_rates.get(top_o, 0)
        sur = FY26_SURCHARGES.get(top_o, 0.15)
        print(f"    {top_o}: base={base:.1%} + surcharge={sur:.1%} = FY26={base+sur:.1%}")

    return gen_rates, stp_rates


# ── 2. STP eligibility (server-side GROUP BY — was 98 s raw) ────────

def load_stp_eligibility() -> pd.DataFrame:
    """Pre-aggregated lane × program summary — GROUP BY runs in Databricks."""
    sql = f"""
    SELECT
        h.country_of_origin_cd,
        h.country_of_destination_cd,
        d.stp_cd,
        COUNT(DISTINCT h.product_cd) AS total_products,
        COUNT(DISTINCT h.supplier_cd) AS total_factories,
        COUNT(*) AS total_records,
        SUM(CASE WHEN d.system_decision_cd = 'M' THEN 1 ELSE 0 END) AS met,
        SUM(CASE WHEN d.system_decision_cd = 'N' THEN 1 ELSE 0 END) AS not_met,
        SUM(CASE WHEN d.system_decision_cd = 'I' THEN 1 ELSE 0 END) AS incomplete
    FROM {TC}.trade_product_special_trade_programs_header h
    JOIN {TC}.trade_product_special_trade_programs_detail d
        ON h.trade_product_special_trade_programs_header_uuid
         = d.trade_product_special_trade_programs_header_uuid
    WHERE d.qualification_effective_start_dt >= '2025-01-01'
      AND d.stp_cd IS NOT NULL
      AND d.transaction_delete_ind = 'N'
      AND h.transaction_delete_ind = 'N'
    GROUP BY h.country_of_origin_cd, h.country_of_destination_cd, d.stp_cd
    """
    summary = _run_query(sql)

    for c in ["total_products", "total_factories", "total_records", "met", "not_met", "incomplete"]:
        summary[c] = pd.to_numeric(summary[c], errors="coerce").fillna(0).astype(int)

    summary["qualification_rate"] = summary["met"] / summary["total_records"].replace(0, 1)
    summary["program_name"] = summary["stp_cd"].map(PROGRAM_NAMES).fillna(summary["stp_cd"])
    summary["origin_name"] = summary["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(summary["country_of_origin_cd"])
    summary["dest_name"] = summary["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(summary["country_of_destination_cd"])
    summary["dest_region"] = summary["country_of_destination_cd"].map(GEO_REGIONS).fillna("Other")

    return summary


# ── 3. Lane-level duty aggregates (server-side) ─────────────────────
# Uses trade_goods_item.customs_amt_usd as goods value source instead of
# commodity_tariff.payable_prc, which stopped receiving US data after Sep 2025.
# Country info comes from commodity_classification where available;
# for items without it we fall back to declaration_filing_header jurisdiction.

# US GSP excludes textiles/apparel (HS Ch 50-63) and most footwear (Ch 64).
# Per Maribel Jimenez: Cambodia is mostly apparel -> excluded from GSP.
# FTAs (CAFTA-DR, JO-US, USMCA) cover all product categories.
_GSP_EXCLUDED_HS2 = set(f"{i:02d}" for i in list(range(50, 64)) + [64])

# Programs where HS-level exclusions apply
_PROGRAMS_WITH_HS_EXCLUSIONS = {
    "US_GSP": _GSP_EXCLUDED_HS2,
}

# Program status — US GSP expired Dec 31, 2020; retroactive renewal possible
PROGRAM_STATUS = {
    "US_GSP": {"status": "expired", "note": "Expired Dec 2020. Retroactive renewal pending in Congress."},
    "CAFTA_DR": {"status": "active", "note": ""},
    "JO_US_FTA": {"status": "active", "note": ""},
    "USMCA": {"status": "active", "note": ""},
    "EG_QIZ": {"status": "active", "note": "Qualifying Industrial Zone"},
}


def _lane_agg_sql(dest_filter: str = "US") -> str:
    """Lane-level duty aggregates from published views.
    Uses commodity_tariff_v PAYABLE_PRC (GEN type) as primary goods value,
    falling back to CUSTOMS_AMT_USD when commodity_tariff data is missing.
    Also pulls AD_VALOREM_BASE_PRC as actual assessed duty amount.
    Filters via dts.IMPORT_COUNTRY_CD (dfh_v join is broken for SOLE source)."""
    _dest_col = f"'{dest_filter}'" if dest_filter != "ALL" else "dts.IMPORT_COUNTRY_CD"
    _dest_where = f"AND dts.IMPORT_COUNTRY_CD = '{dest_filter}'" if dest_filter != "ALL" else ""
    return f"""
    WITH base AS (
        SELECT
            COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) AS country_of_origin_cd,
            {_dest_col} AS country_of_destination_cd,
            SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 4) AS hts_chapter,
            SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 2) AS hts_2digit,
            CAST(COALESCE(
                NULLIF(ct_gen.PAYABLE_PRC, 0),
                NULLIF(CAST(tgi.CUSTOMS_AMT_USD AS DOUBLE), 0),
                0
            ) AS DOUBLE) AS goods_value_usd,
            CAST(COALESCE(ct_gen.AD_VALOREM_BASE_PRC, 0) AS DOUBLE) AS assessed_duty_usd,
            tgi.PREFERENTIAL_ORIGIN_IND AS preferential_origin_ind,
            de.ACCEPTANCE_TMST AS acceptance_tmst
        FROM {PUB}.trade_goods_item_v tgi
        JOIN {PUB}.declaration_trade_shipment_v dts
            ON tgi.DECLARATION_TRADE_SHIPMENT_UUID = dts.DECLARATION_TRADE_SHIPMENT_UUID
        JOIN {PUB}.declaration_entry_v de
            ON dts.DECLARATION_ENTRY_UUID = de.DECLARATION_ENTRY_UUID
        LEFT JOIN {PUB}.item_commodity_v ic
            ON ic.TRADE_GOODS_ITEM_UUID = tgi.TRADE_GOODS_ITEM_UUID
        LEFT JOIN {PUB}.commodity_classification_v cc
            ON cc.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
        LEFT JOIN {PUB}.commodity_tariff_v ct_gen
            ON ct_gen.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
            AND ct_gen.TARIFF_TYPE_CD = 'GEN'
        WHERE de.ACCEPTANCE_TMST >= '{FY_START}'
          AND de.ACCEPTANCE_TMST < '{FY_END}'
          {_dest_where}
    )
    SELECT
        country_of_origin_cd,
        country_of_destination_cd,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT hts_chapter) AS unique_hts,
        SUM(goods_value_usd) AS gen_goods_value,
        COUNT(*) AS gen_row_count,
        SUM(assessed_duty_usd) AS assessed_duty_total,
        0 AS pref_goods_value,
        0 AS pref_row_count,
        SUM(CASE WHEN preferential_origin_ind = 'Y' THEN 1 ELSE 0 END) AS pref_y_count,
        SUM(CASE WHEN preferential_origin_ind != 'Y' THEN 1 ELSE 0 END) AS pref_n_count,
        MIN(acceptance_tmst) AS earliest_dt,
        MAX(acceptance_tmst) AS latest_dt
    FROM base
    WHERE country_of_origin_cd IS NOT NULL
      AND country_of_destination_cd IS NOT NULL
    GROUP BY country_of_origin_cd, country_of_destination_cd
    """


def _load_eligible_goods_value() -> pd.DataFrame:
    """Goods value by origin x 2-digit HS chapter for US imports.
    Uses commodity_tariff_v PAYABLE_PRC with CUSTOMS_AMT_USD fallback."""
    sql = f"""
    WITH base AS (
        SELECT
            COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) AS country_of_origin_cd,
            SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 2) AS hts_2digit,
            CAST(COALESCE(
                NULLIF(ct_gen.PAYABLE_PRC, 0),
                NULLIF(CAST(tgi.CUSTOMS_AMT_USD AS DOUBLE), 0),
                0
            ) AS DOUBLE) AS goods_value_usd
        FROM {PUB}.trade_goods_item_v tgi
        JOIN {PUB}.declaration_trade_shipment_v dts
            ON tgi.DECLARATION_TRADE_SHIPMENT_UUID = dts.DECLARATION_TRADE_SHIPMENT_UUID
        JOIN {PUB}.declaration_entry_v de
            ON dts.DECLARATION_ENTRY_UUID = de.DECLARATION_ENTRY_UUID
        LEFT JOIN {PUB}.item_commodity_v ic
            ON ic.TRADE_GOODS_ITEM_UUID = tgi.TRADE_GOODS_ITEM_UUID
        LEFT JOIN {PUB}.commodity_classification_v cc
            ON cc.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
        LEFT JOIN {PUB}.commodity_tariff_v ct_gen
            ON ct_gen.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
            AND ct_gen.TARIFF_TYPE_CD = 'GEN'
        WHERE de.ACCEPTANCE_TMST >= '{FY_START}'
          AND de.ACCEPTANCE_TMST < '{FY_END}'
          AND dts.IMPORT_COUNTRY_CD = 'US'
    )
    SELECT
        country_of_origin_cd,
        hts_2digit,
        SUM(goods_value_usd) AS goods_value_usd
    FROM base
    WHERE country_of_origin_cd IS NOT NULL
    GROUP BY country_of_origin_cd, hts_2digit
    """
    df = _run_query(sql)
    df["goods_value_usd"] = pd.to_numeric(df["goods_value_usd"], errors="coerce").fillna(0)
    return df


def _calc_eligible_pct(origin: str, programs: list, hs_gv_df: pd.DataFrame) -> float:
    """What fraction of an origin's goods value is eligible for its STP programs?
    FTAs (CAFTA-DR, USMCA, JO_US_FTA, EG_QIZ) cover all HS chapters.
    US_GSP excludes textiles/apparel (Ch 50-63) and footwear (Ch 64)."""
    exclusions = set()
    for prog in programs:
        if prog in _PROGRAMS_WITH_HS_EXCLUSIONS:
            exclusions |= _PROGRAMS_WITH_HS_EXCLUSIONS[prog]

    if not exclusions:
        return 1.0  # FTA — all chapters eligible

    origin_gv = hs_gv_df[hs_gv_df["country_of_origin_cd"] == origin]
    if origin_gv.empty:
        return 1.0

    total = origin_gv["goods_value_usd"].sum()
    if total == 0:
        return 1.0

    excluded = origin_gv[origin_gv["hts_2digit"].isin(exclusions)]["goods_value_usd"].sum()
    # Items with no HS code (None/null) are conservatively treated as excluded
    no_hs = origin_gv[origin_gv["hts_2digit"].isna() | (origin_gv["hts_2digit"] == "")]["goods_value_usd"].sum()

    eligible = total - excluded - no_hs
    return max(eligible / total, 0.0)


def _build_lane_stp_rate(stp_df: pd.DataFrame) -> dict:
    """Build a lane-specific STP rate map keyed on (origin, dest).
    STP programs are lane-specific — Cambodia→US has GSP but Cambodia→Japan
    does not. Rates default to 0% (duty-free) for lanes with an active STP,
    unless the origin has a known partial rate (e.g. Indonesia GSP at 17.6%).
    Returns dict mapping (origin_cd, dest_cd) → preferential rate."""
    lane_rates = {}
    stp_lanes = stp_df.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    )["stp_cd"].apply(list).reset_index()

    for _, row in stp_lanes.iterrows():
        origin = row["country_of_origin_cd"]
        dest = row["country_of_destination_cd"]
        if origin in STP_DUTY_RATES:
            lane_rates[(origin, dest)] = STP_DUTY_RATES[origin]
        else:
            lane_rates[(origin, dest)] = 0.0
    return lane_rates


def load_lane_summary(stp_df: pd.DataFrame, hs_gv_df: pd.DataFrame = None) -> pd.DataFrame:
    """Lane-level summary built entirely from server-side aggregation."""
    lane_duties = _run_query(_lane_agg_sql())

    for c in ["gen_goods_value", "pref_goods_value", "assessed_duty_total"]:
        lane_duties[c] = pd.to_numeric(lane_duties[c], errors="coerce").fillna(0)
    for c in ["total_rows", "unique_hts", "gen_row_count", "pref_row_count", "pref_y_count", "pref_n_count"]:
        lane_duties[c] = pd.to_numeric(lane_duties[c], errors="coerce").fillna(0).astype(int)

    lane_duties.rename(columns={
        "gen_goods_value": "goods_value_usd",
        "pref_goods_value": "pref_goods_value_usd",
    }, inplace=True)

    lane_duties["gen_rate"] = lane_duties["country_of_origin_cd"].map(GEN_DUTY_RATES).fillna(0.25)

    lane_duties["utilization_pct"] = lane_duties["pref_y_count"] / (
        lane_duties["pref_y_count"] + lane_duties["pref_n_count"]
    )
    lane_duties["utilization_pct"] = lane_duties["utilization_pct"].fillna(0)

    # STP program enrichment — merge BEFORE computing duty/savings
    lane_stp = stp_df.groupby(["country_of_origin_cd", "country_of_destination_cd"]).agg(
        stp_programs=("stp_cd", lambda x: list(x.unique())),
        stp_program_names=("program_name", lambda x: list(x.unique())),
        stp_total_products=("total_products", "sum"),
        stp_total_factories=("total_factories", "max"),
        stp_met=("met", "sum"),
        stp_not_met=("not_met", "sum"),
        stp_incomplete=("incomplete", "sum"),
        stp_qualification_rate=("qualification_rate", "mean"),
    ).reset_index()

    lanes = lane_duties.merge(
        lane_stp, on=["country_of_origin_cd", "country_of_destination_cd"], how="left",
    )

    lanes["has_stp"] = lanes["stp_programs"].notna()
    lanes["stp_programs"] = lanes["stp_programs"].apply(lambda x: x if isinstance(x, list) else [])
    lanes["stp_program_names"] = lanes["stp_program_names"].apply(lambda x: x if isinstance(x, list) else [])
    lanes["stp_qualification_rate"] = lanes["stp_qualification_rate"].fillna(0)
    lanes["stp_met"] = lanes["stp_met"].fillna(0)
    lanes["stp_not_met"] = lanes["stp_not_met"].fillna(0)

    # HS-level eligibility: what % of this lane's goods value is actually
    # eligible for the STP program? US GSP excludes textiles/apparel/footwear.
    if hs_gv_df is not None:
        lanes["eligible_pct"] = lanes.apply(
            lambda r: _calc_eligible_pct(
                r["country_of_origin_cd"],
                r["stp_programs"],
                hs_gv_df,
            ) if r["has_stp"] else 0.0,
            axis=1,
        )
    else:
        lanes["eligible_pct"] = np.where(lanes["has_stp"], 1.0, 0.0)

    lanes["eligible_goods_value_usd"] = lanes["goods_value_usd"] * lanes["eligible_pct"]

    # Lane-specific STP rate: only lanes with an actual STP program get a
    # preferential rate.  Lanes without STP -> stp_rate = gen_rate -> no savings.
    lane_stp_rates = _build_lane_stp_rate(stp_df)
    lanes["stp_rate"] = lanes.apply(
        lambda r: lane_stp_rates.get(
            (r["country_of_origin_cd"], r["country_of_destination_cd"]),
            r["gen_rate"],
        ),
        axis=1,
    )

    lanes["gen_duty_usd"] = lanes["goods_value_usd"] * lanes["gen_rate"]
    # STP duty applies only to the eligible portion
    lanes["stp_duty_usd"] = (
        lanes["eligible_goods_value_usd"] * lanes["stp_rate"]
        + (lanes["goods_value_usd"] - lanes["eligible_goods_value_usd"]) * lanes["gen_rate"]
    )

    # Savings realized = duty avoided on the ELIGIBLE portion that DID claim STP.
    lanes["savings_realized_usd"] = np.where(
        lanes["has_stp"],
        (lanes["eligible_goods_value_usd"]
         * (lanes["gen_rate"] - lanes["stp_rate"])
         * lanes["utilization_pct"]).clip(lower=0),
        0.0,
    )

    # Excess duty = overpayment on the ELIGIBLE portion not claiming STP.
    lanes["excess_duty_usd"] = np.where(
        lanes["has_stp"],
        (lanes["eligible_goods_value_usd"]
         * (lanes["gen_rate"] - lanes["stp_rate"])
         * (1 - lanes["utilization_pct"])).clip(lower=0),
        0.0,
    )

    lanes["origin_name"] = lanes["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(lanes["country_of_origin_cd"])
    lanes["dest_name"] = lanes["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(lanes["country_of_destination_cd"])
    lanes["dest_region"] = lanes["country_of_destination_cd"].map(GEO_REGIONS).fillna("Other")

    lanes["is_gap_lane"] = lanes["has_stp"] & (lanes["utilization_pct"] < 0.5)

    return lanes.sort_values("goods_value_usd", ascending=False)


# ── 4. Recovery analysis (server-side aggregation) ───────────────────

def load_recovery(stp_df: pd.DataFrame) -> pd.DataFrame:
    """PSC-eligible GEN declarations on STP-qualified lanes, aggregated server-side.
    Uses commodity_tariff_v PAYABLE_PRC with CUSTOMS_AMT_USD fallback.

    CRITICAL: Filters out HTS chapters excluded by the lane's STP programs.
    US GSP excludes textiles/apparel (Ch 50-63) and footwear (Ch 64).
    For lanes with full-coverage FTAs (CAFTA-DR, USMCA, JO-US FTA), all chapters qualify.
    """
    from real_data_loader import get_excluded_hs2_for_programs

    stp_programs_by_lane = stp_df.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    )["stp_cd"].apply(list).to_dict()

    stp_lanes = set(stp_programs_by_lane.keys())
    if not stp_lanes:
        return pd.DataFrame()

    # Split origins into GSP-only (need HTS filter) and full-coverage (no filter)
    gsp_only_origins = set()
    full_coverage_origins = set()
    for (origin, dest), programs in stp_programs_by_lane.items():
        if dest != "US":
            continue
        excluded = get_excluded_hs2_for_programs(programs)
        if excluded:
            gsp_only_origins.add(origin)
        else:
            full_coverage_origins.add(origin)

    # Build the GSP excluded chapters SQL filter
    gsp_excluded_str = ",".join(f"'{ch}'" for ch in _GSP_EXCLUDED_HS2)

    parts = []

    # Full-coverage origins: no HTS filter needed
    if full_coverage_origins:
        fc_list = ",".join(f"'{o}'" for o in full_coverage_origins)
        parts.append(f"""
        SELECT
            COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) AS country_of_origin_cd,
            'US' AS country_of_destination_cd,
            COUNT(*) AS declaration_count,
            SUM(CAST(COALESCE(
                NULLIF(ct_gen.PAYABLE_PRC, 0),
                NULLIF(CAST(tgi.CUSTOMS_AMT_USD AS DOUBLE), 0),
                0
            ) AS DOUBLE)) AS goods_value,
            MIN(de.ACCEPTANCE_TMST) AS earliest,
            MAX(de.ACCEPTANCE_TMST) AS latest,
            COLLECT_SET(SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 10)) AS hts_codes_arr
        FROM {PUB}.trade_goods_item_v tgi
        JOIN {PUB}.declaration_trade_shipment_v dts
            ON tgi.DECLARATION_TRADE_SHIPMENT_UUID = dts.DECLARATION_TRADE_SHIPMENT_UUID
        JOIN {PUB}.declaration_entry_v de
            ON dts.DECLARATION_ENTRY_UUID = de.DECLARATION_ENTRY_UUID
        LEFT JOIN {PUB}.item_commodity_v ic
            ON ic.TRADE_GOODS_ITEM_UUID = tgi.TRADE_GOODS_ITEM_UUID
        LEFT JOIN {PUB}.commodity_classification_v cc
            ON cc.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
        LEFT JOIN {PUB}.commodity_tariff_v ct_gen
            ON ct_gen.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
            AND ct_gen.TARIFF_TYPE_CD = 'GEN'
        WHERE tgi.PREFERENTIAL_ORIGIN_IND = 'N'
          AND de.ACCEPTANCE_TMST >= dateadd(DAY, -{PSC_WINDOW_DAYS}, current_date())
          AND dts.IMPORT_COUNTRY_CD = 'US'
          AND COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) IN ({fc_list})
        GROUP BY COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD)
        """)

    # GSP-only origins: filter OUT excluded HS chapters
    if gsp_only_origins:
        gsp_list = ",".join(f"'{o}'" for o in gsp_only_origins)
        parts.append(f"""
        SELECT
            COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) AS country_of_origin_cd,
            'US' AS country_of_destination_cd,
            COUNT(*) AS declaration_count,
            SUM(CAST(COALESCE(
                NULLIF(ct_gen.PAYABLE_PRC, 0),
                NULLIF(CAST(tgi.CUSTOMS_AMT_USD AS DOUBLE), 0),
                0
            ) AS DOUBLE)) AS goods_value,
            MIN(de.ACCEPTANCE_TMST) AS earliest,
            MAX(de.ACCEPTANCE_TMST) AS latest,
            COLLECT_SET(SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 10)) AS hts_codes_arr
        FROM {PUB}.trade_goods_item_v tgi
        JOIN {PUB}.declaration_trade_shipment_v dts
            ON tgi.DECLARATION_TRADE_SHIPMENT_UUID = dts.DECLARATION_TRADE_SHIPMENT_UUID
        JOIN {PUB}.declaration_entry_v de
            ON dts.DECLARATION_ENTRY_UUID = de.DECLARATION_ENTRY_UUID
        LEFT JOIN {PUB}.item_commodity_v ic
            ON ic.TRADE_GOODS_ITEM_UUID = tgi.TRADE_GOODS_ITEM_UUID
        LEFT JOIN {PUB}.commodity_classification_v cc
            ON cc.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
        LEFT JOIN {PUB}.commodity_tariff_v ct_gen
            ON ct_gen.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
            AND ct_gen.TARIFF_TYPE_CD = 'GEN'
        WHERE tgi.PREFERENTIAL_ORIGIN_IND = 'N'
          AND de.ACCEPTANCE_TMST >= dateadd(DAY, -{PSC_WINDOW_DAYS}, current_date())
          AND dts.IMPORT_COUNTRY_CD = 'US'
          AND COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) IN ({gsp_list})
          AND SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 2) NOT IN ({gsp_excluded_str})
          AND cc.HTS_CD IS NOT NULL
        GROUP BY COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD)
        """)

    if not parts:
        return pd.DataFrame()

    sql = " UNION ALL ".join(parts)
    recovery = _run_query(sql)

    if recovery.empty:
        return pd.DataFrame()

    recovery["goods_value"] = pd.to_numeric(recovery["goods_value"], errors="coerce").fillna(0)
    recovery["declaration_count"] = pd.to_numeric(recovery["declaration_count"], errors="coerce").fillna(0).astype(int)

    recovery["earliest"] = pd.to_datetime(recovery["earliest"], errors="coerce", utc=True).dt.tz_localize(None)
    recovery["latest"] = pd.to_datetime(recovery["latest"], errors="coerce", utc=True).dt.tz_localize(None)

    # filter to actual STP lanes (origin+dest combos that exist in stp_df)
    recovery["_lane"] = list(zip(recovery["country_of_origin_cd"], recovery["country_of_destination_cd"]))
    recovery = recovery[recovery["_lane"].isin(stp_lanes)].drop(columns=["_lane"])

    if recovery.empty:
        return pd.DataFrame()

    recovery["gen_rate"] = recovery["country_of_origin_cd"].map(GEN_DUTY_RATES).fillna(0.25)
    lane_stp_rates = _build_lane_stp_rate(stp_df)
    recovery["stp_rate"] = recovery.apply(
        lambda r: lane_stp_rates.get(
            (r["country_of_origin_cd"], r["country_of_destination_cd"]),
            r["gen_rate"],
        ),
        axis=1,
    )
    recovery["gen_duty_paid"] = recovery["goods_value"] * recovery["gen_rate"]
    recovery["stp_duty"] = recovery["goods_value"] * recovery["stp_rate"]
    recovery["duty_savings"] = recovery["gen_duty_paid"] - recovery["stp_duty"]

    recovery["origin_name"] = recovery["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(recovery["country_of_origin_cd"])
    recovery["dest_name"] = recovery["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(recovery["country_of_destination_cd"])

    # merge qualification rates from STP eligibility
    qual = stp_df.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    )["qualification_rate"].mean().reset_index().rename(columns={"qualification_rate": "stp_qualification_rate"})

    recovery = recovery.merge(qual, on=["country_of_origin_cd", "country_of_destination_cd"], how="left")
    recovery["stp_qualification_rate"] = recovery["stp_qualification_rate"].fillna(0.5)
    recovery["estimated_recovery"] = recovery["duty_savings"] * recovery["stp_qualification_rate"]

    # STP program info per lane
    stp_by_lane = stp_df.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    ).agg(
        stp_programs=("stp_cd", lambda x: list(x.unique())),
        stp_program_names=("program_name", lambda x: list(x.unique())),
        stp_met=("met", "sum"),
        stp_total_products=("total_products", "sum"),
    ).reset_index()

    recovery = recovery.merge(stp_by_lane, on=["country_of_origin_cd", "country_of_destination_cd"], how="left")
    recovery["stp_programs"] = recovery["stp_programs"].apply(lambda x: x if isinstance(x, list) else [])
    recovery["stp_program_names"] = recovery["stp_program_names"].apply(lambda x: x if isinstance(x, list) else [])

    # HTS codes from COLLECT_SET
    recovery["hts_codes"] = recovery["hts_codes_arr"].apply(
        lambda x: list(x)[:20] if isinstance(x, (list, set)) else []
    )
    recovery["avg_tariff_rate"] = recovery["gen_rate"] * 100
    recovery.drop(columns=["hts_codes_arr"], inplace=True, errors="ignore")

    return recovery.sort_values("estimated_recovery", ascending=False)


# ── 5. Claim packages (built from recovery df, no extra query) ───────

def build_claim_packages(recovery_df: pd.DataFrame) -> list[dict]:
    """Build claim package dicts from the pre-aggregated recovery dataframe."""
    from real_data_loader import _get_required_documents, _get_filing_mechanism

    if recovery_df is None or recovery_df.empty:
        return []

    packages = []
    for _, row in recovery_df.iterrows():
        origin = row["country_of_origin_cd"]
        dest = row["country_of_destination_cd"]

        hts_list = row.get("hts_codes", [])
        hts_detail = [
            {"hts_cd": h, "count": 0, "total_duty": 0, "avg_rate": row.get("avg_tariff_rate", 0)}
            for h in hts_list[:10]
        ]

        packages.append({
            "origin_cd": origin,
            "origin_name": row["origin_name"],
            "dest_cd": dest,
            "dest_name": row["dest_name"],
            "applicable_stps": row.get("stp_programs", []),
            "applicable_stp_names": row.get("stp_program_names", []),
            "qualification_rate": row.get("stp_qualification_rate", 0),
            "declaration_count": int(row["declaration_count"]),
            "gen_duty_paid": float(row["gen_duty_paid"]),
            "estimated_recovery": float(row["estimated_recovery"]),
            "goods_value": float(row["goods_value"]),
            "duty_savings": float(row["duty_savings"]),
            "window_start": row["earliest"].strftime("%Y-%m-%d") if pd.notna(row.get("earliest")) else "N/A",
            "window_end": row["latest"].strftime("%Y-%m-%d") if pd.notna(row.get("latest")) else "N/A",
            "earliest_days_remaining": max(0, PSC_WINDOW_DAYS - (pd.Timestamp.now() - row["earliest"]).days) if pd.notna(row.get("earliest")) else 0,
            "latest_days_remaining": max(0, PSC_WINDOW_DAYS - (pd.Timestamp.now() - row["latest"]).days) if pd.notna(row.get("latest")) else 0,
            "hts_codes": hts_list[:15],
            "hts_detail": hts_detail,
            "required_documents": _get_required_documents(origin, dest, row.get("stp_programs", [])),
            "filing_mechanism": _get_filing_mechanism(dest),
        })

    return packages


# ── 6. Lightweight duties for Sourcing Shift Simulator ───────────────

def load_sim_data() -> pd.DataFrame:
    """Lane x HTS-chapter level aggregates for the sourcing shift simulator.
    Uses commodity_tariff_v PAYABLE_PRC with CUSTOMS_AMT_USD fallback."""
    sql = f"""
    SELECT
        COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) AS country_of_origin_cd,
        'US' AS country_of_destination_cd,
        SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 4) AS hts_chapter,
        SUM(CAST(COALESCE(
            NULLIF(ct_gen.PAYABLE_PRC, 0),
            NULLIF(CAST(tgi.CUSTOMS_AMT_USD AS DOUBLE), 0),
            0
        ) AS DOUBLE)) AS goods_value_usd,
        COUNT(*) AS row_count
    FROM {PUB}.trade_goods_item_v tgi
    JOIN {PUB}.declaration_trade_shipment_v dts
        ON tgi.DECLARATION_TRADE_SHIPMENT_UUID = dts.DECLARATION_TRADE_SHIPMENT_UUID
    JOIN {PUB}.declaration_entry_v de
        ON dts.DECLARATION_ENTRY_UUID = de.DECLARATION_ENTRY_UUID
    LEFT JOIN {PUB}.item_commodity_v ic
        ON ic.TRADE_GOODS_ITEM_UUID = tgi.TRADE_GOODS_ITEM_UUID
    LEFT JOIN {PUB}.commodity_classification_v cc
        ON cc.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
    LEFT JOIN {PUB}.commodity_tariff_v ct_gen
        ON ct_gen.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
        AND ct_gen.TARIFF_TYPE_CD = 'GEN'
    WHERE de.ACCEPTANCE_TMST >= '{FY_START}'
      AND de.ACCEPTANCE_TMST < '{FY_END}'
      AND dts.IMPORT_COUNTRY_CD = 'US'
      AND COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) IS NOT NULL
    GROUP BY COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD),
             SUBSTRING(CAST(cc.HTS_CD AS STRING), 1, 4)
    """
    df = _run_query(sql)
    df["goods_value_usd"] = pd.to_numeric(df["goods_value_usd"], errors="coerce").fillna(0)
    df["row_count"] = pd.to_numeric(df["row_count"], errors="coerce").fillna(0).astype(int)
    df["origin_name"] = df["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(df["country_of_origin_cd"])
    df["dest_name"] = df["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(df["country_of_destination_cd"])
    return df


# ── 7. Global metrics (single server-side query) ────────────────────

def _load_meta_aggregates() -> dict:
    """Top-line numbers: goods value, date range, row counts from published views.
    Uses commodity_tariff_v PAYABLE_PRC with CUSTOMS_AMT_USD fallback."""
    sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(*) AS gen_pref_rows,
        SUM(CAST(COALESCE(
            NULLIF(ct_gen.PAYABLE_PRC, 0),
            NULLIF(CAST(tgi.CUSTOMS_AMT_USD AS DOUBLE), 0),
            0
        ) AS DOUBLE)) AS gen_goods_value,
        SUM(CASE WHEN de.ACCEPTANCE_TMST >= dateadd(DAY, -{PSC_WINDOW_DAYS}, current_date())
                 THEN 1 ELSE 0 END) AS psc_eligible_rows,
        MIN(de.ACCEPTANCE_TMST) AS earliest_dt,
        MAX(de.ACCEPTANCE_TMST) AS latest_dt
    FROM {PUB}.trade_goods_item_v tgi
    JOIN {PUB}.declaration_trade_shipment_v dts
        ON tgi.DECLARATION_TRADE_SHIPMENT_UUID = dts.DECLARATION_TRADE_SHIPMENT_UUID
    JOIN {PUB}.declaration_entry_v de
        ON dts.DECLARATION_ENTRY_UUID = de.DECLARATION_ENTRY_UUID
    LEFT JOIN {PUB}.item_commodity_v ic
        ON ic.TRADE_GOODS_ITEM_UUID = tgi.TRADE_GOODS_ITEM_UUID
    LEFT JOIN {PUB}.commodity_tariff_v ct_gen
        ON ct_gen.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
        AND ct_gen.TARIFF_TYPE_CD = 'GEN'
    WHERE de.ACCEPTANCE_TMST >= '{FY_START}'
      AND de.ACCEPTANCE_TMST < '{FY_END}'
      AND dts.IMPORT_COUNTRY_CD = 'US'
    """
    row = _run_query(sql).iloc[0]
    return {
        "total_rows": int(pd.to_numeric(row["total_rows"], errors="coerce") or 0),
        "gen_pref_rows": int(pd.to_numeric(row["gen_pref_rows"], errors="coerce") or 0),
        "gen_goods_value": float(pd.to_numeric(row["gen_goods_value"], errors="coerce") or 0),
        "psc_eligible_rows": int(pd.to_numeric(row["psc_eligible_rows"], errors="coerce") or 0),
        "earliest_dt": pd.to_datetime(row["earliest_dt"], errors="coerce", utc=True).tz_localize(None) if pd.notna(row["earliest_dt"]) else None,
        "latest_dt": pd.to_datetime(row["latest_dt"], errors="coerce", utc=True).tz_localize(None) if pd.notna(row["latest_dt"]) else None,
    }


# ── 8. Claims detail (loaded lazily, not on startup) ────────────────

def load_claims_detail(stp_df: pd.DataFrame) -> pd.DataFrame:
    """Entry-level detail for PSC-eligible declarations. Only called on demand.
    Uses commodity_tariff_v PAYABLE_PRC with CUSTOMS_AMT_USD fallback.
    Returns line-level rows; HTS filtering applied post-query in get_filing_list_for_lane."""
    stp_lanes = set(zip(stp_df["country_of_origin_cd"], stp_df["country_of_destination_cd"]))
    if not stp_lanes:
        return pd.DataFrame()

    origin_list = ",".join(f"'{o}'" for o, _ in stp_lanes)

    sql = f"""
    SELECT
        COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) AS country_of_origin_cd,
        'US' AS country_of_destination_cd,
        cc.HTS_CD AS hts_cd,
        CAST(COALESCE(
            NULLIF(ct_gen.PAYABLE_PRC, 0),
            NULLIF(CAST(tgi.CUSTOMS_AMT_USD AS DOUBLE), 0),
            0
        ) AS DOUBLE) AS customs_amt_usd,
        tgi.STATISTICAL_AMT_USD AS statistical_amt_usd,
        tgi.PREFERENTIAL_ORIGIN_IND AS preferential_origin_ind,
        tgi.SEQUENCE_NBR AS line_item_nbr,
        de.ACCEPTANCE_TMST AS acceptance_tmst,
        de.DECLARATION_ENTRY_UUID AS declaration_entry_uid,
        de.DECLARATION_STATUS_DESC AS declaration_status_desc,
        de.LOCAL_REFERENCE_NBR AS declaration_identification_nbr,
        de.LOCAL_REFERENCE_NBR AS filing_reference_nbr
    FROM {PUB}.trade_goods_item_v tgi
    JOIN {PUB}.declaration_trade_shipment_v dts
        ON tgi.DECLARATION_TRADE_SHIPMENT_UUID = dts.DECLARATION_TRADE_SHIPMENT_UUID
    JOIN {PUB}.declaration_entry_v de
        ON dts.DECLARATION_ENTRY_UUID = de.DECLARATION_ENTRY_UUID
    LEFT JOIN {PUB}.item_commodity_v ic
        ON ic.TRADE_GOODS_ITEM_UUID = tgi.TRADE_GOODS_ITEM_UUID
    LEFT JOIN {PUB}.commodity_classification_v cc
        ON cc.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
    LEFT JOIN {PUB}.commodity_tariff_v ct_gen
        ON ct_gen.ITEM_COMMODITY_UUID = ic.ITEM_COMMODITY_UUID
        AND ct_gen.TARIFF_TYPE_CD = 'GEN'
    WHERE tgi.PREFERENTIAL_ORIGIN_IND = 'N'
      AND de.ACCEPTANCE_TMST >= dateadd(DAY, -{PSC_WINDOW_DAYS}, current_date())
      AND dts.IMPORT_COUNTRY_CD = 'US'
      AND COALESCE(cc.COUNTRY_OF_ORIGIN_CD, dts.EXPORT_COUNTRY_CD) IN ({origin_list})
    """
    df = _run_query(sql)

    df["customs_amt_usd"] = pd.to_numeric(df["customs_amt_usd"], errors="coerce").fillna(0)
    df["goods_value_usd"] = df["customs_amt_usd"]
    df["statistical_amt_usd"] = pd.to_numeric(df["statistical_amt_usd"], errors="coerce").fillna(0)
    df["acceptance_dt"] = pd.to_datetime(df["acceptance_tmst"], errors="coerce", utc=True).dt.tz_localize(None)

    df["gen_rate"] = df["country_of_origin_cd"].map(GEN_DUTY_RATES).fillna(0.25)
    lane_stp_rates = _build_lane_stp_rate(stp_df)
    df["stp_rate"] = df.apply(
        lambda r: lane_stp_rates.get(
            (r["country_of_origin_cd"], r["country_of_destination_cd"]),
            r["gen_rate"],
        ),
        axis=1,
    )
    df["estimated_gen_duty"] = df["goods_value_usd"] * df["gen_rate"]
    df["estimated_duty_savings"] = df["goods_value_usd"] * (df["gen_rate"] - df["stp_rate"])

    df["origin_name"] = df["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(df["country_of_origin_cd"])
    df["dest_name"] = df["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(df["country_of_destination_cd"])

    df["days_since_acceptance"] = (pd.Timestamp.now() - df["acceptance_dt"]).dt.days
    df["days_remaining"] = (PSC_WINDOW_DAYS - df["days_since_acceptance"]).clip(lower=0)

    entry_col = "filing_reference_nbr" if df["filing_reference_nbr"].notna().any() else "declaration_identification_nbr"
    df["entry_id"] = df[entry_col]

    return df


# ── load_all: orchestrator ───────────────────────────────────────────

def load_all():
    """Load all data live from Databricks using server-side aggregation.
    Scoped to US imports. Returns same structure as real_data_loader.load_all()."""
    import time

    t0 = time.time()
    print("[Databricks] Loading duty rates...")
    live_gen_rates, live_stp_rates = load_duty_rates()
    GEN_DUTY_RATES.update(live_gen_rates)
    STP_DUTY_RATES.update(live_stp_rates)
    print(f"  GEN rates for {len(live_gen_rates)} countries, STP for {len(live_stp_rates)} in {time.time()-t0:.0f}s")

    t1 = time.time()
    print("[Databricks] Loading STP eligibility (server-side agg)...")
    stp_df = load_stp_eligibility()
    print(f"  {len(stp_df)} lane-program combos in {time.time()-t1:.0f}s")

    t1b = time.time()
    print("[Databricks] Loading HS-level goods value for eligibility filtering...")
    hs_gv_df = _load_eligible_goods_value()
    print(f"  {len(hs_gv_df)} origin x HS rows in {time.time()-t1b:.0f}s")

    t2 = time.time()
    print("[Databricks] Loading lane summary (server-side agg)...")
    lanes_df = load_lane_summary(stp_df, hs_gv_df)
    # Log eligibility adjustments (use ascii-safe output for Windows)
    try:
        for _, r in lanes_df[lanes_df["has_stp"] & (lanes_df["eligible_pct"] < 1.0)].iterrows():
            print(f"  ** {r['country_of_origin_cd']}: eligible_pct={r['eligible_pct']:.1%} "
                  f"(${r['eligible_goods_value_usd']/1e6:.1f}M of ${r['goods_value_usd']/1e6:.1f}M)")
    except OSError:
        pass
    print(f"  {len(lanes_df)} lanes in {time.time()-t2:.0f}s")

    t3 = time.time()
    print("[Databricks] Loading meta aggregates...")
    meta_agg = _load_meta_aggregates()
    print(f"  Aggregates in {time.time()-t3:.0f}s")

    t4 = time.time()
    print("[Databricks] Loading recovery analysis (server-side agg)...")
    recovery_df = load_recovery(stp_df)
    print(f"  {len(recovery_df)} recoverable lanes in {time.time()-t4:.0f}s")

    claim_packages = build_claim_packages(recovery_df)
    programs_df = build_program_summary(stp_df, lanes_df)

    t5 = time.time()
    print("[Databricks] Loading claims detail (entry-level)...")
    claims_detail_df = load_claims_detail(stp_df)
    print(f"  {len(claims_detail_df)} claim entries in {time.time()-t5:.0f}s")

    t6 = time.time()
    print("[Databricks] Loading sourcing shift simulator data...")
    sim_df = load_sim_data()
    print(f"  {len(sim_df)} rows in {time.time()-t6:.0f}s")

    # STP total count
    stp_total_df = _run_query(
        f"SELECT COUNT(*) AS c FROM {TC}.trade_product_special_trade_programs_detail "
        f"WHERE qualification_effective_start_dt >= '2025-01-01'"
    )
    stp_total = int(stp_total_df["c"].iloc[0])

    # Date range from meta_agg
    if meta_agg["earliest_dt"] and meta_agg["latest_dt"]:
        date_range = f"{meta_agg['earliest_dt'].strftime('%b %Y')} - {meta_agg['latest_dt'].strftime('%b %Y')}"
    else:
        date_range = "Unknown"

    total_goods_value = meta_agg["gen_goods_value"]
    total_gen_duty = float(lanes_df["gen_duty_usd"].sum())
    total_stp_duty = float(lanes_df["stp_duty_usd"].sum())
    total_savings_potential = float(lanes_df["excess_duty_usd"].sum())
    total_savings_realized = float(lanes_df["savings_realized_usd"].sum())

    # Assessed duty from commodity_tariff_v (actual duty amounts where available)
    total_assessed_duty = float(lanes_df["assessed_duty_total"].sum()) if "assessed_duty_total" in lanes_df.columns else 0.0

    total_time = time.time() - t0
    print(f"[Databricks] All data loaded in {total_time:.0f}s")
    print(f"  Goods value: ${total_goods_value/1e6:,.1f}M  |  Assessed duty: ${total_assessed_duty/1e6:,.1f}M")

    duties_df = sim_df.copy()
    duties_df["tariff_type_cd"] = "GEN"

    return {
        "stp_eligibility": stp_df,
        "duties_paid": duties_df,
        "lanes": lanes_df,
        "programs": programs_df,
        "recovery": recovery_df,
        "claim_packages": claim_packages,
        "claims_detail": claims_detail_df if not claims_detail_df.empty else None,
        "gen_pref_df": pd.DataFrame(),
        "meta": {
            "stp_total_records": stp_total,
            "duties_total_rows": meta_agg["total_rows"],
            "duties_total_records": meta_agg["gen_pref_rows"],
            "duties_sample_limited": False,
            "stp_date_range": "Jan 2025 - Dec 2026",
            "duties_date_range": date_range,
            "fiscal_quarter_counts": {},
            "last_etl": "Live from Databricks",
            "source": f"{PUB} (Databricks published views)",
            "unique_programs": stp_df["stp_cd"].nunique(),
            "unique_origins": stp_df["country_of_origin_cd"].nunique(),
            "unique_destinations": stp_df["country_of_destination_cd"].nunique(),
            "psc_window_days": PSC_WINDOW_DAYS,
            "psc_eligible_rows": meta_agg["psc_eligible_rows"],
            "total_goods_value": total_goods_value,
            "total_gen_duty": total_gen_duty,
            "total_stp_duty": total_stp_duty,
            "total_savings_potential": total_savings_potential,
            "total_savings_realized": total_savings_realized,
            "total_assessed_duty": total_assessed_duty,
            "data_source": "databricks_live",
            "data_completeness_note": (
                "Goods value sourced from commodity_tariff_v (PAYABLE_PRC) "
                "with trade_goods_item_v (CUSTOMS_AMT_USD) fallback. "
                "Approximately 67% of ACE-reported entered value is captured in Databricks. "
                "Key gaps: Indonesia, Thailand, and Malaysia have partial row-level coverage."
            ),
        },
    }
