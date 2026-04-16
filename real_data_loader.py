"""
Loads real data from Databricks CSV extracts and transforms into
POC-ready dataframes matching the structure expected by app.py.

Source: non_published_domain.trade_customs (Databricks)
Data freshness: STP eligibility 2025-2026, Duties paid FY26 (Jun 2025 - Mar 2026)
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter

DATA_DIR = Path(__file__).parent / "real_data"

STP_FILE = DATA_DIR / "Query 1 - STP Eligibility Matrix.csv"
DUTIES_FY26_FILE = DATA_DIR / "Duties_Paid_FY26.csv"
DUTIES_US_FILE = DATA_DIR / "Duties_Paid.csv"
DUTIES_NONUS_FILE = DATA_DIR / "Duties_Paid_NonUs.csv"
CLAIMS_DETAIL_FILE = DATA_DIR / "Recovery_Claims_Detail.csv"
DUTY_RATES_FILE = DATA_DIR / "Duty_Rates.csv"

PSC_WINDOW_DAYS = 180

# FY26 GEN duty rates — populated at runtime by databricks_loader.load_duty_rates().
# When running in CSV-only mode (no Databricks), these remain empty and a 25%
# default rate is used as fallback in lane-level calculations.
GEN_DUTY_RATES = {}

STP_DUTY_RATES = {
    "KH": 0.0,     # US GSP
    "JO": 0.0,     # JO-US FTA / CAFTA-DR
    "EG": 0.0,     # QIZ (Qualifying Industrial Zone)
    "LK": 0.0,     # US GSP
    "ID": 0.176,   # US GSP (partial — not all products qualify)
    "HN": 0.0,     # CAFTA-DR
    "GT": 0.0,     # CAFTA-DR
    "SV": 0.0,     # CAFTA-DR
    "NI": 0.0,     # CAFTA-DR
    "MX": 0.0,     # USMCA
    "IL": 0.0,     # US-Israel FTA / QIZ
    "TH": 0.0,     # US GSP
}


# ── HTS-based eligibility exclusions by program ─────────────────────
# US GSP excludes textiles/apparel (Ch 50-63) and footwear (Ch 64).
# FTAs like CAFTA-DR, USMCA, JO-US FTA cover ALL chapters.
# This must be applied to every calculation path.
_GSP_EXCLUDED_HS2 = set(f"{i:02d}" for i in list(range(50, 64)) + [64])

HTS_EXCLUSIONS_BY_PROGRAM = {
    "US_GSP": _GSP_EXCLUDED_HS2,
    "JP_GSP": set(),      # Japan GSP covers textiles for LDCs
    "GB_GSP": set(),      # UK GSP — covers textiles with ROO
    "EU_GSP": set(),      # EU GSP — covers textiles with ROO
    "CA_LDCT": set(),     # Canada LDC Tariff — covers textiles
}

# Programs that cover ALL HS chapters (no exclusions)
_FULL_COVERAGE_PROGRAMS = {
    "CAFTA_DR", "CAFTA-DR", "USMCA", "JO_US_FTA", "EG_QIZ",
    "CPTPP", "EU_VN", "EU_JP", "GB_VN",
    "ACFTA", "ASEAN", "AANZFTA", "AJCEP", "AKFTA", "AIFTA", "APTA",
    "IJEPA", "IN_JP_CEPA", "KR_IN_CEPA", "JPVNEPA",
    "AU_CN_FTA", "AU_TH_FTA", "CA_JO_FTA", "CN_KR_FTA", "CN_NZ_FTA",
    "CN_PK_FTA", "JP_MX", "JP_TH_FTA", "NZ_TH_FTA",
    "EU_TR", "EU_EG", "EU_JO", "EU_GE", "EU_MX", "EU_CAS", "RCEP",
}


def get_excluded_hs2_for_programs(programs: list) -> set:
    """Get HS-2 chapters excluded by ALL programs on a lane.

    A line item is eligible if ANY program on the lane covers it.
    So we only exclude chapters that are excluded by EVERY program.
    If a lane has US_GSP + JO_US_FTA, nothing is excluded (JO_US_FTA covers all).
    """
    if not programs:
        return set()

    # If any program has full coverage, nothing is excluded
    for p in programs:
        if p in _FULL_COVERAGE_PROGRAMS:
            return set()
        if p not in HTS_EXCLUSIONS_BY_PROGRAM:
            return set()

    # Intersect exclusions: only exclude what ALL programs exclude
    exclusion_sets = [HTS_EXCLUSIONS_BY_PROGRAM.get(p, set()) for p in programs]
    if not exclusion_sets:
        return set()

    result = exclusion_sets[0]
    for s in exclusion_sets[1:]:
        result = result & s

    return result


def is_line_eligible(hts_cd, programs: list) -> bool:
    """Check if a single line item is eligible under ANY of the lane's programs."""
    excluded = get_excluded_hs2_for_programs(programs)
    if not excluded:
        return True
    hts_2 = str(hts_cd)[:2] if pd.notna(hts_cd) else ""
    if not hts_2 or hts_2 == "na":
        return False  # conservative: unknown HTS = not eligible
    return hts_2 not in excluded


def compute_eligible_pct(duties_df: pd.DataFrame, origin: str, dest: str,
                         programs: list) -> float:
    """Compute what % of a lane's GEN goods value is eligible for its STP programs."""
    excluded = get_excluded_hs2_for_programs(programs)
    if not excluded:
        return 1.0

    gen = duties_df[
        (duties_df["country_of_origin_cd"] == origin)
        & (duties_df["country_of_destination_cd"] == dest)
        & (duties_df["tariff_type_cd"] == "GEN")
    ]
    if gen.empty:
        return 1.0

    gen_hts2 = gen["hts_cd"].astype(str).str[:2]
    total_gv = gen["goods_value_usd"].sum()
    if total_gv == 0:
        return 1.0

    excluded_gv = gen[gen_hts2.isin(excluded)]["goods_value_usd"].sum()
    unknown_gv = gen[gen_hts2.isin({"na", "", "Na", "NA"}) | gen["hts_cd"].isna()]["goods_value_usd"].sum()
    eligible = total_gv - excluded_gv - unknown_gv
    return max(eligible / total_gv, 0.0)


def _files_exist() -> bool:
    return STP_FILE.exists() and (DUTIES_FY26_FILE.exists() or DUTIES_US_FILE.exists())


# ── STP program name mapping (codes from Amber Road/E2Open) ─────────
PROGRAM_NAMES = {
    "CPTPP": "Comprehensive & Progressive TPP",
    "ACFTA": "ASEAN-China Free Trade Agreement",
    "EU_VN": "EU-Vietnam Free Trade Agreement",
    "EU_GSP": "EU Generalized System of Preferences",
    "ASEAN": "ASEAN Free Trade Area (AFTA)",
    "AIFTA": "ASEAN-India Free Trade Agreement",
    "AANZFTA": "ASEAN-Australia-NZ FTA",
    "AJCEP": "ASEAN-Japan Comprehensive EPA",
    "AKFTA": "ASEAN-Korea Free Trade Agreement",
    "JPVNEPA": "Japan-Vietnam EPA",
    "GB_VN": "UK-Vietnam Free Trade Agreement",
    "AU_CN_FTA": "Australia-China Free Trade Agreement",
    "GB_GSP": "UK Generalized System of Preferences",
    "CN_KR_FTA": "China-Korea Free Trade Agreement",
    "EU_TR": "EU-Turkey Customs Union",
    "APTA": "Asia-Pacific Trade Agreement",
    "CA_LDCT": "Canada Least Developed Country Tariff",
    "EU_EG": "EU-Egypt Association Agreement",
    "IJEPA": "Indonesia-Japan EPA",
    "EU_GE": "EU-Georgia Association Agreement",
    "CAFTA_DR": "Central America Free Trade Agreement",
    "JO_US_FTA": "Jordan-US Free Trade Agreement",
    "USMCA": "US-Mexico-Canada Agreement",
    "AU_TH_FTA": "Australia-Thailand FTA",
    "CN_NZ_FTA": "China-New Zealand FTA",
    "EG_QIZ": "Egypt Qualifying Industrial Zones",
    "US_GSP": "US Generalized System of Preferences",
    "JP_TH_FTA": "Japan-Thailand EPA",
    "EU_JO": "EU-Jordan Association Agreement",
    "CA_JO_FTA": "Canada-Jordan FTA",
    "RCEP": "Regional Comprehensive Economic Partnership",
    "KR_IN_CEPA": "Korea-India CEPA",
    "IN_JP_CEPA": "India-Japan CEPA",
    "JP_GSP": "Japan Generalized System of Preferences",
    "MY_JP_EPA": "Malaysia-Japan EPA",
}

# ── Geo mapping (since country_hierarchy_lookup was empty) ───────────
GEO_REGIONS = {
    "US": "North America", "CA": "North America", "MX": "North America",
    "HN": "Central America", "SV": "Central America", "GT": "Central America",
    "NI": "Central America", "CR": "Central America",
    "BE": "EMEA", "NL": "EMEA", "DE": "EMEA", "FR": "EMEA", "IT": "EMEA",
    "ES": "EMEA", "GB": "EMEA", "PL": "EMEA", "CZ": "EMEA", "SE": "EMEA",
    "AT": "EMEA", "DK": "EMEA", "FI": "EMEA", "IE": "EMEA", "PT": "EMEA",
    "GR": "EMEA", "RO": "EMEA", "HU": "EMEA", "SK": "EMEA", "BG": "EMEA",
    "HR": "EMEA", "SI": "EMEA", "LT": "EMEA", "LV": "EMEA", "EE": "EMEA",
    "TR": "EMEA", "EG": "EMEA", "ZA": "EMEA", "IL": "EMEA",
    "JP": "Asia Pacific", "KR": "Asia Pacific", "AU": "Asia Pacific",
    "NZ": "Asia Pacific", "SG": "Asia Pacific", "MY": "Asia Pacific",
    "TH": "Asia Pacific", "PH": "Asia Pacific", "ID": "Asia Pacific",
    "VN": "Asia Pacific", "IN": "Asia Pacific", "CN": "Greater China",
    "TW": "Greater China", "HK": "Greater China",
    "JO": "Middle East", "GE": "Caucasus",
    "KH": "Asia Pacific", "BD": "Asia Pacific", "LK": "Asia Pacific",
    "PK": "Asia Pacific", "BR": "Latin America",
}

COUNTRY_NAMES = {
    "VN": "Vietnam", "CN": "China", "ID": "Indonesia", "KH": "Cambodia",
    "IN": "India", "JO": "Jordan", "MX": "Mexico", "EG": "Egypt",
    "TH": "Thailand", "LK": "Sri Lanka", "PK": "Pakistan", "MY": "Malaysia",
    "TR": "Turkey", "BD": "Bangladesh", "HN": "Honduras", "SV": "El Salvador",
    "GT": "Guatemala", "NI": "Nicaragua", "GE": "Georgia", "PH": "Philippines",
    "TW": "Taiwan", "BR": "Brazil", "US": "United States", "JP": "Japan",
    "KR": "South Korea", "AU": "Australia", "NZ": "New Zealand",
    "SG": "Singapore", "BE": "Belgium", "NL": "Netherlands", "DE": "Germany",
    "GB": "United Kingdom", "CA": "Canada", "FR": "France", "IT": "Italy",
    "ES": "Spain", "PL": "Poland", "CZ": "Czech Republic", "SE": "Sweden",
}


def load_stp_eligibility() -> pd.DataFrame:
    """Load and summarize STP eligibility by origin -> destination -> program."""
    df = pd.read_csv(STP_FILE, encoding="utf-8", encoding_errors="replace")

    current = df[df["qualification_effective_start_dt"] >= "2025-01-01"].copy()

    summary = (
        current.groupby(["country_of_origin_cd", "country_of_destination_cd", "stp_cd"])
        .agg(
            total_products=("product_cd", "nunique"),
            total_factories=("supplier_cd", "nunique"),
            total_records=("product_cd", "count"),
            met=("system_decision_cd", lambda x: (x == "M").sum()),
            not_met=("system_decision_cd", lambda x: (x == "N").sum()),
            incomplete=("system_decision_cd", lambda x: (x == "I").sum()),
        )
        .reset_index()
    )

    summary["qualification_rate"] = summary["met"] / summary["total_records"]
    summary["program_name"] = summary["stp_cd"].map(PROGRAM_NAMES).fillna(summary["stp_cd"])
    summary["origin_name"] = summary["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(summary["country_of_origin_cd"])
    summary["dest_name"] = summary["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(summary["country_of_destination_cd"])
    summary["dest_region"] = summary["country_of_destination_cd"].map(GEO_REGIONS).fillna("Other")

    return summary


def load_duties_paid() -> pd.DataFrame:
    """Load and process actual duty payment data. Prefers FY26 file with dates."""
    dfs = []

    if DUTIES_FY26_FILE.exists():
        dfs.append(pd.read_csv(DUTIES_FY26_FILE, encoding="utf-8", encoding_errors="replace"))
    else:
        if DUTIES_US_FILE.exists():
            dfs.append(pd.read_csv(DUTIES_US_FILE, encoding="utf-8", encoding_errors="replace"))
        if DUTIES_NONUS_FILE.exists():
            dfs.append(pd.read_csv(DUTIES_NONUS_FILE, encoding="utf-8", encoding_errors="replace"))

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    df["payable_prc"] = pd.to_numeric(df["payable_prc"], errors="coerce").fillna(0)
    df["goods_value_usd"] = df["payable_prc"]
    df["customs_amt_usd"] = pd.to_numeric(df["customs_amt_usd"], errors="coerce").fillna(0)
    df["statistical_amt_usd"] = pd.to_numeric(df["statistical_amt_usd"], errors="coerce").fillna(0)
    df["tariff_rate_pct"] = pd.to_numeric(df["tariff_rate_pct"], errors="coerce").fillna(0)
    df["ad_valorem_base_prc"] = pd.to_numeric(df["ad_valorem_base_prc"], errors="coerce").fillna(0)

    df["gen_rate"] = df["country_of_origin_cd"].map(GEN_DUTY_RATES).fillna(0.25)
    df["estimated_gen_duty"] = df["goods_value_usd"] * df["gen_rate"]

    df["is_preferential"] = df["preferential_origin_ind"] == "Y"
    df["hts_chapter"] = df["hts_cd"].astype(str).str[:4]
    df["origin_name"] = df["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(df["country_of_origin_cd"])
    df["dest_name"] = df["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(df["country_of_destination_cd"])
    df["dest_region"] = df["country_of_destination_cd"].map(GEO_REGIONS).fillna("Other")

    if "acceptance_tmst" in df.columns:
        df["acceptance_dt"] = pd.to_datetime(df["acceptance_tmst"], errors="coerce")
        df["fiscal_year"] = df["acceptance_dt"].apply(_to_nike_fy)
        df["fiscal_quarter"] = df["acceptance_dt"].apply(_to_nike_fq)
        df["in_psc_window"] = df["acceptance_dt"] >= (pd.Timestamp.now() - pd.Timedelta(days=PSC_WINDOW_DAYS))
    else:
        df["acceptance_dt"] = pd.NaT
        df["fiscal_year"] = "Unknown"
        df["fiscal_quarter"] = "Unknown"
        df["in_psc_window"] = False

    return df


def _to_nike_fy(dt) -> str:
    if pd.isna(dt):
        return "Unknown"
    return f"FY{dt.year + 1}" if dt.month >= 6 else f"FY{dt.year}"


def _to_nike_fq(dt) -> str:
    if pd.isna(dt):
        return "Unknown"
    fy = dt.year + 1 if dt.month >= 6 else dt.year
    if dt.month in (6, 7, 8):
        q = "Q1"
    elif dt.month in (9, 10, 11):
        q = "Q2"
    elif dt.month in (12, 1, 2):
        q = "Q3"
    else:
        q = "Q4"
    return f"FY{fy} {q}"


def build_recovery_analysis(duties_df: pd.DataFrame, stp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify declarations within the PSC window where the product's origin->dest
    lane has STP eligibility but preferential treatment was NOT applied.

    CRITICAL: Filters line items by HTS eligibility. US GSP excludes apparel
    (Ch 50-63) and footwear (Ch 64). Only line items in eligible HS chapters
    are counted as recoverable. If a lane has a full-coverage FTA (e.g.,
    JO-US FTA), all chapters are eligible.
    """
    if "in_psc_window" not in duties_df.columns:
        return pd.DataFrame()

    psc_eligible = duties_df[
        (duties_df["in_psc_window"])
        & (duties_df["preferential_origin_ind"] == "N")
        & (duties_df["tariff_type_cd"] == "GEN")
    ].copy()

    if psc_eligible.empty:
        return pd.DataFrame()

    # Build lane→programs lookup
    stp_programs_by_lane = stp_df.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    )["stp_cd"].apply(list).to_dict()

    stp_lanes = set(stp_programs_by_lane.keys())

    psc_eligible["has_stp"] = psc_eligible.apply(
        lambda r: (r["country_of_origin_cd"], r["country_of_destination_cd"]) in stp_lanes,
        axis=1,
    )

    recoverable = psc_eligible[psc_eligible["has_stp"]].copy()

    if recoverable.empty:
        return pd.DataFrame()

    # Filter by HTS eligibility — only keep line items whose HS chapter
    # is covered by at least one program on the lane
    recoverable["_lane_programs"] = recoverable.apply(
        lambda r: stp_programs_by_lane.get(
            (r["country_of_origin_cd"], r["country_of_destination_cd"]), []
        ),
        axis=1,
    )
    recoverable["_hts_eligible"] = recoverable.apply(
        lambda r: is_line_eligible(r["hts_cd"], r["_lane_programs"]),
        axis=1,
    )
    recoverable = recoverable[recoverable["_hts_eligible"]].copy()

    if recoverable.empty:
        return pd.DataFrame()

    # Compute lane-specific STP rate for recoverable entries
    recoverable["stp_rate"] = recoverable.apply(
        lambda r: STP_DUTY_RATES.get(r["country_of_origin_cd"], r["gen_rate"]),
        axis=1,
    )
    recoverable["estimated_stp_duty"] = recoverable["goods_value_usd"] * recoverable["stp_rate"]
    recoverable["estimated_duty_savings"] = recoverable["estimated_gen_duty"] - recoverable["estimated_stp_duty"]

    qual_by_lane = stp_df.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    )["qualification_rate"].mean().reset_index().rename(
        columns={"qualification_rate": "stp_qualification_rate"}
    )

    recovery_summary = (
        recoverable.groupby(["country_of_origin_cd", "country_of_destination_cd", "origin_name", "dest_name"])
        .agg(
            declaration_count=("country_of_origin_cd", "count"),
            goods_value=("goods_value_usd", "sum"),
            gen_duty_paid=("estimated_gen_duty", "sum"),
            stp_duty=("estimated_stp_duty", "sum"),
            duty_savings=("estimated_duty_savings", "sum"),
            earliest=("acceptance_dt", "min"),
            latest=("acceptance_dt", "max"),
        )
        .reset_index()
    )

    recovery_summary = recovery_summary.merge(
        qual_by_lane,
        on=["country_of_origin_cd", "country_of_destination_cd"],
        how="left",
    )
    recovery_summary["stp_qualification_rate"] = recovery_summary["stp_qualification_rate"].fillna(0.5)

    recovery_summary["estimated_recovery"] = (
        recovery_summary["duty_savings"] * recovery_summary["stp_qualification_rate"]
    )

    recovery_summary = recovery_summary.sort_values("estimated_recovery", ascending=False)

    stp_by_lane = stp_df.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    ).agg(
        stp_programs=("stp_cd", lambda x: list(x.unique())),
        stp_program_names=("program_name", lambda x: list(x.unique())),
        stp_met=("met", "sum"),
        stp_total_products=("total_products", "sum"),
    ).reset_index()

    recovery_summary = recovery_summary.merge(
        stp_by_lane,
        on=["country_of_origin_cd", "country_of_destination_cd"],
        how="left",
    )
    recovery_summary["stp_programs"] = recovery_summary["stp_programs"].apply(
        lambda x: x if isinstance(x, list) else []
    )
    recovery_summary["stp_program_names"] = recovery_summary["stp_program_names"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    hts_by_lane = recoverable.groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    ).agg(
        hts_codes=("hts_cd", lambda x: list(x.unique())[:20]),
        avg_tariff_rate=("tariff_rate_pct", "mean"),
    ).reset_index()

    recovery_summary = recovery_summary.merge(
        hts_by_lane,
        on=["country_of_origin_cd", "country_of_destination_cd"],
        how="left",
    )

    return recovery_summary


def build_claim_package(recovery_df: pd.DataFrame, duties_df: pd.DataFrame,
                        stp_df: pd.DataFrame = None) -> list[dict]:
    """
    Build a structured claim-package for each recoverable lane.
    Each package contains the information needed to file a PSC with CBP.
    Only includes HTS-eligible line items per the lane's STP programs.
    """
    if recovery_df is None or recovery_df.empty:
        return []

    # Build lane→programs lookup for HTS filtering
    stp_programs_by_lane = {}
    if stp_df is not None:
        stp_programs_by_lane = stp_df.groupby(
            ["country_of_origin_cd", "country_of_destination_cd"]
        )["stp_cd"].apply(list).to_dict()

    packages = []
    for _, row in recovery_df.iterrows():
        origin = row["country_of_origin_cd"]
        dest = row["country_of_destination_cd"]
        lane_programs = stp_programs_by_lane.get((origin, dest), row.get("stp_programs", []))

        lane_decl = duties_df[
            (duties_df["country_of_origin_cd"] == origin)
            & (duties_df["country_of_destination_cd"] == dest)
            & (duties_df["in_psc_window"])
            & (duties_df["preferential_origin_ind"] == "N")
            & (duties_df["tariff_type_cd"] == "GEN")
        ].copy()

        # Filter to HTS-eligible line items only
        lane_decl = lane_decl[
            lane_decl["hts_cd"].apply(lambda h: is_line_eligible(h, lane_programs))
        ]

        hts_summary = lane_decl.groupby("hts_cd").agg(
            count=("hts_cd", "count"),
            total_duty=("estimated_gen_duty", "sum"),
            avg_rate=("gen_rate", lambda x: x.mean() * 100),
        ).reset_index().sort_values("total_duty", ascending=False)

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
            "window_start": row["earliest"].strftime("%Y-%m-%d") if pd.notna(row["earliest"]) else "N/A",
            "window_end": row["latest"].strftime("%Y-%m-%d") if pd.notna(row["latest"]) else "N/A",
            "hts_codes": hts_summary["hts_cd"].tolist()[:15],
            "hts_detail": hts_summary.head(10).to_dict("records"),
            "required_documents": _get_required_documents(origin, dest, row.get("stp_programs", [])),
            "filing_mechanism": _get_filing_mechanism(dest),
        })

    return packages


def _get_required_documents(origin: str, dest: str, stp_codes: list) -> list[dict]:
    """Return the documents needed for a PSC/recovery claim on this lane."""
    docs = [
        {
            "document": "Certificate of Origin (CoO)",
            "source": "E2Open / Amber Road",
            "status": "Verify in trade_document_reference",
            "description": f"Proof that goods from {COUNTRY_NAMES.get(origin, origin)} meet Rules of Origin for the applicable STP",
        },
        {
            "document": "Entry Summary (CBP Form 7501)",
            "source": "Broker / NTS",
            "status": "Available in declaration_entry",
            "description": "Original customs entry showing GEN duty was paid",
        },
        {
            "document": "Commercial Invoice",
            "source": "Supplier / E2Open",
            "status": "Required",
            "description": "Invoice showing transaction value and origin of goods",
        },
        {
            "document": "Bill of Lading / Airway Bill",
            "source": "Carrier / iTMS",
            "status": "Available in ILM",
            "description": "Transportation document confirming shipment routing",
        },
    ]

    if dest == "US":
        docs.append({
            "document": "Post-Summary Correction (PSC) Filing",
            "source": "CBP / ACE Portal",
            "status": "To file",
            "description": "Electronic correction to original entry within 180 days of liquidation (19 USC 1520(d))",
        })
    elif dest == "JP":
        docs.append({
            "document": "Amendment Request",
            "source": "Japan Customs",
            "status": "To file",
            "description": "Request to amend the import declaration to claim EPA preferential rate",
        })

    for stp_code in stp_codes[:3]:
        name = PROGRAM_NAMES.get(stp_code, stp_code)
        docs.append({
            "document": f"STP Qualification Proof — {name}",
            "source": "E2Open / Amber Road",
            "status": "Verify qualification_decision_ind = Met",
            "description": f"Confirm product qualifies under {name} (system_decision_cd = 'M' in trade_product_special_trade_programs)",
        })

    return docs


def _get_filing_mechanism(dest: str) -> dict:
    """Return the filing mechanism details for a destination country."""
    mechanisms = {
        "US": {
            "method": "Post-Summary Correction (PSC)",
            "authority": "US Customs & Border Protection (CBP)",
            "portal": "ACE (Automated Commercial Environment)",
            "deadline": "180 days from date of liquidation",
            "legal_basis": "19 USC 1520(d) — Refund of excessive duties",
            "typical_processing": "30-90 days after filing",
        },
        "JP": {
            "method": "Amendment of Import Declaration",
            "authority": "Japan Customs",
            "portal": "NACCS (Nippon Automated Cargo & Port Consolidated System)",
            "deadline": "5 years from original import date",
            "legal_basis": "Japan Customs Tariff Law, Article 7-14",
            "typical_processing": "1-3 months",
        },
    }
    return mechanisms.get(dest, {
        "method": "Post-Entry Amendment",
        "authority": f"Customs authority in {COUNTRY_NAMES.get(dest, dest)}",
        "portal": "Varies by country",
        "deadline": "Varies — typically 1-5 years",
        "legal_basis": "Local customs law",
        "typical_processing": "Varies",
    })


def load_claims_detail() -> pd.DataFrame | None:
    """Load the entry-level claims detail for filing-ready recovery packages."""
    if not CLAIMS_DETAIL_FILE.exists():
        return None

    df = pd.read_csv(CLAIMS_DETAIL_FILE, encoding="utf-8", encoding_errors="replace")

    df["payable_prc"] = pd.to_numeric(df["payable_prc"], errors="coerce").fillna(0)
    df["goods_value_usd"] = df["payable_prc"]
    df["customs_amt_usd"] = pd.to_numeric(df["customs_amt_usd"], errors="coerce").fillna(0)
    df["statistical_amt_usd"] = pd.to_numeric(df["statistical_amt_usd"], errors="coerce").fillna(0)
    df["acceptance_dt"] = pd.to_datetime(df["acceptance_tmst"], errors="coerce")

    df["gen_rate"] = df["country_of_origin_cd"].map(GEN_DUTY_RATES).fillna(0.25)
    df["stp_rate"] = df.apply(
        lambda r: STP_DUTY_RATES.get(r["country_of_origin_cd"], r["gen_rate"]),
        axis=1,
    )
    df["estimated_gen_duty"] = df["goods_value_usd"] * df["gen_rate"]
    df["estimated_duty_savings"] = df["goods_value_usd"] * (df["gen_rate"] - df["stp_rate"])

    df["origin_name"] = df["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(df["country_of_origin_cd"])
    df["dest_name"] = df["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(df["country_of_destination_cd"])

    df["days_since_acceptance"] = (pd.Timestamp.now(tz="UTC") - df["acceptance_dt"]).dt.days
    df["days_remaining"] = PSC_WINDOW_DAYS - df["days_since_acceptance"]
    df["days_remaining"] = df["days_remaining"].clip(lower=0)

    entry_col = "filing_reference_nbr" if df["filing_reference_nbr"].notna().any() else "declaration_identification_nbr"
    df["entry_id"] = df[entry_col]

    return df


def get_filing_list_for_lane(claims_df: pd.DataFrame, origin_cd: str, dest_cd: str,
                             lane_programs: list = None) -> pd.DataFrame:
    """
    Extract a filing-ready list of entries for a specific lane.
    This is what the broker takes to file PSCs.
    Filters out HTS codes excluded by the lane's STP programs.
    """
    if claims_df is None or claims_df.empty:
        return pd.DataFrame()

    lane = claims_df[
        (claims_df["country_of_origin_cd"] == origin_cd)
        & (claims_df["country_of_destination_cd"] == dest_cd)
    ].copy()

    # Filter to HTS-eligible line items only
    if lane_programs:
        lane = lane[
            lane["hts_cd"].apply(lambda h: is_line_eligible(h, lane_programs))
        ]

    if lane.empty:
        return pd.DataFrame()

    filing_list = lane.groupby(["entry_id", "filing_reference_nbr", "declaration_identification_nbr"]).agg(
        acceptance_date=("acceptance_dt", "first"),
        line_items=("line_item_nbr", "count"),
        total_duty=("estimated_gen_duty", "sum"),
        goods_value=("goods_value_usd", "sum"),
        hts_codes=("hts_cd", lambda x: ", ".join(sorted(str(v) for v in x.dropna().unique())[:5]) or "Pending classification"),
        days_remaining=("days_remaining", "min"),
        origin=("origin_name", "first"),
        destination=("dest_name", "first"),
        declaration_status=("declaration_status_desc", "first"),
    ).reset_index().sort_values("days_remaining", ascending=True)

    return filing_list


def build_lane_summary(duties_df: pd.DataFrame, stp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a lane-level summary joining duty payments with STP eligibility.
    This is the core dataframe that powers the POC visualizations.

    IMPORTANT: Each goods item has multiple tariff-type rows (GEN, PREF, ADD,
    ADD01-05, 012, 013, 056, 499, etc.). We use GEN rows for customs value
    and duty calculations to avoid double-counting.
    """
    gen_mask = duties_df["tariff_type_cd"] == "GEN"
    pref_mask = duties_df["tariff_type_cd"] == "PREF"
    gen_or_pref = duties_df[gen_mask | pref_mask]

    lane_duties = gen_or_pref.groupby(["country_of_origin_cd", "country_of_destination_cd"]).agg(
        total_rows=("country_of_origin_cd", "count"),
        unique_hts=("hts_chapter", "nunique"),
    ).reset_index()

    gen_by_lane = duties_df[gen_mask].groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    ).agg(
        goods_value_usd=("goods_value_usd", "sum"),
        gen_duty_usd=("estimated_gen_duty", "sum"),
        gen_row_count=("goods_value_usd", "count"),
    ).reset_index()

    pref_by_lane = duties_df[pref_mask].groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    ).agg(
        pref_goods_value_usd=("goods_value_usd", "sum"),
        pref_row_count=("goods_value_usd", "count"),
    ).reset_index()

    pref_y_by_lane = gen_or_pref[gen_or_pref["is_preferential"]].groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    ).size().reset_index(name="pref_y_count")

    pref_n_by_lane = gen_or_pref[~gen_or_pref["is_preferential"]].groupby(
        ["country_of_origin_cd", "country_of_destination_cd"]
    ).size().reset_index(name="pref_n_count")

    lane_duties = lane_duties.merge(gen_by_lane, on=["country_of_origin_cd", "country_of_destination_cd"], how="left")
    lane_duties = lane_duties.merge(pref_by_lane, on=["country_of_origin_cd", "country_of_destination_cd"], how="left")
    lane_duties = lane_duties.merge(pref_y_by_lane, on=["country_of_origin_cd", "country_of_destination_cd"], how="left")
    lane_duties = lane_duties.merge(pref_n_by_lane, on=["country_of_origin_cd", "country_of_destination_cd"], how="left")

    lane_duties["goods_value_usd"] = lane_duties["goods_value_usd"].fillna(0)
    lane_duties["gen_duty_usd"] = lane_duties["gen_duty_usd"].fillna(0)
    lane_duties["pref_goods_value_usd"] = lane_duties["pref_goods_value_usd"].fillna(0)
    lane_duties["pref_y_count"] = lane_duties["pref_y_count"].fillna(0)
    lane_duties["pref_n_count"] = lane_duties["pref_n_count"].fillna(0)

    lane_duties["utilization_pct"] = lane_duties["pref_y_count"] / (
        lane_duties["pref_y_count"] + lane_duties["pref_n_count"]
    )
    lane_duties["utilization_pct"] = lane_duties["utilization_pct"].fillna(0)

    # STP program enrichment — merge BEFORE computing STP duty
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
        lane_stp,
        on=["country_of_origin_cd", "country_of_destination_cd"],
        how="left",
    )

    lanes["has_stp"] = lanes["stp_programs"].notna()
    lanes["stp_programs"] = lanes["stp_programs"].apply(lambda x: x if isinstance(x, list) else [])
    lanes["stp_program_names"] = lanes["stp_program_names"].apply(lambda x: x if isinstance(x, list) else [])
    lanes["stp_qualification_rate"] = lanes["stp_qualification_rate"].fillna(0)
    lanes["stp_met"] = lanes["stp_met"].fillna(0)
    lanes["stp_not_met"] = lanes["stp_not_met"].fillna(0)

    # Lane-specific STP rate: only lanes with an actual STP program get a
    # preferential rate.  Lanes without STP → stp_rate = gen_rate → no savings.
    lanes["gen_rate"] = lanes["country_of_origin_cd"].map(GEN_DUTY_RATES).fillna(0.25)
    lanes["stp_rate"] = lanes.apply(
        lambda r: STP_DUTY_RATES.get(r["country_of_origin_cd"], r["gen_rate"])
                  if r["has_stp"] else r["gen_rate"],
        axis=1,
    )
    lanes["origin_name"] = lanes["country_of_origin_cd"].map(COUNTRY_NAMES).fillna(lanes["country_of_origin_cd"])
    lanes["dest_name"] = lanes["country_of_destination_cd"].map(COUNTRY_NAMES).fillna(lanes["country_of_destination_cd"])
    lanes["dest_region"] = lanes["country_of_destination_cd"].map(GEO_REGIONS).fillna("Other")

    # Compute eligible_pct from actual HTS distribution vs program exclusions.
    # This determines what fraction of the lane's goods value is actually covered
    # by the STP program(s). US GSP excludes apparel (Ch 50-63) & footwear (Ch 64).
    lanes["eligible_pct"] = lanes.apply(
        lambda r: compute_eligible_pct(
            duties_df, r["country_of_origin_cd"], r["country_of_destination_cd"],
            r["stp_programs"]
        ) if r["has_stp"] else 0.0,
        axis=1,
    )
    lanes["eligible_goods_value_usd"] = lanes["goods_value_usd"] * lanes["eligible_pct"]

    # STP duty: preferential rate on eligible portion + GEN rate on excluded portion
    lanes["stp_duty_usd"] = (
        lanes["eligible_goods_value_usd"] * lanes["stp_rate"]
        + (lanes["goods_value_usd"] - lanes["eligible_goods_value_usd"]) * lanes["gen_rate"]
    )
    # Savings realized: duty avoided on the eligible portion that DID claim STP
    lanes["savings_realized_usd"] = np.where(
        lanes["has_stp"],
        (lanes["eligible_goods_value_usd"]
         * (lanes["gen_rate"] - lanes["stp_rate"])
         * lanes["utilization_pct"]).clip(lower=0),
        0.0,
    )
    # Excess duty: savings potential on the eligible portion only, adjusted for utilization
    lanes["excess_duty_usd"] = np.where(
        lanes["has_stp"],
        (lanes["eligible_goods_value_usd"]
         * (lanes["gen_rate"] - lanes["stp_rate"])
         * (1 - lanes["utilization_pct"])).clip(lower=0),
        0.0,
    )

    lanes["is_gap_lane"] = lanes["has_stp"] & (lanes["utilization_pct"] < 0.5)

    return lanes.sort_values("goods_value_usd", ascending=False)


def build_program_summary(stp_df: pd.DataFrame, lanes_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize by STP program across all lanes."""
    prog = stp_df.groupby(["stp_cd", "program_name"]).agg(
        lane_count=("country_of_origin_cd", "count"),
        total_products=("total_products", "sum"),
        total_factories=("total_factories", "sum"),
        met=("met", "sum"),
        not_met=("not_met", "sum"),
        incomplete=("incomplete", "sum"),
        avg_qualification_rate=("qualification_rate", "mean"),
        origins=("country_of_origin_cd", "nunique"),
        destinations=("country_of_destination_cd", "nunique"),
    ).reset_index().sort_values("total_products", ascending=False)

    return prog


def load_all():
    """Load all real data and return structured dict."""
    if not _files_exist():
        return None

    stp_df = load_stp_eligibility()
    duties_df = load_duties_paid()
    lanes_df = build_lane_summary(duties_df, stp_df)
    programs_df = build_program_summary(stp_df, lanes_df)
    recovery_df = build_recovery_analysis(duties_df, stp_df)
    claim_packages = build_claim_package(recovery_df, duties_df, stp_df)
    claims_detail = load_claims_detail()

    gen_pref_df = duties_df[duties_df["tariff_type_cd"].isin(["GEN", "PREF"])]
    gen_df = duties_df[duties_df["tariff_type_cd"] == "GEN"]

    has_dates = "acceptance_dt" in duties_df.columns and duties_df["acceptance_dt"].notna().any()
    if has_dates:
        earliest = duties_df["acceptance_dt"].min()
        latest = duties_df["acceptance_dt"].max()
        date_range = f"{earliest.strftime('%b %Y')} - {latest.strftime('%b %Y')}"
        fy_counts = gen_pref_df["fiscal_quarter"].value_counts().to_dict()
    else:
        date_range = "Unknown"
        fy_counts = {}

    total_goods_value = float(gen_df["goods_value_usd"].sum())
    # Use lane-level totals which properly account for lane-specific STP rates
    total_gen_duty = float(lanes_df["gen_duty_usd"].sum())
    total_stp_duty = float(lanes_df["stp_duty_usd"].sum())
    total_savings_potential = total_gen_duty - total_stp_duty
    total_savings_realized = float(lanes_df["savings_realized_usd"].sum())

    return {
        "stp_eligibility": stp_df,
        "duties_paid": duties_df,
        "lanes": lanes_df,
        "programs": programs_df,
        "recovery": recovery_df,
        "claim_packages": claim_packages,
        "claims_detail": claims_detail,
        "gen_pref_df": gen_pref_df,
        "meta": {
            "stp_total_records": len(pd.read_csv(STP_FILE, usecols=[0], encoding="utf-8", encoding_errors="replace")),
            "duties_total_rows": len(duties_df),
            "duties_total_records": int((duties_df["tariff_type_cd"].isin(["GEN", "PREF"])).sum()),
            "duties_sample_limited": len(duties_df) == 500_000,
            "stp_date_range": "Jan 2025 - Dec 2026",
            "duties_date_range": date_range,
            "fiscal_quarter_counts": fy_counts,
            "last_etl": "Apr 1, 2026",
            "source": "non_published_domain.trade_customs (Databricks)",
            "unique_programs": stp_df["stp_cd"].nunique(),
            "unique_origins": stp_df["country_of_origin_cd"].nunique(),
            "unique_destinations": stp_df["country_of_destination_cd"].nunique(),
            "psc_window_days": PSC_WINDOW_DAYS,
            "psc_eligible_rows": int(duties_df["in_psc_window"].sum()) if "in_psc_window" in duties_df.columns else 0,
            "total_goods_value": total_goods_value,
            "total_gen_duty": total_gen_duty,
            "total_stp_duty": total_stp_duty,
            "total_savings_potential": total_savings_potential,
            "total_savings_realized": total_savings_realized,
        },
    }
