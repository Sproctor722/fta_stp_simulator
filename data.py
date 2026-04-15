"""
Data layer for the STP/FTA Simulator POC.

Validated against Taruna Vazirani's STP domain expertise (Apr 1, 2026).
Key facts confirmed:
  - $487M paid on products with active STPs (annual broker data review)
  - $50M savings commitment for FY27 under FTO program
  - Root causes: sourcing decisions, missing data in trade systems, HTS misclass
  - Data pipeline: Trade Automation (Amber Road/E2Open) → NTS → NTN → NDF → CCH
  - S.C.O.P.E. assumes "STP always applies" (e.g., VN→EU) — doesn't model gaps
  - Certificate of Origin compliance is a major failure point (missing stamps, etc.)

Synthetic data calibrated to approximate Nike's sourcing profile.
Swap with real Databricks/SOLE/NDF sources when available.
"""

import pandas as pd
import numpy as np
from typing import Optional

# ---------------------------------------------------------------------------
# Reference data: Trade Programs
# ---------------------------------------------------------------------------
TRADE_PROGRAMS = pd.DataFrame([
    {"program_id": "CAFTA-DR",  "name": "Central America Free Trade Agreement",  "type": "FTA", "regions": "Central America → US", "preferential_rate_pct": 0.0},
    {"program_id": "USMCA",     "name": "US-Mexico-Canada Agreement",             "type": "FTA", "regions": "MX/CA → US",           "preferential_rate_pct": 0.0},
    {"program_id": "EU-VN-FTA", "name": "EU-Vietnam Free Trade Agreement",        "type": "FTA", "regions": "VN → EU",              "preferential_rate_pct": 0.0},
    {"program_id": "RCEP",      "name": "Regional Comprehensive Economic Partnership", "type": "FTA", "regions": "APAC intra-regional", "preferential_rate_pct": 2.5},
    {"program_id": "JFTA",      "name": "Jordan Free Trade Agreement",            "type": "FTA", "regions": "JO → US",              "preferential_rate_pct": 0.0},
    {"program_id": "GSP",       "name": "Generalized System of Preferences",      "type": "STP", "regions": "Developing → US/EU",   "preferential_rate_pct": 0.0},
    {"program_id": "CPTPP",     "name": "Comprehensive & Progressive TPP",        "type": "FTA", "regions": "VN/MY → CPTPP members","preferential_rate_pct": 1.0},
    {"program_id": "EU-ID-CEPA","name": "EU-Indonesia CEPA (Pending)",            "type": "FTA", "regions": "ID → EU",              "preferential_rate_pct": 3.5},
    {"program_id": "KR-VN-FTA", "name": "Korea-Vietnam FTA",                      "type": "FTA", "regions": "VN → KR",              "preferential_rate_pct": 0.0},
    {"program_id": "APTA",      "name": "Asia-Pacific Trade Agreement",           "type": "STP", "regions": "CN/IN/KR/BD → members","preferential_rate_pct": 3.0},
    {"program_id": "ACFTA",     "name": "ASEAN-China Free Trade Agreement",       "type": "FTA", "regions": "ASEAN → CN",           "preferential_rate_pct": 0.0},
    {"program_id": "EU-EBA",    "name": "EU Everything But Arms (LDCs)",          "type": "STP", "regions": "LDCs → EU",            "preferential_rate_pct": 0.0},
    {"program_id": "UK-VN-FTA", "name": "UK-Vietnam Free Trade Agreement",        "type": "FTA", "regions": "VN → UK",              "preferential_rate_pct": 0.0},
    {"program_id": "AGOA",      "name": "African Growth & Opportunity Act",       "type": "STP", "regions": "Africa → US",          "preferential_rate_pct": 0.0},
    {"program_id": "FTZ-US",    "name": "US Foreign Trade Zone Program",          "type": "FTZ", "regions": "US DCs",               "preferential_rate_pct": None},
    {"program_id": "FTZ-EU",    "name": "EU Free Zone / Bonded Warehouse",        "type": "FTZ", "regions": "EU DCs",               "preferential_rate_pct": None},
])

# ---------------------------------------------------------------------------
# Reference data: Countries / Origins
# ---------------------------------------------------------------------------
COUNTRIES = {
    "VN": "Vietnam",
    "CN": "China",
    "ID": "Indonesia",
    "IN": "India",
    "KH": "Cambodia",
    "BD": "Bangladesh",
    "JO": "Jordan",
    "MX": "Mexico",
    "BR": "Brazil",
    "TH": "Thailand",
    "TW": "Taiwan",
    "EG": "Egypt",
}

DESTINATION_MARKETS = {
    "US": "North America (US)",
    "EU": "Europe (EMEA)",
    "GC": "Greater China",
    "JP": "Japan",
    "KR": "South Korea",
    "APLA": "Asia Pacific & Latin America",
}

# ---------------------------------------------------------------------------
# Product categories with typical HS codes and duty profiles
# ---------------------------------------------------------------------------
PRODUCT_CATEGORIES = pd.DataFrame([
    {"category": "Footwear - Athletic", "hs_chapter": "6404",  "statutory_rate_us": 20.0, "statutory_rate_eu": 16.9, "statutory_rate_gc": 24.0, "volume_pct": 0.45},
    {"category": "Footwear - Casual",   "hs_chapter": "6403",  "statutory_rate_us": 8.5,  "statutory_rate_eu": 8.0,  "statutory_rate_gc": 24.0, "volume_pct": 0.15},
    {"category": "Apparel - Knit",      "hs_chapter": "6110",  "statutory_rate_us": 32.0, "statutory_rate_eu": 12.0, "statutory_rate_gc": 16.0, "volume_pct": 0.20},
    {"category": "Apparel - Woven",     "hs_chapter": "6205",  "statutory_rate_us": 19.7, "statutory_rate_eu": 12.0, "statutory_rate_gc": 16.0, "volume_pct": 0.10},
    {"category": "Equipment / Bags",    "hs_chapter": "4202",  "statutory_rate_us": 17.6, "statutory_rate_eu": 3.7,  "statutory_rate_gc": 10.0, "volume_pct": 0.07},
    {"category": "Accessories",         "hs_chapter": "6505",  "statutory_rate_us": 6.8,  "statutory_rate_eu": 4.7,  "statutory_rate_gc": 14.0, "volume_pct": 0.03},
])


def _statutory_rate(category_row: pd.Series, dest: str) -> float:
    col_map = {"US": "statutory_rate_us", "EU": "statutory_rate_eu", "GC": "statutory_rate_gc"}
    col = col_map.get(dest, "statutory_rate_us")
    return category_row.get(col, 15.0)


# ---------------------------------------------------------------------------
# STP Eligibility Matrix: which program applies for which origin→dest
# ---------------------------------------------------------------------------
_ELIGIBILITY_RAW = [
    # (origin, destination, program_id, eligible, utilization_pct, roo_compliant)
    ("VN", "US",   "CPTPP",     False, 0,    False),  # US not in CPTPP
    ("VN", "EU",   "EU-VN-FTA", True,  0.90, True),   # Taruna: "90% is true" for VN→EU STP
    ("VN", "JP",   "RCEP",      True,  0.65, True),
    ("VN", "KR",   "KR-VN-FTA", True,  0.80, True),
    ("VN", "GC",   "RCEP",      True,  0.30, True),   # Low utilization in GC — key gap area
    ("VN", "GC",   "ACFTA",     True,  0.35, True),   # Older agreement, partially overlaps RCEP
    ("VN", "APLA", "CPTPP",     True,  0.55, True),
    ("CN", "US",   None,        False, 0,    False),   # No FTA
    ("CN", "EU",   None,        False, 0,    False),
    ("CN", "GC",   None,        False, 0,    False),   # Domestic, no STP needed
    ("CN", "JP",   "RCEP",      True,  0.40, True),
    ("CN", "KR",   "RCEP",      True,  0.35, True),
    ("CN", "APLA", "APTA",      True,  0.25, False),   # ROO compliance issues
    ("ID", "US",   "GSP",       True,  0.60, True),
    ("ID", "EU",   "EU-ID-CEPA",False, 0,    False),   # Pending ratification
    ("ID", "JP",   "RCEP",      True,  0.70, True),
    ("ID", "GC",   "RCEP",      True,  0.45, True),
    ("ID", "APLA", "RCEP",      True,  0.50, True),
    ("KH", "US",   "GSP",       True,  0.55, True),
    ("KH", "EU",   "GSP",       True,  0.65, True),    # EBA scheme
    ("BD", "EU",   "GSP",       True,  0.70, True),    # EBA scheme
    ("BD", "US",   "GSP",       True,  0.40, False),   # ROO gaps
    ("JO", "US",   "JFTA",      True,  0.85, True),
    ("MX", "US",   "USMCA",     True,  0.90, True),
    ("IN", "US",   None,        False, 0,    False),   # GSP suspended
    ("IN", "EU",   None,        False, 0,    False),
    ("IN", "JP",   "RCEP",      False, 0,    False),   # India not in RCEP
    ("TH", "US",   "GSP",       True,  0.50, True),
    ("TH", "JP",   "RCEP",      True,  0.60, True),
    ("EG", "EU",   "GSP",       True,  0.45, True),
]

STP_ELIGIBILITY = pd.DataFrame(
    _ELIGIBILITY_RAW,
    columns=["origin", "destination", "program_id", "eligible", "utilization_pct", "roo_compliant"],
)


# ---------------------------------------------------------------------------
# Synthetic trade lane duty data (represents ~$2.8B total duty across Nike)
# ---------------------------------------------------------------------------
np.random.seed(42)

def generate_trade_lane_data() -> pd.DataFrame:
    """Generate realistic trade lane duty data for the POC."""
    rows = []

    # Distribution of Nike sourcing (approximate, based on public data)
    sourcing_mix = {
        "VN": 0.50, "CN": 0.16, "ID": 0.12, "IN": 0.06,
        "KH": 0.04, "BD": 0.03, "JO": 0.02, "MX": 0.02,
        "TH": 0.03, "EG": 0.01, "BR": 0.01,
    }

    dest_mix = {
        "US": 0.40, "EU": 0.28, "GC": 0.15, "JP": 0.07,
        "KR": 0.05, "APLA": 0.05,
    }

    total_cogs = 7_200_000_000  # ~$7.2B approximate COGS

    for origin, origin_share in sourcing_mix.items():
        for dest, dest_share in dest_mix.items():
            for _, cat in PRODUCT_CATEGORIES.iterrows():
                lane_volume = total_cogs * origin_share * dest_share * cat["volume_pct"]
                if lane_volume < 100_000:
                    continue

                stat_rate = _statutory_rate(cat, dest)

                elig = STP_ELIGIBILITY[
                    (STP_ELIGIBILITY["origin"] == origin)
                    & (STP_ELIGIBILITY["destination"] == dest)
                    & (STP_ELIGIBILITY["eligible"] == True)
                ]

                if not elig.empty:
                    best = elig.iloc[0]
                    prog = TRADE_PROGRAMS[TRADE_PROGRAMS["program_id"] == best["program_id"]]
                    pref_rate = prog["preferential_rate_pct"].iloc[0] if not prog.empty and pd.notna(prog["preferential_rate_pct"].iloc[0]) else stat_rate * 0.3
                    util = best["utilization_pct"]
                    roo = best["roo_compliant"]
                    program_id = best["program_id"]
                else:
                    pref_rate = stat_rate
                    util = 0.0
                    roo = False
                    program_id = None

                duty_at_statutory = lane_volume * (stat_rate / 100)
                duty_at_preferential = lane_volume * (pref_rate / 100)
                actual_duty = duty_at_statutory * (1 - util) + duty_at_preferential * util
                max_savings = duty_at_statutory - duty_at_preferential
                realized_savings = max_savings * util
                unrealized_savings = max_savings * (1 - util)

                leakage_reason = None
                if program_id and util < 1.0:
                    reason_weights = [0.30, 0.10, 0.15, 0.15, 0.10, 0.05, 0.05, 0.03, 0.04, 0.03]
                    leakage_reason = np.random.choice(LEAKAGE_REASONS, p=reason_weights)

                rows.append({
                    "origin": origin,
                    "origin_name": COUNTRIES.get(origin, origin),
                    "destination": dest,
                    "destination_name": DESTINATION_MARKETS.get(dest, dest),
                    "category": cat["category"],
                    "hs_chapter": cat["hs_chapter"],
                    "lane_volume_usd": round(lane_volume),
                    "statutory_rate_pct": stat_rate,
                    "preferential_rate_pct": pref_rate,
                    "program_id": program_id,
                    "program_eligible": program_id is not None,
                    "utilization_pct": util,
                    "roo_compliant": roo,
                    "duty_at_statutory": round(duty_at_statutory),
                    "duty_at_preferential": round(duty_at_preferential),
                    "actual_duty_paid": round(actual_duty),
                    "max_possible_savings": round(max_savings),
                    "realized_savings": round(realized_savings),
                    "unrealized_savings": round(unrealized_savings),
                    "leakage_reason": leakage_reason,
                })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Data pipeline: confirmed by Taruna (Apr 2026)
# Trade Automation (Amber Road/E2Open) → NTS → NTN → NDF (Databricks) → CCH → Broker
# ---------------------------------------------------------------------------
DATA_PIPELINE_STAGES = [
    {"stage": "Trade Automation", "system": "Amber Road / E2Open", "description": "STP eligibility check, CoO generation, factory interaction — input to ILM (our team)"},
    {"stage": "NTS", "system": "Nike Trade Services", "description": "Nike's internal trade data aggregation"},
    {"stage": "NTN", "system": "Nike Trade Network", "description": "Trade compliance data routing"},
    {"stage": "NDF", "system": "Databricks (SOLE)", "description": "Foundational data layer — our team's domain"},
    {"stage": "CCH", "system": "CCH Filing System", "description": "Filing pack assembly for customs brokers"},
    {"stage": "Broker", "system": "External Brokers", "description": "Customs entry filing with STP claims"},
]

# ---------------------------------------------------------------------------
# Companion programs (context from Taruna, not simulated yet)
# ---------------------------------------------------------------------------
COMPANION_PROGRAMS = {
    "first_sale": {
        "name": "First Sale Valuation",
        "current_savings": 35_000_000,
        "expansion_target": 50_000_000,
        "status": "Active — Ultra expansion in progress",
        "description": "Lower dutiable value by using first-sale price instead of transaction value",
    },
    "duty_drawback": {
        "name": "Duty Drawback",
        "current_savings": 5_000_000,
        "expansion_target": 15_000_000,
        "status": "Active — returns only, small scale",
        "description": "Recover duties paid on returned/re-exported goods",
    },
    "ftz": {
        "name": "Foreign Trade Zones",
        "current_savings": 0,
        "expansion_target": 20_000_000,
        "status": "Not pursued — volume may not justify",
        "description": "Duty deferral and inverted tariff benefits at US DCs",
    },
}

# ---------------------------------------------------------------------------
# Root causes for STP leakage (from Taruna's broker data review)
# ---------------------------------------------------------------------------
LEAKAGE_REASONS = [
    "Sourcing decision did not consider STP eligibility",
    "HTS code misclassification",
    "Missing data in trade systems (NTS/NTN)",
    "Certificate of Origin not filed or incomplete",
    "Factory failed Rules of Origin compliance",
    "Certificate stamp missing or invalid",
    "STP qualification not sent to broker in filing pack",
    "Trade agreement not configured in Trade Automation",
    "PO data incomplete — origin/destination mismatch",
    "Factory did not confirm ROO in time for shipment",
]


def get_trade_lane_data() -> pd.DataFrame:
    return generate_trade_lane_data()


# ---------------------------------------------------------------------------
# Sourcing shift simulation
# ---------------------------------------------------------------------------
def simulate_sourcing_shift(
    df: pd.DataFrame,
    from_origin: str,
    to_origin: str,
    shift_pct: float,
    categories: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Simulate moving `shift_pct` of production from `from_origin` to `to_origin`.
    Returns a comparison dataframe with before/after duty impacts per destination.
    """
    mask = df["origin"] == from_origin
    if categories:
        mask = mask & df["category"].isin(categories)

    shifting_lanes = df[mask].copy()
    if shifting_lanes.empty:
        return pd.DataFrame()

    results = []
    for _, lane in shifting_lanes.iterrows():
        dest = lane["destination"]
        cat = lane["category"]
        shifted_volume = lane["lane_volume_usd"] * shift_pct

        before_stat_rate = lane["statutory_rate_pct"]
        before_pref_rate = lane["preferential_rate_pct"]
        before_util = lane["utilization_pct"]
        before_program = lane["program_id"]

        new_elig = STP_ELIGIBILITY[
            (STP_ELIGIBILITY["origin"] == to_origin)
            & (STP_ELIGIBILITY["destination"] == dest)
            & (STP_ELIGIBILITY["eligible"] == True)
        ]

        cat_row = PRODUCT_CATEGORIES[PRODUCT_CATEGORIES["category"] == cat]
        after_stat_rate = _statutory_rate(cat_row.iloc[0], dest) if not cat_row.empty else before_stat_rate

        if not new_elig.empty:
            best = new_elig.iloc[0]
            prog = TRADE_PROGRAMS[TRADE_PROGRAMS["program_id"] == best["program_id"]]
            after_pref_rate = prog["preferential_rate_pct"].iloc[0] if not prog.empty and pd.notna(prog["preferential_rate_pct"].iloc[0]) else after_stat_rate * 0.3
            after_util = best["utilization_pct"]
            after_roo = best["roo_compliant"]
            after_program = best["program_id"]
        else:
            after_pref_rate = after_stat_rate
            after_util = 0.0
            after_roo = False
            after_program = None

        before_duty = shifted_volume * (before_stat_rate / 100) * (1 - before_util) + \
                      shifted_volume * (before_pref_rate / 100) * before_util
        after_duty = shifted_volume * (after_stat_rate / 100) * (1 - after_util) + \
                     shifted_volume * (after_pref_rate / 100) * after_util

        results.append({
            "destination": dest,
            "destination_name": DESTINATION_MARKETS.get(dest, dest),
            "category": cat,
            "shifted_volume_usd": round(shifted_volume),
            "before_origin": from_origin,
            "before_program": before_program,
            "before_statutory_rate": before_stat_rate,
            "before_preferential_rate": before_pref_rate,
            "before_utilization": before_util,
            "before_duty": round(before_duty),
            "after_origin": to_origin,
            "after_program": after_program,
            "after_statutory_rate": after_stat_rate,
            "after_preferential_rate": after_pref_rate,
            "after_utilization": after_util,
            "after_roo_compliant": after_roo,
            "after_duty": round(after_duty),
            "duty_delta": round(after_duty - before_duty),
            "program_change": f"{before_program or 'None'} → {after_program or 'None'}",
        })

    return pd.DataFrame(results)
