"""
FTA Rules of Origin data layer.

Parses the 37 FTA/STP JSON summaries from Brandon Brown's Trade AI Hub
into queryable structures for the STP/FTA Simulator.

Each JSON file contains:
  - overview.summary: FTA-level Rules of Origin overview
  - overview.glossary: term definitions
  - overview.spec_count / chapter_count: rule complexity metrics
  - chapters.<HS chapter>.summary: chapter-specific qualification rules
  - chapters.<HS chapter>.glossary: chapter-specific terms
  - chapters.<HS chapter>.spec_count: rule count for that chapter

Product categories map to HS chapters:
  - Footwear: Chapter 64
  - Apparel:  Chapters 61 (knitted), 62 (woven)
  - Equipment/Accessories: Chapter 63 (other textiles), 95 (sports)
"""

import json
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


_DEFAULT_FTA_DIR = (
    r"c:\Users\sproc2\Downloads\Free Trade Agreement Summaries"
    if os.name == "nt"
    else "/Workspace/Users/shannon.proctor/fta-data"
)
FTA_DATA_DIR = Path(os.environ.get("FTA_DATA_DIR", _DEFAULT_FTA_DIR))

HS_CHAPTER_LABELS = {
    "61": "Knitted Apparel",
    "62": "Woven Apparel",
    "63": "Other Textile Articles",
    "64": "Footwear",
    "95": "Toys / Games / Sports Equipment",
    "99": "Special Provisions / De Minimis",
}

PRODUCT_CATEGORY_CHAPTERS = {
    "Footwear":    ["64"],
    "Apparel":     ["61", "62"],
    "Equipment":   ["63", "95"],
    "Accessories": ["63", "95"],
}

# Maps FTA file stems to the origin→destination trade lanes they cover.
# Keys match JSON filenames (without .json). Values are lists of
# (origin_countries, destination_countries) tuples using ISO-2 codes.
# "ASEAN" and similar group codes are expanded below.
_ASEAN = ["BN", "KH", "ID", "LA", "MY", "MM", "PH", "SG", "TH", "VN"]
_CAFTA = ["CR", "DO", "SV", "GT", "HN", "NI"]
_CPTPP = ["AU", "BN", "CA", "CL", "JP", "MY", "MX", "NZ", "PE", "SG", "VN"]
_EU = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
]

FTA_LANE_COVERAGE: dict[str, dict] = {
    "CAFTA_DR":    {"origins": _CAFTA,  "destinations": ["US"]},
    "USMCA":       {"origins": ["US", "MX", "CA"], "destinations": ["US", "MX", "CA"]},
    "JO_US_FTA":   {"origins": ["JO"],  "destinations": ["US"]},
    "US_GSP":      {"origins": ["KH", "TH", "ID", "IN", "BD", "LK", "PK", "PH", "EG", "JO", "GE"],
                    "destinations": ["US"]},
    "CPTPP":       {"origins": _CPTPP,  "destinations": _CPTPP},
    "EU_VN":       {"origins": ["VN"],  "destinations": _EU},
    "EU_GSP":      {"origins": ["KH", "BD", "LK", "PK", "ID", "IN", "VN", "MM", "PH"],
                    "destinations": _EU},
    "EU_JP":       {"origins": ["JP"] + _EU, "destinations": _EU + ["JP"]},
    "EU_EG":       {"origins": ["EG"],  "destinations": _EU},
    "EU_JO":       {"origins": ["JO"],  "destinations": _EU},
    "EU_TR":       {"origins": ["TR"],  "destinations": _EU},
    "EU_GE":       {"origins": ["GE"],  "destinations": _EU},
    "EU_MX":       {"origins": ["MX"],  "destinations": _EU},
    "EU_CAS":      {"origins": ["KH", "VN", "PH", "MM", "LA"],
                    "destinations": _EU},
    "GB_VN":       {"origins": ["VN"],  "destinations": ["GB"]},
    "GB_GSP":      {"origins": ["KH", "BD", "LK", "PK", "ID", "IN", "VN", "MM"],
                    "destinations": ["GB"]},
    "ACFTA":       {"origins": _ASEAN,  "destinations": ["CN"]},
    "ASEAN":       {"origins": _ASEAN,  "destinations": _ASEAN},
    "AANZFTA":     {"origins": _ASEAN + ["AU", "NZ"],
                    "destinations": _ASEAN + ["AU", "NZ"]},
    "AJCEP":       {"origins": _ASEAN + ["JP"],
                    "destinations": _ASEAN + ["JP"]},
    "AKFTA":       {"origins": _ASEAN + ["KR"],
                    "destinations": _ASEAN + ["KR"]},
    "AIFTA":       {"origins": _ASEAN + ["IN"],
                    "destinations": _ASEAN + ["IN"]},
    "APTA":        {"origins": ["CN", "IN", "KR", "BD", "LK"],
                    "destinations": ["CN", "IN", "KR", "BD", "LK"]},
    "AU_CN_FTA":   {"origins": ["AU", "CN"], "destinations": ["AU", "CN"]},
    "AU_TH_FTA":   {"origins": ["AU", "TH"], "destinations": ["AU", "TH"]},
    "CA_JO_FTA":   {"origins": ["CA", "JO"], "destinations": ["CA", "JO"]},
    "CN_KR_FTA":   {"origins": ["CN", "KR"], "destinations": ["CN", "KR"]},
    "CN_NZ_FTA":   {"origins": ["CN", "NZ"], "destinations": ["CN", "NZ"]},
    "CN_PK_FTA":   {"origins": ["CN", "PK"], "destinations": ["CN", "PK"]},
    "IJEPA":       {"origins": ["ID", "JP"], "destinations": ["ID", "JP"]},
    "IN_JP_CEPA":  {"origins": ["IN", "JP"], "destinations": ["IN", "JP"]},
    "JP_GSP":      {"origins": ["KH", "BD", "VN", "ID", "IN", "MM", "PK", "LK"],
                    "destinations": ["JP"]},
    "JP_MX":       {"origins": ["JP", "MX"], "destinations": ["JP", "MX"]},
    "JP_TH_FTA":   {"origins": ["JP", "TH"], "destinations": ["JP", "TH"]},
    "JPVNEPA":     {"origins": ["JP", "VN"], "destinations": ["JP", "VN"]},
    "KR_IN_CEPA":  {"origins": ["KR", "IN"], "destinations": ["KR", "IN"]},
    "NZ_TH_FTA":   {"origins": ["NZ", "TH"], "destinations": ["NZ", "TH"]},
}


@dataclass
class ChapterRule:
    """Rules of Origin for one HS chapter within one FTA."""
    chapter: str
    chapter_label: str
    summary: str
    glossary: dict[str, str]
    spec_count: int


@dataclass
class FTAAgreement:
    """Parsed representation of a single FTA/STP agreement."""
    code: str
    name: str
    generated_at: str
    overview_summary: str
    overview_glossary: dict[str, str]
    total_spec_count: int
    chapter_count: int
    chapters: dict[str, ChapterRule]
    origins: list[str] = field(default_factory=list)
    destinations: list[str] = field(default_factory=list)

    def has_chapter(self, ch: str) -> bool:
        return ch in self.chapters

    def covers_lane(self, origin: str, destination: str) -> bool:
        return origin in self.origins and destination in self.destinations

    def get_rule_for_product(self, product_category: str) -> list[ChapterRule]:
        chapters = PRODUCT_CATEGORY_CHAPTERS.get(product_category, [])
        return [self.chapters[ch] for ch in chapters if ch in self.chapters]


# Program code → human-readable name (extends real_data_loader.PROGRAM_NAMES)
FTA_DISPLAY_NAMES: dict[str, str] = {
    "AANZFTA":    "ASEAN-Australia-NZ FTA",
    "ACFTA":      "ASEAN-China FTA",
    "AIFTA":      "ASEAN-India FTA",
    "AJCEP":      "ASEAN-Japan Comprehensive EPA",
    "AKFTA":      "ASEAN-Korea FTA",
    "APTA":       "Asia-Pacific Trade Agreement",
    "ASEAN":      "ASEAN Free Trade Area (AFTA)",
    "AU_CN_FTA":  "Australia-China FTA",
    "AU_TH_FTA":  "Australia-Thailand FTA",
    "CA_JO_FTA":  "Canada-Jordan FTA",
    "CAFTA_DR":   "CAFTA-DR (Central America)",
    "CN_KR_FTA":  "China-Korea FTA",
    "CN_NZ_FTA":  "China-New Zealand FTA",
    "CN_PK_FTA":  "China-Pakistan FTA",
    "CPTPP":      "CPTPP (Trans-Pacific)",
    "EU_CAS":     "EU-Central Asia GSP+",
    "EU_EG":      "EU-Egypt Association",
    "EU_GE":      "EU-Georgia Association",
    "EU_GSP":     "EU Generalized System of Preferences",
    "EU_JO":      "EU-Jordan Association",
    "EU_JP":      "EU-Japan EPA",
    "EU_MX":      "EU-Mexico FTA",
    "EU_TR":      "EU-Turkey Customs Union",
    "EU_VN":      "EU-Vietnam FTA",
    "GB_GSP":     "UK Generalized System of Preferences",
    "GB_VN":      "UK-Vietnam FTA",
    "IJEPA":      "Indonesia-Japan EPA",
    "IN_JP_CEPA": "India-Japan CEPA",
    "JO_US_FTA":  "Jordan-US FTA",
    "JP_GSP":     "Japan GSP",
    "JP_MX":      "Japan-Mexico EPA",
    "JP_TH_FTA":  "Japan-Thailand EPA",
    "JPVNEPA":    "Japan-Vietnam EPA",
    "KR_IN_CEPA": "Korea-India CEPA",
    "NZ_TH_FTA":  "New Zealand-Thailand FTA",
    "US_GSP":     "US Generalized System of Preferences",
    "USMCA":      "USMCA (US-Mexico-Canada)",
}


def _extract_rule_type_tags(summary: str) -> dict:
    """Extract key qualification characteristics from a chapter summary."""
    s = summary.lower()
    tags = {
        "has_tariff_shift": False,
        "tariff_shift_type": None,
        "has_rvc": False,
        "rvc_threshold": None,
        "rvc_method": None,
        "has_process_rule": False,
        "process_type": None,
        "has_yarn_forward": False,
        "has_de_minimis": False,
        "de_minimis_type": None,
        "rule_logic": "AND",
    }

    if "yarn-forward" in s or "yarn\u2011forward" in s or "yarn forward" in s:
        tags["has_yarn_forward"] = True
        tags["has_tariff_shift"] = True
        tags["tariff_shift_type"] = "yarn-forward"
    elif "change in chapter" in s or "change to" in s and "from any other chapter" in s:
        tags["has_tariff_shift"] = True
        tags["tariff_shift_type"] = "CC"
    elif "change in tariff heading" in s or "cth" in s:
        tags["has_tariff_shift"] = True
        tags["tariff_shift_type"] = "CTH"
    elif "change in tariff subheading" in s or "ctsh" in s:
        tags["has_tariff_shift"] = True
        tags["tariff_shift_type"] = "CTSH"
    elif "tariff shift" in s or "tariff-shift" in s:
        tags["has_tariff_shift"] = True
        tags["tariff_shift_type"] = "TS"

    if "no rvc" in s or "no regional value content" in s:
        tags["has_rvc"] = False
    elif "rvc" in s or "regional value content" in s or "maxnom" in s:
        tags["has_rvc"] = True
        for pct in ["35%", "40%", "45%", "50%", "55%", "60%", "65%", "70%"]:
            if pct in s:
                tags["rvc_threshold"] = pct
                break
        if "build-up" in s or "build\u2011up" in s:
            tags["rvc_method"] = "Build-Up"
        elif "build-down" in s or "build\u2011down" in s:
            tags["rvc_method"] = "Build-Down"
        elif "transaction value" in s:
            tags["rvc_method"] = "Transaction Value"
        elif "net cost" in s:
            tags["rvc_method"] = "Net Cost"
        elif "inc" in s and ("exw" in s or "ex-works" in s or "ex\u2011works" in s):
            tags["rvc_method"] = "MaxNOM (EXW)"
        elif "fob" in s:
            tags["rvc_method"] = "FOB"

    if "cut" in s and ("sew" in s or "assembl" in s):
        tags["has_process_rule"] = True
        tags["process_type"] = "cut-and-sew"
    elif "weaving" in s and "making-up" in s or "making\u2011up" in s:
        tags["has_process_rule"] = True
        tags["process_type"] = "weaving + making-up"
    elif "knitting" in s and "making-up" in s or "making\u2011up" in s:
        tags["has_process_rule"] = True
        tags["process_type"] = "knitting + making-up"
    elif "substantial transformation" in s:
        tags["has_process_rule"] = True
        tags["process_type"] = "substantial transformation"
    elif "process" in s and ("required" in s or "must" in s):
        tags["has_process_rule"] = True
        tags["process_type"] = "specified process"

    if "de minimis" in s or "de\u2011minimis" in s or "tolerance" in s:
        tags["has_de_minimis"] = True
        if "by weight" in s or "weight" in s and "10%" in s:
            tags["de_minimis_type"] = "10% by weight"
        elif "by value" in s or "10% of" in s:
            tags["de_minimis_type"] = "10% by value"
        elif "8%" in s:
            tags["de_minimis_type"] = "8% of EXW"
        elif "15%" in s:
            tags["de_minimis_type"] = "15% of EXW"
        elif "no de minimis" in s or "none" in s:
            tags["has_de_minimis"] = False

    if " or " in s and ("option a" in s or "option b" in s or "path a" in s
                         or "choose one" in s or "alternative" in s):
        tags["rule_logic"] = "OR"

    return tags


def load_fta(filepath: Path) -> FTAAgreement:
    """Parse a single FTA JSON file into an FTAAgreement."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    code = filepath.stem
    overview = data.get("overview", {})
    chapters_raw = data.get("chapters", {})

    chapters = {}
    for ch_key, ch_data in chapters_raw.items():
        chapters[ch_key] = ChapterRule(
            chapter=ch_key,
            chapter_label=HS_CHAPTER_LABELS.get(ch_key, f"Chapter {ch_key}"),
            summary=ch_data.get("summary", ""),
            glossary=ch_data.get("glossary", {}),
            spec_count=ch_data.get("spec_count", 0),
        )

    lane_info = FTA_LANE_COVERAGE.get(code, {})

    return FTAAgreement(
        code=code,
        name=FTA_DISPLAY_NAMES.get(code, code),
        generated_at=data.get("generated_at", ""),
        overview_summary=overview.get("summary", ""),
        overview_glossary=overview.get("glossary", {}),
        total_spec_count=overview.get("spec_count", 0),
        chapter_count=overview.get("chapter_count", 0),
        chapters=chapters,
        origins=lane_info.get("origins", []),
        destinations=lane_info.get("destinations", []),
    )


def load_all_ftas(data_dir: Optional[Path] = None) -> dict[str, FTAAgreement]:
    """Load all FTA JSON files from the data directory."""
    d = data_dir or FTA_DATA_DIR
    if not d.exists():
        return {}

    agreements = {}
    for fp in sorted(d.glob("*.json")):
        try:
            fta = load_fta(fp)
            agreements[fta.code] = fta
        except Exception:
            continue
    return agreements


def find_agreements_for_lane(
    origin: str,
    destination: str,
    agreements: dict[str, FTAAgreement],
) -> list[FTAAgreement]:
    """Return all FTAs that cover a given origin→destination lane."""
    return [a for a in agreements.values() if a.covers_lane(origin, destination)]


def find_agreements_for_product(
    product_category: str,
    agreements: dict[str, FTAAgreement],
) -> dict[str, list[ChapterRule]]:
    """Return chapter rules for a product category across all FTAs."""
    result = {}
    chapters = PRODUCT_CATEGORY_CHAPTERS.get(product_category, [])
    for code, fta in agreements.items():
        rules = [fta.chapters[ch] for ch in chapters if ch in fta.chapters]
        if rules:
            result[code] = rules
    return result


def build_comparison_table(
    origin: str,
    destination: str,
    product_category: str,
    agreements: dict[str, FTAAgreement],
) -> list[dict]:
    """Build a comparison of qualification rules across FTAs for a lane + product."""
    lane_ftas = find_agreements_for_lane(origin, destination, agreements)
    chapters = PRODUCT_CATEGORY_CHAPTERS.get(product_category, [])

    rows = []
    for fta in lane_ftas:
        for ch in chapters:
            if ch not in fta.chapters:
                continue
            rule = fta.chapters[ch]
            tags = _extract_rule_type_tags(rule.summary)
            rows.append({
                "FTA": fta.name,
                "FTA Code": fta.code,
                "HS Chapter": ch,
                "Chapter": rule.chapter_label,
                "Tariff Shift": tags["tariff_shift_type"] or "None",
                "Yarn Forward": "Yes" if tags["has_yarn_forward"] else "No",
                "RVC Required": tags["rvc_threshold"] or "No",
                "RVC Method": tags["rvc_method"] or "—",
                "Process Rule": tags["process_type"] or "None",
                "De Minimis": tags["de_minimis_type"] or "None",
                "Rule Logic": tags["rule_logic"],
                "Spec Count": rule.spec_count,
            })

    return rows


def get_qualification_difficulty(tags: dict) -> str:
    """Rate qualification difficulty based on rule tags."""
    if tags["has_yarn_forward"]:
        return "Hard"
    if tags["has_tariff_shift"] and tags["has_rvc"] and tags["has_process_rule"]:
        return "Hard"
    if tags["has_rvc"] and not tags["has_tariff_shift"]:
        threshold = tags.get("rvc_threshold", "")
        if threshold and int(threshold.rstrip("%")) <= 35:
            return "Easy"
        return "Moderate"
    if tags["has_tariff_shift"] and not tags["has_rvc"]:
        if tags["tariff_shift_type"] in ("CC", "CTH"):
            return "Moderate"
        return "Easy"
    if tags["rule_logic"] == "OR":
        return "Moderate"
    return "Moderate"


def build_difficulty_matrix(
    agreements: dict[str, FTAAgreement],
) -> list[dict]:
    """Build a matrix of qualification difficulty by FTA × product category."""
    rows = []
    for code, fta in agreements.items():
        row = {"FTA": fta.name, "FTA Code": code}
        for cat, chapters in PRODUCT_CATEGORY_CHAPTERS.items():
            if cat == "Equipment":
                continue
            difficulties = []
            for ch in chapters:
                if ch in fta.chapters:
                    tags = _extract_rule_type_tags(fta.chapters[ch].summary)
                    difficulties.append(get_qualification_difficulty(tags))
            if difficulties:
                worst = "Hard" if "Hard" in difficulties else (
                    "Moderate" if "Moderate" in difficulties else "Easy"
                )
                row[cat] = worst
            else:
                row[cat] = "—"
        rows.append(row)
    return rows


def _summarize_rule_for_lane(fta: FTAAgreement) -> dict:
    """Produce a compact rule summary across all chapters in one FTA."""
    all_tags = []
    for ch in fta.chapters.values():
        all_tags.append(_extract_rule_type_tags(ch.summary))

    if not all_tags:
        return {"difficulty": "—", "rule_summary": "No chapter data", "dominant_rule": "—"}

    difficulties = [get_qualification_difficulty(t) for t in all_tags]
    worst = "Hard" if "Hard" in difficulties else (
        "Moderate" if "Moderate" in difficulties else "Easy"
    )

    has_yf = any(t["has_yarn_forward"] for t in all_tags)
    ts_types = [t["tariff_shift_type"] for t in all_tags if t["has_tariff_shift"] and t["tariff_shift_type"]]
    rvc_thresholds = [t["rvc_threshold"] for t in all_tags if t["has_rvc"] and t["rvc_threshold"]]
    prc_types = [t["process_type"] for t in all_tags if t["has_process_rule"] and t["process_type"]]

    parts = []
    if has_yf:
        parts.append("Yarn-forward")
    elif ts_types:
        parts.append(ts_types[0])
    if rvc_thresholds:
        parts.append(f"RVC {rvc_thresholds[0]}")
    if prc_types:
        parts.append(prc_types[0])

    dominant = " + ".join(parts) if parts else "See rules"

    return {"difficulty": worst, "rule_summary": dominant, "dominant_rule": dominant}


def enrich_lanes_with_fta(
    lanes_df,
    agreements: dict[str, FTAAgreement],
):
    """Add FTA rule columns to a lanes DataFrame in-place.

    Prioritizes the lane's actual STP programs (from eligibility data) over
    theoretical coverage from the FTA JSON files. If the lane has STP programs,
    we look up rules for THOSE programs. If not, we check FTA coverage to show
    what's theoretically available.

    Adds: fta_name, fta_code, fta_difficulty, fta_rule_summary, fta_count, roo_discount
    """
    fta_names = []
    fta_codes = []
    fta_difficulties = []
    fta_rule_summaries = []
    fta_counts = []

    # Map common STP codes to FTA JSON codes
    _stp_to_fta = {
        "CAFTA_DR": "CAFTA_DR", "CAFTA-DR": "CAFTA_DR",
        "USMCA": "USMCA", "US_GSP": "US_GSP",
        "JO_US_FTA": "JO_US_FTA", "EG_QIZ": None,
        "CPTPP": "CPTPP", "EU_GSP": "EU_GSP",
        "EU_VN": "EU_VN", "EU_JP": "EU_JP",
        "GB_GSP": "GB_GSP", "GB_VN": "GB_VN",
        "ACFTA": "ACFTA", "ASEAN": "ASEAN",
        "AIFTA": "AIFTA", "AKFTA": "AKFTA",
        "AJCEP": "AJCEP", "AANZFTA": "AANZFTA",
        "APTA": "APTA", "IJEPA": "IJEPA",
        "JP_GSP": "JP_GSP", "JPVNEPA": "JPVNEPA",
        "RCEP": None, "CA_LDCT": None,
    }

    for _, row in lanes_df.iterrows():
        origin = row.get("country_of_origin_cd", "")
        dest = row.get("country_of_destination_cd", "")
        has_stp = row.get("has_stp", False)
        lane_programs = row.get("stp_programs", [])
        if not isinstance(lane_programs, list):
            lane_programs = []

        # Strategy 1: Use the lane's actual STP programs to find the best FTA
        if has_stp and lane_programs:
            best = None
            best_diff = None
            best_info = None
            for prog in lane_programs:
                fta_code = _stp_to_fta.get(prog, prog)
                if fta_code and fta_code in agreements:
                    fta_obj = agreements[fta_code]
                    info = _summarize_rule_for_lane(fta_obj)
                    d = info["difficulty"]
                    if best is None or _diff_rank(d) < _diff_rank(best_diff):
                        best = fta_obj
                        best_diff = d
                        best_info = info

            if best:
                fta_names.append(best.name)
                fta_codes.append(best.code)
                fta_difficulties.append(best_info["difficulty"])
                fta_rule_summaries.append(best_info["rule_summary"])
                fta_counts.append(len(lane_programs))
                continue

            # STP program exists but no matching FTA JSON (e.g., EG_QIZ)
            fta_names.append(", ".join(lane_programs[:2]))
            fta_codes.append(lane_programs[0])
            fta_difficulties.append("—")
            fta_rule_summaries.append("Rules not in FTA database")
            fta_counts.append(len(lane_programs))
            continue

        # Strategy 2: No STP eligibility data — check FTA coverage map
        matching = find_agreements_for_lane(origin, dest, agreements)
        if matching:
            best = None
            best_diff = None
            best_info = None
            for fta in matching:
                info = _summarize_rule_for_lane(fta)
                d = info["difficulty"]
                if best is None or _diff_rank(d) < _diff_rank(best_diff):
                    best = fta
                    best_diff = d
                    best_info = info

            fta_names.append(best.name)
            fta_codes.append(best.code)
            fta_difficulties.append(best_info["difficulty"])
            fta_rule_summaries.append(best_info["rule_summary"])
            fta_counts.append(len(matching))
        else:
            fta_names.append("None")
            fta_codes.append("")
            fta_difficulties.append("—")
            fta_rule_summaries.append("No FTA available")
            fta_counts.append(0)

    lanes_df["fta_name"] = fta_names
    lanes_df["fta_code"] = fta_codes
    lanes_df["fta_difficulty"] = fta_difficulties
    lanes_df["fta_rule_summary"] = fta_rule_summaries
    lanes_df["fta_count"] = fta_counts
    lanes_df["roo_discount"] = lanes_df["fta_difficulty"].map(ROO_DIFFICULTY_DISCOUNT).fillna(0.50)

    return lanes_df


def get_fta_info_for_origin(
    origin: str,
    destination: str,
    agreements: dict[str, FTAAgreement],
) -> dict:
    """Get compact FTA info for a single origin→destination pair."""
    matching = find_agreements_for_lane(origin, destination, agreements)
    if not matching:
        return {
            "fta_name": "None",
            "fta_code": "",
            "difficulty": "—",
            "rule_summary": "No FTA available",
            "count": 0,
            "all_ftas": [],
        }

    best = None
    best_diff = None
    best_info = None
    for fta in matching:
        info = _summarize_rule_for_lane(fta)
        d = info["difficulty"]
        if best is None or _diff_rank(d) < _diff_rank(best_diff):
            best = fta
            best_diff = d
            best_info = info

    return {
        "fta_name": best.name,
        "fta_code": best.code,
        "difficulty": best_info["difficulty"],
        "rule_summary": best_info["rule_summary"],
        "count": len(matching),
        "all_ftas": [f.name for f in matching],
    }


def _diff_rank(difficulty: str) -> int:
    return {"Easy": 0, "Moderate": 1, "Hard": 2}.get(difficulty, 3)


# ── ROO Difficulty Discount ──────────────────────────────────────────
# Reflects the realistic probability of qualifying for preferential
# treatment given the complexity of the Rules of Origin.
#
# Easy   (RVC <=35%, simple process rule): most products qualify → 90%
# Moderate (tariff shift, RVC >35%, OR logic): some complexity → 70%
# Hard   (yarn-forward, multiple requirements): significant barriers → 40%
# Unknown (no FTA data): conservative default → 50%

ROO_DIFFICULTY_DISCOUNT = {
    "Easy": 0.90,
    "Moderate": 0.70,
    "Hard": 0.40,
    "—": 0.50,
}


def get_roo_discount(difficulty: str) -> float:
    """Return the ROO qualification probability for a given difficulty level."""
    return ROO_DIFFICULTY_DISCOUNT.get(difficulty, 0.50)
