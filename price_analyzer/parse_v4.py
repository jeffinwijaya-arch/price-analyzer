"""
parse_v4.py — Rolex & Luxury Watch Listing Parser
Multi-stage dial extraction with premium dial detection.

Pipeline order per listing:
  1. FIXED_DIAL   — single-dial references (100% confidence)
  2. SUFFIX_DIAL  — reference suffix encodes dial (95%)
  3. Premium scan — Tiffany, Paul Newman, Meteorite, etc. (60-100%)
  4. Context scan — text near "dial:" / "colour:" keywords (85%)
  5. Color scan   — broad color tokens, validated vs. rolex_dial_options (60-80%)
  6. Synonym scan — dealer shorthand dict (65%)
"""

import re
import json
from pathlib import Path
from typing import Optional

_DIR = Path(__file__).parent


def _load_json(filename):
    path = _DIR / filename
    if path.exists() and path.stat().st_size > 0:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# Reference data — loaded once at import time
# ---------------------------------------------------------------------------
_DIAL_OPTIONS = _load_json("rolex_dial_options.json")
_DIAL_SYNONYMS = _load_json("dial_synonyms.json")
_DIAL_CATALOG = _load_json("dial_reference_catalog.json")
_WATCH_MASTER = _load_json("watch_reference_master.json")

# ---------------------------------------------------------------------------
# FIXED_DIAL — references that ship with exactly one dial
#              text analysis is skipped; premium override still applies
# ---------------------------------------------------------------------------
FIXED_DIAL = {
    # Submariner No-Date
    "124060":     "Black",
    # Submariner Date Steel
    "126610LN":   "Black",
    "126610LV":   "Black",   # green bezel, black dial (Kermit)
    # Submariner Date Two-Tone
    "126613LN":   "Black",
    "126613LB":   "Blue",
    # Submariner Date Yellow Gold
    "126618LN":   "Black",
    "126618LB":   "Blue",
    # Submariner Date White Gold
    "126619LB":   "Blue",
    # Legacy Submariner
    "116610LN":   "Black",
    "116610LV":   "Black",
    "116613LN":   "Black",
    "116613LB":   "Blue",
    "116618LN":   "Black",
    "116618LB":   "Blue",
    # Sea-Dweller / Deepsea (standard black; D-Blue is a premium override)
    "126600":     "Black",
    "116660":     "Black",
    # GMT-Master II — dial is always Black unless noted otherwise
    "126710BLNR": "Black",   # Batman
    "126710BLRO": "Black",   # Pepsi steel
    "126718GRNR": "Black",   # Yellow gold Green/Black
    "126720VTNM": "Black",   # Jubilee Green
    "116710BLNR": "Black",
    "116710LN":   "Black",
    "116710LV":   "Black",
    # GMT-Master II White Gold Pepsi — ONLY dial is Meteorite
    "126719BLRO": "Meteorite",
    # Day-Date 40 Platinum — ONLY dial is Ice Blue
    "228206":     "Ice Blue",
    # Day-Date 36 Platinum
    "128236":     "Ice Blue",
    # Day-Date 36 Platinum (Oyster bracelet)
    "127236":     "Ice Blue",
    # Milgauss
    "116400GV":   "Black",
    "116400":     "Black",
    # Air-King
    "126900":     "Black",
    "116900":     "Black",
    # Explorer I
    "124270":     "Black",
    "214270":     "Black",
    "114270":     "Black",
    # Yacht-Master Oystersteel/Platinum (Slate only)
    "126622":     "Slate",
    "116622":     "Slate",
    # Yacht-Master Oystersteel/Everose
    "126621":     "Black",
    "116621":     "Black",
}

# ---------------------------------------------------------------------------
# SUFFIX_DIAL — reference suffix -> dial inference (most specific first)
# ---------------------------------------------------------------------------
SUFFIX_DIAL = {
    "BLNR": "Black",    # Batman
    "BLRO": "Black",    # Pepsi
    "CHNR": "Black",    # Rootbeer (black dominant dial)
    "BKSO": "Black",
    "VTNM": "Black",    # Green/Black GMT
    "LN":   "Black",    # Black bezel + Black dial
    "LB":   "Blue",     # Black bezel + Blue dial
    "LV":   "Black",    # Green bezel + Black dial (Kermit)
}

# ---------------------------------------------------------------------------
# _PREMIUM_REF_MAP — guards against false positives.
#   Maps premium dial name -> list of refs that legitimately offer it.
# ---------------------------------------------------------------------------
_PREMIUM_REF_MAP = {
    "Tiffany Blue": [
        "124300", "126000", "277200", "279160", "279177",
    ],
    "Paul Newman": [
        "6239", "6241", "6263", "6265",
        "116508", "126508",
    ],
    "Meteorite": [
        "116508", "116509", "116519", "116519LN",
        "126509", "126519", "126519LN",
        "228235", "228238", "228239", "228206",
        "128238", "128235", "128239",
        "126334", "126333", "126331",
        "126719BLRO",
    ],
    "Wimbledon": [
        "126334", "126331", "126333", "126238",
        "116334", "116331",
        "126201", "126301",
    ],
    "Ice Blue": [
        "228206", "128236", "127236", "228396TBR", "128396TBR",
    ],
    "Turquoise Stone": [
        "228345", "228235", "228238", "228239", "228349",
        "128345", "128235", "128238", "128239",
    ],
    "Tiger Eye": [
        "18038", "18238", "118238",
        "128238", "228238",
    ],
    "Lapis Lazuli": [
        "18038", "18238",
        "128238", "228238", "228235",
    ],
    "Aventurine": [
        "128345", "228235", "228238", "228349",
    ],
    "Grossular": [
        "126555", "118338", "118348",
    ],
    "Onyx": [
        "228235", "228238", "228239",
    ],
    "Ombre":        ["228235"],
    "Ombre Slate":  ["228235"],
    "Eisenkiesel":  ["228235", "228238", "228239"],
    "D-Blue":       ["126660"],
    "Candy Pink":   ["124300", "126000", "277200"],
    "Apple Green":  ["124300", "126000"],
    "Coral Red":    ["124300", "126000", "277200", "279160"],
}

# ---------------------------------------------------------------------------
# Premium dial patterns — (compiled_regex, canonical_name, priority 0-100)
#   Higher priority wins when multiple patterns match the same text.
#   Turquoise Stone must precede plain turquoise to avoid false mapping.
# ---------------------------------------------------------------------------
_PREMIUM_PATTERNS = [
    # Tiffany Blue / Turquoise Blue (OP models)
    (re.compile(r"\btiff(?:any)?(?:\s+blue)?\b", re.I),      "Tiffany Blue",    100),
    (re.compile(r"\bturquoise\s+blue\b",          re.I),      "Tiffany Blue",    100),
    (re.compile(r"\brobin'?s?\s+egg\s*blue?\b",   re.I),      "Tiffany Blue",    100),
    (re.compile(r"\bturq\b",                       re.I),      "Tiffany Blue",     90),
    # tb = Tiffany Blue shorthand — only credible for OP references
    # Handled in detect_premium_dial() with ref-aware penalty; included here
    # so the pattern fires at all — mismatch will drop confidence below threshold
    (re.compile(r"\btb\b",                         re.I),      "Tiffany Blue",     85),
    # Plain "turquoise" — Tiffany Blue for OP refs (allowed passes), Turquoise Stone
    # for DD/other refs via stage-5 color scan (_COLOR_PATTERNS) after this is rejected
    (re.compile(r"\bturquoise\b",                  re.I),      "Tiffany Blue",     75),
    # Paul Newman (Daytona exotic)
    (re.compile(r"\bpaul\s+newman\b",              re.I),      "Paul Newman",     100),
    (re.compile(r"\bpaul\s*n\b",                   re.I),      "Paul Newman",      90),
    (re.compile(r"\bpn\b",                         re.I),      "Paul Newman",      75),
    (re.compile(r"\bexotic\s*(?:dial|face)?\b",    re.I),      "Paul Newman",      70),
    # Meteorite
    (re.compile(r"\bmeteor(?:ite)?\b",             re.I),      "Meteorite",       100),
    (re.compile(r"\bmeteo\b",                      re.I),      "Meteorite",        90),
    (re.compile(r"\bmete\b",                       re.I),      "Meteorite",        85),
    (re.compile(r"\bmet\b",                        re.I),      "Meteorite",        55),
    # Wimbledon
    (re.compile(r"\bwimbledon\b",                  re.I),      "Wimbledon",       100),
    (re.compile(r"\bwimbo\b",                      re.I),      "Wimbledon",        90),
    (re.compile(r"\bwimb?\b",                      re.I),      "Wimbledon",        85),
    # Ice Blue (platinum models only)
    (re.compile(r"\bice\s*blue\b",                 re.I),      "Ice Blue",        100),
    (re.compile(r"\biceblue\b",                    re.I),      "Ice Blue",        100),
    (re.compile(r"\bib\b",                         re.I),      "Ice Blue",         65),
    # D-Blue (Deepsea 126660)
    (re.compile(r"\bd[\s\-]?blue\b",               re.I),      "D-Blue",          100),
    (re.compile(r"\bjames\s+cameron\b",            re.I),      "D-Blue",          100),
    # Ombre (Day-Date 40 Everose)
    (re.compile(r"\bombr[eE\xe9]\s+slate\b",       re.I),      "Ombre Slate",     100),
    (re.compile(r"\bombr[eE\xe9]\b",               re.I),      "Ombre",           100),
    # Stone dials — Turquoise Stone before plain turquoise
    (re.compile(r"\bturquoise\s*(?:stone|dial)\b", re.I),      "Turquoise Stone", 100),
    (re.compile(r"\baventurine\b",                 re.I),      "Aventurine",      100),
    (re.compile(r"\bgrossular\b",                  re.I),      "Grossular",       100),
    (re.compile(r"\bgiraffe\b",                    re.I),      "Grossular",       100),
    (re.compile(r"\bonyx\b",                       re.I),      "Onyx",            100),
    (re.compile(r"\blapis\s+lazuli\b",             re.I),      "Lapis Lazuli",    100),
    (re.compile(r"\blapis\b",                      re.I),      "Lapis Lazuli",     90),
    (re.compile(r"\btiger'?s?\s*eye\b",            re.I),      "Tiger Eye",       100),
    # Candy Pink / Apple Green / Coral Red (OP special colours)
    (re.compile(r"\bcandy\s*pink\b",               re.I),      "Candy Pink",      100),
    (re.compile(r"\bapple\s*green\b",              re.I),      "Apple Green",     100),
    (re.compile(r"\bcoral\s+red\b",                re.I),      "Coral Red",       100),
    (re.compile(r"\bcoral\b|\bcherry\s+red\b",     re.I),      "Coral Red",        75),
    # Pave / MOP
    (re.compile(r"\bpav[eE\xe9]\b",               re.I),      "Pave",            100),
    (re.compile(r"\bmother\s+of\s+pearl\b",        re.I),      "MOP",             100),
    (re.compile(r"\bmop\b",                        re.I),      "MOP",              90),
    # Special dials
    (re.compile(r"\bpuzzle\b",                     re.I),      "Puzzle",          100),
    (re.compile(r"\bcelebration\b",                re.I),      "Celebration",     100),
    (re.compile(r"\beisenk(?:iesel)?\b",           re.I),      "Eisenkiesel",     100),
    (re.compile(r"\beisen\b",                      re.I),      "Eisenkiesel",      90),
]

# ---------------------------------------------------------------------------
# Standard colour patterns — lower priority, broad matching
# ---------------------------------------------------------------------------
_COLOR_PATTERNS = [
    (re.compile(r"\bblack\b|\bblk\b|\bbk\b|\bblck\b",             re.I), "Black"),
    (re.compile(r"\bwhite\b|\bwht\b|\bwh\b",                       re.I), "White"),
    (re.compile(r"\bbright\s+blue\b|\bbb\b",                       re.I), "Bright Blue"),
    (re.compile(r"\bblue\b|\bblu\b",                               re.I), "Blue"),
    (re.compile(r"\bturquoise\b",                                   re.I), "Turquoise Stone"),
    (re.compile(r"\bpistachio\s*(?:green)?\b",                     re.I), "Mint Green"),
    (re.compile(r"\bmint\s*(?:green)?\b|\bminty\b",               re.I), "Mint Green"),
    (re.compile(r"\bolive\s*(?:green)?\b|\bog\b",                 re.I), "Olive Green"),
    (re.compile(r"\bpalm\s*(?:green)?\b",                          re.I), "Palm Green"),
    (re.compile(r"\bapple\s*green\b",                              re.I), "Apple Green"),
    (re.compile(r"\bgreen\b|\bgrn\b|\bgg\b|\bstarbucks\b|\bhulk\b|\bkermit\b", re.I), "Green"),
    (re.compile(r"\bsilver\b|\bslvr\b|\bslv\b|\bbenz\b",          re.I), "Silver"),
    (re.compile(r"\bchampagne\b|\bchamp\b|\bcham\b|\bchp\b",      re.I), "Champagne"),
    (re.compile(r"\bchocolate\b|\bchoco\b|\bcho\b",                re.I), "Chocolate"),
    (re.compile(r"\bgr[ae]y\b|\bghost\b",                          re.I), "Grey"),
    (re.compile(r"\bsalmon\b",                                     re.I), "Salmon"),
    (re.compile(r"\baubergine\b|\baub\b|\bpurp(?:le)?\b",         re.I), "Aubergine"),
    (re.compile(r"\bsundust\b|\bsd\b",                             re.I), "Sundust"),
    (re.compile(r"\bpink\b",                                       re.I), "Pink"),
    (re.compile(r"\bslate\b",                                      re.I), "Slate"),
    (re.compile(r"\brhodum\b|\brhodium\b",                         re.I), "Rhodium"),
    (re.compile(r"\byellow\b",                                     re.I), "Yellow"),
    (re.compile(r"\bivory\b",                                      re.I), "Ivory"),
    (re.compile(r"\bbrown\b",                                      re.I), "Brown"),
    (re.compile(r"\breverse\s*panda\b|\brev\s*panda\b|\brp\b",    re.I), "Reverse Panda"),
    (re.compile(r"\bpanda\b",                                      re.I), "Panda"),
    (re.compile(r"\bpepsi\b",                                      re.I), "Blue Red"),
    (re.compile(r"\bbatman\b|\bbatgirl\b",                         re.I), "Blue Black"),
    (re.compile(r"\bsprite\b",                                     re.I), "Green Black"),
    (re.compile(r"\bcoke\b",                                       re.I), "Black Red"),
]

# Looks for a colour token in the neighbourhood of the word "dial" / "colour"
_DIAL_CONTEXT_RE = re.compile(
    r"(?:dial|face|colour|color|cadran)\s*[:\-]?\s*"
    r"([a-z][a-z0-9 '\-]{1,40}?)(?:\s*[,;/|]|$)",
    re.I,
)
# "with a <colour> dial" / "with <colour> dial"
_WITH_DIAL_RE = re.compile(
    r"with\s+(?:an?\s+)?([a-z][a-z0-9 '\-]{1,30}?)\s+(?:colou?r(?:ed)?\s+)?dial\b",
    re.I,
)
# "featuring/features a <colour> dial"
_FEATURING_DIAL_RE = re.compile(
    r"featur(?:es?|ing)\s+(?:an?\s+)?([a-z][a-z0-9 '\-]{1,30}?)\s+dial\b",
    re.I,
)
# Used to guard 'sd'/'sun' synonyms in Sea-Dweller context
_SEA_DWELLER_CTX_RE = re.compile(r"sea[\s\-]?dwell|126600|116600", re.I)
# Guard words rejected in context scans (module-level for reuse)
_CTX_REJECT = frozenset({
    "complete", "box", "papers", "set", "with", "and", "or", "only",
    "full", "sticker", "warranty", "card", "tag", "inner", "outer",
    "new", "mint", "watch", "rolex", "ref", "model", "serial",
    "service", "unworn", "lightly", "worn", "used", "excellent",
})
# Canonical colour names from _COLOR_PATTERNS — used in stage-4 known-check
_ALL_COLOR_CANONICALS = frozenset(c for _, c in _COLOR_PATTERNS)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ref_clean(ref):
    return ref.upper().strip().replace(" ", "") if ref else None


def _premium_allowed(premium, ref):
    """True if this premium dial is catalogued for the given reference."""
    allowed = _PREMIUM_REF_MAP.get(premium)
    if allowed is None or ref is None:
        return True
    rc = _ref_clean(ref)
    return any(rc.startswith(r.upper()) for r in allowed)


def normalize_dial(raw, ref=None):
    """Map raw text to canonical dial name using dial_synonyms.json."""
    if not raw:
        return None
    key = raw.strip().lower()
    hit = _DIAL_SYNONYMS.get(key)
    if hit:
        return hit
    key2 = re.sub(r"\s+(?:dial|face)$", "", key).strip()
    if key2 != key:
        hit = _DIAL_SYNONYMS.get(key2)
        if hit:
            return hit
    return raw.strip().title()


def detect_premium_dial(text, ref=None):
    """
    Scan text for premium-dial signals.
    Returns the highest-confidence match dict or None.
    """
    best = None
    best_priority = -1

    for pattern, canonical, priority in _PREMIUM_PATTERNS:
        m = pattern.search(text)
        if not m:
            continue
        eff = priority if _premium_allowed(canonical, ref) else max(priority - 40, 10)
        if eff > best_priority:
            best_priority = eff
            best = {
                "dial":         canonical,
                "confidence":   min(eff / 100.0, 1.0),
                "matched_text": m.group(0),
                "is_premium":   True,
            }

    return best


# ---------------------------------------------------------------------------
# Core extractor
# ---------------------------------------------------------------------------

def extract_dial(text, ref=None):
    """
    Multi-stage dial extraction from free-form listing text.

    Returns dict:
        dial          - canonical dial name or None
        confidence    - 0.0 to 1.0
        is_premium    - True if a premium/collectible dial was detected
        premium_type  - name of premium dial or None
        method        - pipeline stage that produced the result
    """
    result = {
        "dial":         None,
        "confidence":   0.0,
        "is_premium":   False,
        "premium_type": None,
        "method":       "none",
    }

    if not text:
        return result

    rc = _ref_clean(ref)

    # Stage 1 — FIXED_DIAL
    if rc and rc in FIXED_DIAL:
        result.update({
            "dial":       FIXED_DIAL[rc],
            "confidence": 1.0,
            "method":     "fixed_dial",
        })
        premium = detect_premium_dial(text, ref)
        if premium and premium["confidence"] >= 0.80:
            result.update({
                "dial":         premium["dial"],
                "confidence":   premium["confidence"],
                "is_premium":   True,
                "premium_type": premium["dial"],
                "method":       "premium_override",
            })
        return result

    # Stage 2 — SUFFIX_DIAL
    if rc:
        for suffix, dial in SUFFIX_DIAL.items():
            if rc.endswith(suffix):
                result.update({
                    "dial":       dial,
                    "confidence": 0.95,
                    "method":     "suffix_dial",
                })
                break

    # Stage 3 — Premium dial scan
    premium = detect_premium_dial(text, ref)
    if premium and premium["confidence"] >= 0.60:
        result.update({
            "dial":         premium["dial"],
            "confidence":   premium["confidence"],
            "is_premium":   True,
            "premium_type": premium["dial"],
            "method":       "premium_pattern",
        })
        return result

    # If stage 2 found a dial already, return it (premium scan found nothing)
    if result["dial"]:
        return result

    # Stage 4 — Contextual extraction: "dial: X", "colour: X", "with X dial", "featuring X dial"
    for ctx_re in (_DIAL_CONTEXT_RE, _WITH_DIAL_RE, _FEATURING_DIAL_RE):
        for raw_ctx in ctx_re.findall(text):
            stripped = raw_ctx.strip().lower()
            if stripped in _CTX_REJECT or len(stripped) < 3:
                continue
            normalised = normalize_dial(stripped, ref)
            known = stripped in _DIAL_SYNONYMS or normalised in _ALL_COLOR_CANONICALS
            if normalised and known:
                result.update({
                    "dial":       normalised,
                    "confidence": 0.85,
                    "method":     "context_match",
                })
                return result

    # Stage 5 — Colour token scan
    for pattern, canonical in _COLOR_PATTERNS:
        if not pattern.search(text):
            continue
        if rc and rc in _DIAL_OPTIONS:
            valid = _DIAL_OPTIONS[rc]
            if canonical in valid:
                result.update({
                    "dial":       canonical,
                    "confidence": 0.82,
                    "method":     "color_pattern_validated",
                })
                return result
            for v in valid:
                if canonical.lower() in v.lower() or v.lower() in canonical.lower():
                    result.update({
                        "dial":       v,
                        "confidence": 0.72,
                        "method":     "color_pattern_partial",
                    })
                    return result
        else:
            result.update({
                "dial":       canonical,
                "confidence": 0.60,
                "method":     "color_pattern_unvalidated",
            })
            return result

    # Stage 6 — Synonym dictionary full-text scan
    text_lower = text.lower()
    _is_sea_dweller = bool(_SEA_DWELLER_CTX_RE.search(text))
    _has_sunray = bool(re.search(r"\bsun(?:ray|burst|shine)\b", text_lower))
    for raw_key, canonical in _DIAL_SYNONYMS.items():
        # Guard: 'sd'/'sun' must not resolve to Sundust in Sea-Dweller listings
        if raw_key == "sd" and _is_sea_dweller:
            continue
        # Guard: 'sun' must not resolve to Sundust when 'sunray'/'sunburst' is present
        if raw_key == "sun" and (_is_sea_dweller or _has_sunray):
            continue
        if re.search(r"\b" + re.escape(raw_key) + r"\b", text_lower):
            result.update({
                "dial":       canonical,
                "confidence": 0.65,
                "method":     "synonym_scan",
            })
            return result

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_listing(listing):
    """
    Enrich a listing dict with parsed dial data.
    Expects keys: title, description (optional), ref / reference / model (optional).
    """
    title = listing.get("title", "") or ""
    desc  = listing.get("description", "") or ""
    ref   = (
        listing.get("ref")
        or listing.get("reference")
        or listing.get("model")
        or listing.get("ref_number")
    )
    return {**listing, "parsed_dial": extract_dial(f"{title} {desc}", ref=ref)}


def analyze_listings(listings):
    """Batch-parse listings and return accuracy statistics."""
    total         = len(listings)
    empty_dial    = 0
    premium_count = 0
    by_method     = {}
    results       = []

    for listing in listings:
        parsed = parse_listing(listing)
        dr     = parsed["parsed_dial"]

        if not dr["dial"]:
            empty_dial += 1
        if dr["is_premium"]:
            premium_count += 1

        by_method[dr["method"]] = by_method.get(dr["method"], 0) + 1
        results.append(parsed)

    return {
        "total":            total,
        "empty_dial":       empty_dial,
        "empty_pct":        round(empty_dial / total * 100, 1) if total else 0,
        "premium_detected": premium_count,
        "by_method":        by_method,
        "results":          results,
    }
