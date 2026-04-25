"""
parse_v4.py — Rolex & Luxury Watch Listing Parser
Multi-stage dial extraction with premium dial detection.

Pipeline order per listing:
  1. FIXED_DIAL      — single-dial references (100% confidence)
  2. SUFFIX_DIAL     — reference suffix encodes dial (95%)
  3. Premium scan    — Tiffany, Paul Newman, Meteorite, etc. (60-100%)
  4. Context scan    — text near "dial:" / "colour:" keywords (85%)
  5. Color scan      — broad color tokens, validated vs. rolex_dial_options (60-80%)
  6. Synonym scan    — dealer shorthand dict (65%)
  7. Model inference — last-resort statistical default when no text yields a dial (55%)
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
    "116619LB":   "Blue",   # Legacy WG Submariner Date (predecessor to 126619LB)
    # Legacy Submariner
    "116610LN":   "Black",
    "116610LV":   "Green",   # Hulk: green dial + green bezel (NOT Kermit)
    "116613LN":   "Black",
    "116613LB":   "Blue",
    "116618LN":   "Black",
    "116618LB":   "Blue",
    # Sea-Dweller / Deepsea (standard black; D-Blue is a premium override)
    "126600":     "Black",
    "116660":     "Black",
    "126660":     "Black",   # Modern Deepsea (D-Blue triggers premium_override at Stage 1)
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
    # Yacht-Master Oystersteel/Everose — multiple dial options; removed from fixed
    # "126621" and "116621" handled by color scan + dial_options
    # Yacht-Master 42 Everose — exactly one dial (Sundust)
    "126595":     "Sundust",
    # Yacht-Master 40 Everose — exactly one dial (Chocolate)
    "126655":     "Chocolate",
    # Daytona RG Rainbow — all listings are Rainbow bezel/dial combination
    "116285BBR":  "Rainbow",
    # Day-Date TBR gem-set bracelet variants — Ice Blue only
    "228396TBR":  "Ice Blue",
    "128396TBR":  "Ice Blue",
    # Daytona WG Sapphire 2020s generation — one catalogued dial
    "126579":     "MOP",
    "126589":     "MOP",
    # Sky-Dweller 336259 gem-set diamond — Pave is the only offered dial
    "336259":     "Pave",
    # DD36 WG Diamond (118365) — Ice Blue Baguette is the canonical catalogued dial
    "118365":     "Ice Blue Baguette",
    # ---- new single-dial refs confirmed via wholesale data (2024-2025 catalog) ----
    # Explorer 40 (2021+ generation) and Explorer 36
    "224270":     "Black",
    "124273":     "Black",
    # Sea-Dweller newer generations
    "126067":     "Black",    # SD 43mm (successor to 126600 in some markets)
    "126603":     "Black",    # SD TT 43mm
    "136660":     "Black",    # Deepsea II (successor to 126660)
    # GMT-Master II newer variants — all confirmed single Black dial
    "126710GRNR": "Black",    # Black / Green-Black GRNR bezel
    "126720VTNR": "Black",    # Green ceramic VTNR
    "126715CHNR": "Black",    # Rootbeer new-generation
    "126729VTNR": "Black",    # YG VTNR
    # Submariner special
    "116659SABR": "Black",    # Sub WG sapphire bezel
    "114060":     "Black",    # Legacy Sub No-Date (pre-124060)
    # Yacht-Master single-dial confirmed refs
    "226659":     "Black",    # YM42 TT Black
    "226627":     "Black",    # YM42 Steel Oysterflex Black
    "268622":     "Slate",    # YM37 Oysterflex Slate
    # Rolex 1908 Platinum — Ice Blue is the only confirmed dial (wholesale: 251 listings)
    "52506":      "Ice Blue",
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
        # Datejust 31 (2024 release)
        "278289",
        # New OP41 (2024 release, also sold as 134300)
        "134300",
        # DD36 YG — baguette/stone variants
        "118208", "118238",
        # OP34 / OP28 secondary sizes (confirmed secondary market)
        "124200", "276200", "15210", "52506", "52508", "52509",
        # Lady-Datejust Tiffany Blue (confirmed catalog)
        "279135", "279136",
        # Daytona WG — Tiffany x Rolex collaboration / dealer-commissioned
        "116509", "116519", "116519LN", "126509", "126519", "126519LN",
        # Daytona YG Tiffany
        "126508", "116508",
        # Daytona RG/YG leather strap Tiffany
        "116505", "116515", "116518",
        # Daytona WG newer generation
        "126538", "126535", "126506",
        # Daytona WG Pave (ultra-rare)
        "116576",
        # Daytona WG Sapphire/gem-set (2024 catalog)
        "126599",
        # Sky-Dweller Tiffany (confirmed secondary market; wholesale data confirms 336238)
        "336238", "336934", "336935", "326934", "326935",
        # OP34 Tiffany Blue (2022+ catalog)
        "124200",
    ],
    "Paul Newman": [
        "6239", "6241", "6262", "6263", "6264", "6265",
        "116508", "126508",
        # Daytona YG leather strap variants (77 Paul Newman listings in wholesale)
        "116518", "126518LN",
        # Daytona YG (116528) — Paul Newman variant confirmed in wholesale data
        "116528",
    ],
    "Meteorite": [
        "116508", "116509", "116519", "116519LN",
        "126509", "126519", "126519LN",
        # 228206 (DD40 Platinum) excluded: FIXED_DIAL Ice Blue only
        "228235", "228238", "228239",
        "128238", "128235",
        "126334", "126333", "126331",
        # Legacy DJ41 Datejust variants with Meteorite
        "116334", "116331",
        "126719BLRO",
        # Additional Day-Date and gem-set refs offering Meteorite
        "228349", "228396", "228236",
        "118235", "118206",
        # Daytona WG/YG Sapphire & Pave — Meteorite dial catalogued option
        "116576", "116598",
        "116589", "116589SACI",
        # Daytona RG/YG leather & additional Daytona variants (in dial_options)
        "116505", "116515", "116518", "116528",
        # Rolex 1908 Everose (52509) — Meteorite confirmed in wholesale data
        "52509",
        # Sky-Dweller WG gem-set (326259) — Meteorite confirmed in wholesale data
        "326259",
    ],
    "Wimbledon": [
        "126334", "126331", "126333", "126238",
        "116334", "116331",
        "126201", "126301",
        # Additional Datejust variants where Wimbledon is offered
        "126303", "126233", "126231",
        "126283", "126203",
        # Legacy steel Datejust (vintage Wimbledon motif)
        "116300", "126200",
        # DJ36 WG — Wimbledon in official dial_options
        "126234",
        # DJ41 steel — Wimbledon confirmed in wholesale data
        "126300",
        # DJ36 steel — Wimbledon confirmed
        "126231",
        # Legacy Datejust 36/34 (ref 15000 series) — Wimbledon confirmed in wholesale
        "15000", "15010", "15050",
    ],
    "Ice Blue": [
        "228206", "128236", "127236", "228396TBR", "128396TBR",
        # Yacht-Master 42 Platinum (116/126 generation)
        "116506", "116506A", "126506",
        # Day-Date 40 Platinum variant
        "228236",
        # Legacy Day-Date Platinum / gem-set variants
        "118206", "118346", "118366",
        # Land-Dweller Platinum variants
        "127286", "127336", "127386",
        # Day-Date 36/40 with Ice Blue baguette variants
        "128396", "228396",
        # Daytona WG Pave / Sapphire (rare but catalogued)
        "116576", "116598",
        # DD36 Everose (Ice Blue in dial_options)
        "128235",
        # Day-Date II Platinum (Ice Blue in dial_options)
        "218206",
        # DD36 Platinum baguette variant
        "127385",
        # Rolex 1908 Platinum (Ice Blue confirmed)
        "52506",
    ],
    "Ice Blue Baguette": [
        "127286", "127386", "128396", "228396", "228396TBR", "128396TBR",
        "118346",
        # DD36 Platinum (Ice Blue Baguette in dial_options)
        "128236",
        # YM42 Platinum — Ice Blue Baguette confirmed in dial_options
        "126506", "116506",
    ],
    "Turquoise Stone": [
        "228345", "228235", "228238", "228239", "228349",
        "128345", "128235", "128238", "128239",
        # DD36 gem-set stone bracelet and additional refs (in dial_options)
        "128395", "118238", "128398", "128159",
        # DD36 WG baguette (128396) — confirmed wholesale
        "128396",
        # WG Daytona dealer-special Turquoise/Beach (confirmed wholesale)
        "116519", "116519LN", "116509",
    ],
    "Tiger Eye": [
        "18038", "18238", "118238",
        "128238", "228238",
        # Daytona WG/YG Sapphire and gem-set variants
        "116589", "116589SACI", "116598", "116588", "116518",
        "228345", "128345",
    ],
    "Lapis Lazuli": [
        "18038", "18238",
        "128238", "228238", "228235",
        # 228345/128345 stone-bracelet DD40/36 also offer Lapis Lazuli
        "228345", "128345",
    ],
    "Aventurine": [
        "128345", "228235", "228238", "228349",
        # 228345 (all-stone bracelet DD40) and 228349 gem-set also offer Aventurine
        "228345",
        # DD36 Everose — Aventurine confirmed in dial_options
        "128235",
        # DD36 WG baguette and gem-set bracelet (confirmed wholesale + catalog)
        "128395", "128158",
        # YM42 gem-set platinum (wholesale confirmed)
        "116506",
    ],
    "Grossular": [
        "126555", "118338", "118348",
    ],
    # Carnelian — reddish-brown semi-precious stone dial (Day-Date 36/40)
    "Carnelian": [
        "128238", "128239", "128235", "128345", "128398",
        "228238", "228235", "228239", "228349", "228345",
        "118238", "118235",
    ],
    "Onyx": [
        "228235", "228238", "228239",
        # Day-Date 40 Platinum and additional variants
        "228236", "218239", "118239",
        # DD40 gem-set and stone-bracelet variants (confirmed wholesale)
        "228348", "228345", "228349", "228398",
        # Legacy Datejust (15000 series) — Onyx confirmed in wholesale data
        "15000", "15010",
    ],
    "Ombre": [
        "228235",
        # Day-Date 36 variants (Everose, WG, YG) also offer Ombré dials
        "128235", "128238", "128239",
        # Datejust 31 offers special Ombré dials
        "278288", "278289",
        # Day-Date 40 additional variants (confirmed wholesale)
        "228236", "228349", "228238", "228239", "228348", "228398",
        "228345",
        # Day-Date 36 platinum/baguette variants
        "128399", "128349",
        # Rolex Date (15210) Ombré special dials
        "15210",
    ],
    "Ombre Slate":  [
        "228235", "228345", "228238", "228239",
        "128235", "128238", "128239",
    ],
    # Red Ombré — Datejust 31 + gem-set variants (278278: 98 listings in wholesale)
    "Red Ombré":    ["278289", "278288", "278278", "128238"],
    # Green Ombré — DD36/DD40 variants (high wholesale volume: 82/33/36 listings)
    "Green Ombré":  [
        "278288", "228348", "228398",
        "228235", "228238", "228239", "228345",
        "128238", "128239", "128235", "128398",
    ],
    "Eisenkiesel": [
        "228235", "228238", "228239",
        # Day-Date 36 variants (all precious-metal DD36 can be ordered with Eisenkiesel)
        "128235", "128238", "128239", "128395", "128345",
    ],
    "D-Blue":       ["126660"],
    "Candy Pink":   ["124300", "124200", "126000", "277200", "134300"],
    "Apple Green":  ["124300", "124200", "126000"],
    "Coral Red":    ["124300", "124200", "126000", "277200", "279160"],
    # Stone dials — new for 2023-2025 catalog
    "Sodalite": [
        "116589", "116589SACI", "126589",
        # WG Daytona dealer-special Sodalite (confirmed wholesale)
        "116519", "116519LN", "116509",
    ],
    "Malachite": [
        "278288", "278289",
        # DD36 YG — confirmed in wholesale data
        "128238", "128235",
    ],
    "Opal": [
        "118208", "118238",
    ],
    # Bright Green — Day-Date 40/36 lacquer dial (2023-2025 catalog; high wholesale volume)
    "Bright Green": [
        "228345", "228349",
        # DD40 solid-gold variants (confirmed in wholesale: 266/151/91/75 listings)
        "228238", "228235", "228239", "228348", "228398",
        # DD40 Platinum / baguette variants
        "228236",
        # DD36 variants (in dial_options)
        "128235", "128238", "128239",
    ],
    # Rainbow — Daytona diamond-set bezel and Day-Date stone variants
    "Rainbow": [
        "116505", "116595RBOW", "126595RBOW", "116599RBOW",
        # Additional Daytona gem-set models with rainbow bezel
        "116576", "116598", "116520", "116509",
        "116595", "116759",
        "126599",
        # Daytona RG Rainbow — dedicated rainbow-bezel reference (wholesale: 15/15 listings)
        "116285BBR", "116285",
        # Day-Date 36 stone dial with rainbow
        "128395", "128345", "228349", "228345",
        # DD36 YG/RG (128238) catalogued with Rainbow dial
        "128238",
        # DD36 WG (128239) catalogued with Rainbow dial (in dial_options)
        "128239",
        # DD36 Everose (128235) confirmed in dial_options and wholesale
        "128235",
        # Gem-set Daytona (116599) rainbow variant
        "116599",
        # Miscellaneous gem-set refs
        "268655", "279458",
    ],
    # Celebration — Day-Date ornate precious-stone dial (all DD36/DD40 gold/plat)
    "Celebration": [
        "228235", "228238", "228239", "228345", "228349",
        "128235", "128238", "128239", "128158",
        # Legacy Day-Date
        "118238", "118208", "18038", "18238",
        # OP36/OP31/OP41 special Celebration dials (confirmed wholesale)
        "126000", "277200", "134300", "124300", "124200",
    ],
    # Puzzle — Day-Date 40 abstract segmented dial
    "Puzzle": [
        "228235", "228238", "228239", "228345", "228349",
        "128235", "128238", "128239",
        "118238", "18038", "18238",
    ],
    # Jubilee Motif — Datejust special-order dial with jubilee pattern
    "Jubilee Motif": [
        "126234", "126238", "126233", "126200", "126201",
        "126334", "126333", "126300", "126301",
        "116234", "116200",
    ],
    # Stella — 1970s-80s lacquer Day-Date, extremely collectible
    "Stella": [
        "1803", "1804", "1807", "1808",
        "18038", "18238", "18039", "18239",
        "18039", "18348",
    ],
    # Panda / Reverse Panda — white/black Daytona dials; promote to premium so Stage 3
    # can override the suffix-inferred 'Black' that Stage 2 sets for LN-suffix refs.
    "Panda": [
        "126500LN", "116500LN",
        "116509", "126509",
        "116519LN", "126519LN",
        "116503", "116505",
    ],
    "Reverse Panda": [
        "126500LN", "116500LN",
    ],
    # Pave — full diamond dial across Day-Date, Daytona gem-set, and high-jewellery Datejust
    "Pave": [
        "228235", "228239", "228345", "228349",
        "128235", "128238", "128239", "128345",
        "118208", "118238", "118348",
        "116589", "126589",
        # Day-Date 36 stone/special refs
        "128158", "128159", "128395",
        # Daytona WG/YG Pave and Sapphire
        "116576", "116578", "116578SACO", "116579",
        # High-jewellery gem-set models (Datejust/YM diamond)
        "126679", "126281", "279458", "279381", "278381",
        "279138", "116659", "116243",
        "116595", "116758",
        "268655", "126755",
        # Yacht-Master Platinum/gem-set Pave
        "116695", "116695SATS", "116655",
        # Additional Day-Date 40 variants with Pave (confirmed wholesale)
        "228348", "228238", "228236", "228396", "228398",
        # Additional gem-set and Daytona variants
        "116588", "116599", "226668", "226679",
        "116509", "116505", "126506", "116506",
        # Datejust / Sea-Master / DJ baguette Pave
        "126539", "128396",
        # Rolex Date / Sky-Dweller Pave
        "15210", "279171",
        # Daytona Rainbow gem-set (116759SANR) — Pave confirmed
        "116759SANR", "116759",
        # Lady-Datejust gem-set (278243, 278344) — Pave confirmed in wholesale
        "278243", "278344",
        # Sky-Dweller WG gem-set (326259) — Pave in wholesale data
        "326259",
        # AP Royal Oak Offshore gem-set (15510ST) — Pave in wholesale data
        "15510ST",
    ],
}

# ---------------------------------------------------------------------------
# Premium dial patterns — (compiled_regex, canonical_name, priority 0-100)
#   Higher priority wins when multiple patterns match the same text.
#   Turquoise Stone must precede plain turquoise to avoid false mapping.
# ---------------------------------------------------------------------------
_PREMIUM_PATTERNS = [
    # Tiffany Blue / Turquoise Blue (OP models)
    # T&Co / Tiffany & Co dealer shorthands — highest priority
    (re.compile(r"\btiffany\s*&\s*co\b",           re.I),      "Tiffany Blue",    100),
    (re.compile(r"\bt\s*&\s*co\b",                 re.I),      "Tiffany Blue",    100),
    # "tco" — shorthand without ampersand used in HK/Asia dealer listings
    (re.compile(r"\btco\b",                        re.I),      "Tiffany Blue",     90),
    (re.compile(r"\btiff(?:any)?(?:\s+blue)?\b",   re.I),      "Tiffany Blue",    100),
    # Tiffany Blue typo variants — dealer manual-input errors; common in secondary market
    (re.compile(r"\btifffany\b|\btifanny\b|\btifany\b", re.I), "Tiffany Blue",    90),
    (re.compile(r"\bturquoise\s+blue\b",           re.I),      "Tiffany Blue",    100),
    (re.compile(r"\brobin'?s?\s+egg\s*blue?\b",    re.I),      "Tiffany Blue",    100),
    (re.compile(r"\bofficial\s+tiff(?:any)?\b",    re.I),      "Tiffany Blue",    100),
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
    # "Newman" standalone — lower confidence, requires Daytona context for full value
    (re.compile(r"\bnewman\b",                     re.I),      "Paul Newman",      65),
    # Meteorite — "white pepsi" / "wg pepsi" is the 126719BLRO dealer nickname → Meteorite
    (re.compile(r"\bwhite\s+(?:gold\s+)?pepsi\b|\bwg\s+pepsi\b|\bpepsi\s+wg\b", re.I), "Meteorite", 95),
    (re.compile(r"\bmeteor(?:ite)?\b",             re.I),      "Meteorite",       100),
    (re.compile(r"\bmeteo\b",                      re.I),      "Meteorite",        90),
    (re.compile(r"\bmete\b",                       re.I),      "Meteorite",        85),
    (re.compile(r"\bmet\b",                        re.I),      "Meteorite",        68),
    # Wimbledon
    (re.compile(r"\bwimbledon\b",                  re.I),      "Wimbledon",       100),
    (re.compile(r"\bwimbo\b",                      re.I),      "Wimbledon",        90),
    (re.compile(r"\bwimb?\b",                      re.I),      "Wimbledon",        85),
    # Ice Blue Baguette / Stick composite — must precede plain Ice Blue to win priority
    (re.compile(r"\bice\s*blue\s+baguette\b",      re.I),      "Ice Blue Baguette", 105),
    (re.compile(r"\bib\s+baguette\b",              re.I),      "Ice Blue Baguette",  90),
    # Ice Blue (platinum models only)
    (re.compile(r"\bice\s*blue\b",                 re.I),      "Ice Blue",        100),
    (re.compile(r"\biceblue\b",                    re.I),      "Ice Blue",        100),
    (re.compile(r"\bib\b",                         re.I),      "Ice Blue",         65),
    # D-Blue (Deepsea 126660)
    (re.compile(r"\bd[\s\-]?blue\b",               re.I),      "D-Blue",          100),
    (re.compile(r"\bjames\s+cameron\b",            re.I),      "D-Blue",          100),
    # "blue" alone for 126660 — dealers often label the D-Blue dial as just "blue"
    # For non-126660 refs: eff = max(85-45,10)=40 < threshold → no-op
    (re.compile(r"\bblue\b",                       re.I),      "D-Blue",           85),
    # Ombré variants — specific before generic
    (re.compile(r"\bombr[eE\xe9]\s+slate\b",               re.I), "Ombre Slate",  100),
    # reversed-order alias: "slate ombre" / "slate ombré"
    (re.compile(r"\bslate\s+ombr[eE\xe9]\b",               re.I), "Ombre Slate",  100),
    (re.compile(r"\bred\s+ombr[eE\xe9]\b",                 re.I), "Red Ombré",    100),
    # "gradient red" / "red gradient" — informal ombré descriptions
    (re.compile(r"\bgradient\s+red\b|\bred\s+gradient\b",  re.I), "Red Ombré",     90),
    (re.compile(r"\bgreen\s+ombr[eE\xe9]\b",               re.I), "Green Ombré",  100),
    # "gradient green" / "green gradient" — Stage 3 must intercept before Stage 5 catches "green"
    (re.compile(r"\bgradient\s+green\b|\bgreen\s+gradient\b", re.I), "Green Ombré", 90),
    # reversed-order: "ombre red" / "ombre green" — must precede plain \bombré\b so they win priority
    (re.compile(r"\bombr[eE\xe9]\s+red\b",                 re.I), "Red Ombré",    100),
    (re.compile(r"\bombr[eE\xe9]\s+green\b",               re.I), "Green Ombré",  100),
    (re.compile(r"\bombr[eE\xe9]\b",                       re.I), "Ombre",        100),
    # Stone dials — Turquoise Stone before plain turquoise
    (re.compile(r"\bturquoise\s*(?:stone|dial)\b", re.I),      "Turquoise Stone", 100),
    (re.compile(r"\baventurine\b",                 re.I),      "Aventurine",      100),
    (re.compile(r"\bgrossular\b",                  re.I),      "Grossular",       100),
    (re.compile(r"\bgiraffe\b",                    re.I),      "Grossular",       100),
    (re.compile(r"\bonyx\b",                       re.I),      "Onyx",            100),
    (re.compile(r"\blapis\s+lazuli\b",             re.I),      "Lapis Lazuli",    100),
    (re.compile(r"\blapis\b",                      re.I),      "Lapis Lazuli",     90),
    (re.compile(r"\btiger'?s?\s*eye\b",            re.I),      "Tiger Eye",       100),
    # Additional stone/special dials (2023-2025 catalog additions)
    (re.compile(r"\bsodalite\b",                   re.I),      "Sodalite",        100),
    (re.compile(r"\bmalachite\b",                  re.I),      "Malachite",       100),
    (re.compile(r"\bopal\b",                       re.I),      "Opal",            100),
    # Carnelian — reddish semi-precious stone dial; Day-Date exclusive
    (re.compile(r"\bcarnelian\b",                  re.I),      "Carnelian",       100),
    # Carnelian typo variants — common manual-entry errors in dealer listings
    (re.compile(r"\bcarneilian\b|\bcarnilian\b|\bcarnelion\b|\bcarnelean\b|\bcarneleon\b", re.I), "Carnelian", 90),
    (re.compile(r"\bcarn\b",                       re.I),      "Carnelian",        72),
    # Bright Green lacquer (Day-Date 40 special)
    (re.compile(r"\bbright\s*green\b|\bavocado\b", re.I),      "Bright Green",     90),
    # Rainbow (Daytona diamond bezel)
    (re.compile(r"\brainbow\b",                    re.I),      "Rainbow",          95),
    # Candy Pink / Apple Green / Coral Red (OP special colours)
    (re.compile(r"\bcandy\s*pink\b",               re.I),      "Candy Pink",      100),
    (re.compile(r"\bapple\s*green\b",              re.I),      "Apple Green",     100),
    (re.compile(r"\bcoral\s+red\b",                re.I),      "Coral Red",       100),
    (re.compile(r"\bcoral\b|\bcherry\s+red\b",     re.I),      "Coral Red",        75),
    # "candy" alone → Candy Pink shorthand (OP special); non-OP refs get -45 penalty → below threshold
    (re.compile(r"\bcandy\b",                      re.I),      "Candy Pink",       65),
    # Pave / MOP
    (re.compile(r"\bpav[eE\xe9]\b",               re.I),      "Pave",            100),
    (re.compile(r"\bmother\s+of\s+pearl\b",        re.I),      "MOP",             100),
    (re.compile(r"\bmop\b",                        re.I),      "MOP",              90),
    # Special dials
    (re.compile(r"\bpuzzle\b",                     re.I),      "Puzzle",          100),
    (re.compile(r"\bcelebration\b",                re.I),      "Celebration",     100),
    (re.compile(r"\beisenk(?:iesel)?\b",           re.I),      "Eisenkiesel",     100),
    (re.compile(r"\beisen\b",                      re.I),      "Eisenkiesel",      90),
    # "eisk" — ultra-short Eisenkiesel abbreviation used in HK/Asia dealer listings
    (re.compile(r"\beisk\b",                       re.I),      "Eisenkiesel",      85),
    # Jubilee Motif dial (special Day-Date / Datejust dial pattern)
    (re.compile(r"\bjubilee\s+(?:motif|dial)\b",   re.I),      "Jubilee Motif",    95),
    # Pave shorthand 'pv' — HK/Asia dealer abbreviation
    (re.compile(r"\bpv\b",                         re.I),      "Pave",             70),
    # Stella — 1970s-80s vintage Day-Date lacquer dial (extreme collector premium)
    (re.compile(r"\bstella\b",                     re.I),      "Stella",          100),
    # Panda / Reverse Panda / Tuxedo — white-dial Daytonas command a premium over black.
    # Promoted to premium so Stage 3 can override the LN suffix-inferred 'Black' (Stage 2).
    (re.compile(r"\breverse\s*panda\b|\brev\s*panda\b", re.I), "Reverse Panda",   85),
    # 'rp' — Reverse Panda shorthand used by HK/Asia dealers and online listings.
    # Low priority (70) so it only fires when ref-validated; penalty drops it to 30
    # for non-RP refs, well below the 0.60 threshold.
    (re.compile(r"\brp\b",                         re.I),      "Reverse Panda",    70),
    (re.compile(r"\btuxedo\b",                     re.I),      "Panda",            80),
    (re.compile(r"\bpanda\b",                      re.I),      "Panda",            80),
    # Tiffany Blue — no-space variant ("tiffanyblue") and egg-blue abbreviation
    (re.compile(r"\btiffanyblue\b",                re.I),      "Tiffany Blue",    100),
    (re.compile(r"\begg\s*blue\b",                 re.I),      "Tiffany Blue",     85),
    # Wimbledon — sellers sometimes write "wimbledon green" or "green wimbledon"
    (re.compile(r"\bwimbledon\s*green\b|\bgreen\s*wimbledon\b", re.I), "Wimbledon", 95),
    # Informal Tiffany Blue descriptors — widely used in European/Asian dealer listings.
    # Priority 70-80 → eff ≥ 0.60 for OP refs (Tiffany Blue allowed) → returns Tiffany Blue.
    # For non-OP refs eff = 25-35 (<0.60 threshold) → falls through to Stage 5 → plain Blue.
    (re.compile(r"\blight\s*blue\b|\bpowder\s*blue\b|\bbaby\s*blue\b|\bpastel\s*blue\b", re.I), "Tiffany Blue", 70),
    # "celeste" — Italian/Spanish for "sky/heavenly blue"; standard term in EU market for OP Tiffany
    (re.compile(r"\bceleste\b",                                 re.I),      "Tiffany Blue",    80),
    # "teal" / "aqua" — casual descriptors used by private sellers for the OP Tiffany colour;
    # also used for Turquoise Stone (DD refs) which Stage 5 catches after Stage 3 rejects here.
    (re.compile(r"\bteal\b|\baqua(?:\s*blue)?\b",              re.I),      "Tiffany Blue",    65),
]

# ---------------------------------------------------------------------------
# Standard colour patterns — lower priority, broad matching
# ---------------------------------------------------------------------------
_COLOR_PATTERNS = [
    (re.compile(r"\bblack\b|\bblk\b|\bbk\b|\bblck\b",             re.I), "Black"),
    (re.compile(r"\bwhite\b|\bwht\b|\bwh\b",                       re.I), "White"),
    (re.compile(r"\bbright\s+blue\b|\bbb\b",                       re.I), "Bright Blue"),
    # teal/aqua before plain 'blue'/'green' — prevents "aqua blue" partial-matching "Blue Diamond"
    # and "aqua green" partial-matching "Bright Green" on refs that offer Turquoise Stone.
    (re.compile(r"\bteal\b|\baqua(?:\s*(?:blue|green))?\b",        re.I), "Turquoise Stone"),
    (re.compile(r"\bblue\b|\bblu\b",                               re.I), "Blue"),
    # "turq" shorthand — maps to Turquoise Stone here (Stage 5) for DD/non-OP refs.
    # For OP refs, Stage 3 premium scan intercepts turq → Tiffany Blue before Stage 5 runs.
    (re.compile(r"\bturq\b",                                       re.I), "Turquoise Stone"),
    (re.compile(r"\bturquoise\b",                                   re.I), "Turquoise Stone"),
    (re.compile(r"\bpistachio\s*(?:green)?\b",                     re.I), "Mint Green"),
    # "mint green" or "minty" only — standalone "mint" excluded to avoid "mint condition" FP
    (re.compile(r"\bmint\s+green\b|\bminty\b",                    re.I), "Mint Green"),
    (re.compile(r"\bsage\s+green\b|\bseafoam\s+green\b|\bseafoam\b|\bsage\b", re.I), "Mint Green"),
    (re.compile(r"\bolive\s*(?:green)?\b|\bog\b",                 re.I), "Olive Green"),
    (re.compile(r"\bpalm\s*(?:green)?\b",                          re.I), "Palm Green"),
    (re.compile(r"\bapple\s*green\b",                              re.I), "Apple Green"),
    (re.compile(r"\bgreen\b|\bgrn\b|\bgg\b|\bstarbucks\b|\bhulk\b|\bkermit\b", re.I), "Green"),
    (re.compile(r"\bsilver\b|\bslvr\b|\bslv\b|\bbenz\b",          re.I), "Silver"),
    (re.compile(r"\bchampagne\b|\bchamp\b|\bcham\b|\bchp\b",      re.I), "Champagne"),
    (re.compile(r"\bchocolate\b|\bchoco\b|\bcho\b",                re.I), "Chocolate"),
    (re.compile(r"\bgr[ae]y\b|\bghost\b|\bpewter\b|\bcharcoal\b", re.I), "Grey"),
    (re.compile(r"\bsalmon\b",                                     re.I), "Salmon"),
    (re.compile(r"\baubergine\b|\baub\b|\bpurp(?:le)?\b|\bgrape\b|\bplum\b", re.I), "Aubergine"),
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
    # Colors missing from original list — added to resolve wholesale data gaps
    (re.compile(r"\bred\b",                                        re.I), "Red"),
    (re.compile(r"\bskeleton(?:ized)?\b|\bopenwork(?:ed)?\b|\bopen\s*heart\b", re.I), "Skeletonized"),
    (re.compile(r"\bbeige\b",                                      re.I), "Beige"),
    (re.compile(r"\borange\b|\btangerine\b",                       re.I), "Orange"),
    (re.compile(r"\btaupe\b",                                      re.I), "Taupe"),
    (re.compile(r"\blavender\b|\blilac\b",                         re.I), "Lavender"),
    (re.compile(r"\bburgund(?:y)?\b",                              re.I), "Burgundy"),
    (re.compile(r"\bazzurro\b",                                    re.I), "Bright Blue"),
    (re.compile(r"\bcarnelian\b",                                  re.I), "Carnelian"),
    # teal/aqua handled at top of _COLOR_PATTERNS (before blue/green) to prevent partial mismatches
    # Jade — informal for green dials (Day-Date stone and lacquer)
    (re.compile(r"\bjade(?:\s*green)?\b",                       re.I), "Green"),
    # Celeste — Italian/Spanish for "sky blue"; Stage 3 intercepts for OP refs as Tiffany Blue;
    # for all other refs (DD, DJ, etc.) falls here as plain Blue.
    (re.compile(r"\bceleste\b",                                 re.I), "Blue"),
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

# Composite dial pattern — "Base Color + Modifier" (e.g. "Black Baguette", "Silver Diamond")
# Catches dealer dial labels that Stage 5 validation would reject because they aren't
# in the simple color list; returns the full composite name rather than just the base.
_COMPOSITE_DIAL_RE = re.compile(
    r"\b(black|white|silver|blue|green|red|pink|champagne|chocolate|gr[ae]y|gold|"
    r"brown|olive|salmon|sundust|aubergine|ivory|yellow|mint\s+green|olive\s+green|"
    r"bright\s+blue|coral|turquoise|meteorite|lavender|beige|orange|tiffany\s+blue|"
    r"ice\s+blue|bright\s+green|rhodium|carnelian)"
    r"\s+(baguette\s+diamond|stick\s+roman\s+vi\s+ix\s+diamond|stick\s+diamond|"
    r"roman\s+vi\s+ix\s+diamond|roman\s+vi\s+ix|roman\s+vi|roman|baguette|stick|diamond|"
    r"pav[e\xe9\xc9]|mop|omber|ombr[e\xe9])\b",
    re.I,
)


# ---------------------------------------------------------------------------
# _LAST_RESORT_DIAL — Stage 7: fallback when every text stage returns nothing.
#   Only fires when result["dial"] is still None after Stages 1-6.
#   Values chosen where one dial is >65% of non-empty wholesale listings
#   AND the model/material strongly implies it (not just statistical noise).
#   Confidence: 0.55 — below every text-based stage so text always wins.
# ---------------------------------------------------------------------------
_LAST_RESORT_DIAL = {
    # Yacht-Master 42 Platinum: Ice Blue is the catalog offering (69% of non-empty)
    "116506":  "Ice Blue",
    "116506A": "Ice Blue",
    # Daytona YG Diamond (116588): Tiger Eye is 83% dominant
    "116588":  "Tiger Eye",
    # Daytona WG gem-set 2020+ (126599): Rainbow is 77% dominant
    "126599":  "Rainbow",
    # Day-Date 36 Aubergine bracelet (118348): Aubergine is 67% dominant
    "118348":  "Aubergine",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ref_clean(ref):
    return ref.upper().strip().replace(" ", "") if ref else None


def _norm_dial(s):
    """Accent-insensitive lowercase for dial comparisons (é→e, etc.)."""
    return (s.replace('é', 'e').replace('è', 'e').replace('ê', 'e')
              .replace('É', 'E').replace('È', 'E').replace('Ê', 'E')
              .lower())


def _premium_allowed(premium, ref):
    """True if this premium dial is catalogued for the given reference."""
    allowed = _PREMIUM_REF_MAP.get(premium)
    if allowed is None or ref is None:
        return True
    rc = _ref_clean(ref)
    if any(rc.startswith(r.upper()) for r in allowed):
        return True
    # Override: if the ref's dial_options explicitly list this dial, trust the catalog.
    # Prevents _PREMIUM_REF_MAP gaps from blocking legitimate dials (e.g. Aventurine on 128395).
    if rc in _DIAL_OPTIONS and premium in _DIAL_OPTIONS[rc]:
        return True
    return False


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
        eff = priority if _premium_allowed(canonical, ref) else max(priority - 45, 10)
        if eff > best_priority:
            best_priority = eff
            best = {
                "dial":         canonical,
                "confidence":   min(eff / 100.0, 1.0),
                "matched_text": m.group(0),
                "is_premium":   True,
            }

    return best


def _check_premium(canonical, ref=None):
    """True when canonical is a documented premium dial valid for this ref."""
    if not canonical or canonical not in _PREMIUM_REF_MAP:
        return False
    return _premium_allowed(canonical, ref)


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
        fixed = FIXED_DIAL[rc]
        is_fp = _check_premium(fixed, ref)
        result.update({
            "dial":         fixed,
            "confidence":   1.0,
            "is_premium":   is_fp,
            "premium_type": fixed if is_fp else None,
            "method":       "fixed_dial",
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

    # Stage 2 result handling:
    # · Single-option ref (suffix uniquely determines dial) → return now.
    # · Multi-option ref → explicit colour text in listing should override the
    #   suffix guess; fall through to Stages 4-6 so the text can be read.
    #   Stage 2's result stays in `result` as a fallback: if Stages 4-6 find
    #   nothing, the final `return result` will use it.
    if result["dial"]:
        _single_option = not (rc and rc in _DIAL_OPTIONS and len(_DIAL_OPTIONS[rc]) > 1)
        if _single_option:
            return result
        # else: multi-option ref → fall through to Stages 4-6

    # Stage 4 — Contextual extraction: "dial: X", "colour: X", "with X dial", "featuring X dial"
    for ctx_re in (_DIAL_CONTEXT_RE, _WITH_DIAL_RE, _FEATURING_DIAL_RE):
        for raw_ctx in ctx_re.findall(text):
            stripped = raw_ctx.strip().lower()
            if stripped in _CTX_REJECT or len(stripped) < 3:
                continue
            normalised = normalize_dial(stripped, ref)
            known = stripped in _DIAL_SYNONYMS or normalised in _ALL_COLOR_CANONICALS
            if normalised and known:
                # Guard: premium dial not catalogued for this ref → skip to next match
                if normalised in _PREMIUM_REF_MAP and rc and not _premium_allowed(normalised, rc):
                    continue
                is_p = _check_premium(normalised, ref)
                result.update({
                    "dial":         normalised,
                    "confidence":   0.85,
                    "is_premium":   is_p,
                    "premium_type": normalised if is_p else None,
                    "method":       "context_match",
                })
                return result

    # Stage 4.5 — Composite dial: "Base Color + Modifier" (e.g. "Black Baguette")
    # Runs before the single-token color scan to preserve the full composite name.
    m_comp = _COMPOSITE_DIAL_RE.search(text)
    if m_comp:
        composite = " ".join(m_comp.group(0).split()).title()
        # Restore Roman numerals that title() lowercases (VI→Vi, IX→Ix, etc.)
        composite = re.sub(r'\b(Vi|Ix|Xi|Xiv|Iv|Iii|Ii)\b', lambda x: x.group().upper(), composite)
        # Guard: for known refs, reject composites not in the ref's valid dial list.
        # Prevents e.g. "yellow gold diamond" → "Gold Diamond" for 116588 (Tiger Eye ref).
        _comp_valid = True
        if rc and rc in _DIAL_OPTIONS:
            valid = _DIAL_OPTIONS[rc]
            _cn = _norm_dial(composite)
            _comp_valid = composite in valid or any(
                _norm_dial(v) == _cn or composite.lower() in v.lower() or v.lower() in composite.lower()
                for v in valid
            )
        if _comp_valid:
            is_p = _check_premium(composite, ref)
            result.update({
                "dial":         composite,
                "confidence":   0.80,
                "is_premium":   is_p,
                "premium_type": composite if is_p else None,
                "method":       "composite_dial",
            })
            return result

    # Stage 5 — Colour token scan
    _color_fallback = None  # first unvalidated match; used in Stage 5b
    for pattern, canonical in _COLOR_PATTERNS:
        if not pattern.search(text):
            continue
        if rc and rc in _DIAL_OPTIONS:
            valid = _DIAL_OPTIONS[rc]
            # Exact match — also accent-insensitive (Pavé == Pave, Ombré == Ombre)
            _canon_n = _norm_dial(canonical)
            _exact = canonical if canonical in valid else next(
                (v for v in valid if _norm_dial(v) == _canon_n), None
            )
            if _exact:
                is_p = _check_premium(_exact, ref)
                result.update({
                    "dial":         _exact,
                    "confidence":   0.82,
                    "is_premium":   is_p,
                    "premium_type": _exact if is_p else None,
                    "method":       "color_pattern_validated",
                })
                return result
            for v in valid:
                if canonical.lower() in v.lower() or v.lower() in canonical.lower():
                    is_p = _check_premium(v, ref)
                    result.update({
                        "dial":         v,
                        "confidence":   0.72,
                        "is_premium":   is_p,
                        "premium_type": v if is_p else None,
                        "method":       "color_pattern_partial",
                    })
                    return result
            # Color matched but not in validated list — remember as low-confidence fallback
            if _color_fallback is None:
                _color_fallback = canonical
        else:
            is_p = _check_premium(canonical, ref)
            result.update({
                "dial":         canonical,
                "confidence":   0.60,
                "is_premium":   is_p,
                "premium_type": canonical if is_p else None,
                "method":       "color_pattern_unvalidated",
            })
            return result

    # Stage 5b — Color fallback: pattern matched but ref validation rejected it.
    # Guard: if the ref has KNOWN dial options and the colour isn't among them,
    # suppress the result rather than return a likely-wrong classification.
    # Only emit when the ref is uncharted (not in dial_options) — in that case
    # 0.45-confidence is better than nothing.
    if _color_fallback:
        if rc and rc in _DIAL_OPTIONS:
            pass  # Ref is known; wrong colour → skip, fall through to Stage 6
        else:
            is_p = _check_premium(_color_fallback, ref)
            result.update({
                "dial":         _color_fallback,
                "confidence":   0.45,
                "is_premium":   is_p,
                "premium_type": _color_fallback if is_p else None,
                "method":       "color_fallback",
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
        # Guard: 'mint' must not resolve to Mint Green when used as a condition descriptor.
        # Covers: "mint condition", "near mint", "almost mint", "mint 9/10", "mint-like new"
        if raw_key == "mint" and re.search(
            r"\bmint\s+(?:condition|unworn|state|box|set|complete)\b"
            r"|\b(?:near|almost|virtually|essentially)\s+mint\b"
            r"|\bmint[\s\-]+(?:like\s*new|\d)",
            text_lower
        ):
            continue
        if not re.search(r"\b" + re.escape(raw_key) + r"\b", text_lower):
            continue
        # Guard: if this synonym resolves to a premium dial, validate it against the ref.
        # This prevents e.g. "turq" → Tiffany Blue firing on a Day-Date listing.
        if canonical in _PREMIUM_REF_MAP and rc and not _premium_allowed(canonical, rc):
            # Tiffany Blue shorthands (tb/turq/tiff/tco) on DD/gem-set refs that offer
            # Turquoise Stone → remap rather than drop, since dealers use the same
            # shorthand for both dials depending on model context.
            if canonical == "Tiffany Blue" and rc in _DIAL_OPTIONS and "Turquoise Stone" in _DIAL_OPTIONS[rc]:
                canonical = "Turquoise Stone"
            else:
                continue
        # Guard: when the ref has known dial options, skip synonyms that resolve to
        # a dial not offered on that ref. Prevents "mint" → Mint Green on a DJ41 steel.
        if rc and rc in _DIAL_OPTIONS:
            valid = _DIAL_OPTIONS[rc]
            canon_l = canonical.lower()
            _canon_n = _norm_dial(canonical)
            if canonical not in valid and not any(
                _norm_dial(v) == _canon_n or v.lower().startswith(canon_l) or v.lower() in canon_l
                for v in valid
            ):
                continue
        is_p = _check_premium(canonical, ref)
        result.update({
            "dial":         canonical,
            "confidence":   0.65,
            "is_premium":   is_p,
            "premium_type": canonical if is_p else None,
            "method":       "synonym_scan",
        })
        return result

    # Stage 7 — model inference (last resort when all text stages yield nothing)
    if rc and not result["dial"] and rc in _LAST_RESORT_DIAL:
        inferred = _LAST_RESORT_DIAL[rc]
        is_p = _check_premium(inferred, ref)
        result.update({
            "dial":         inferred,
            "confidence":   0.55,
            "is_premium":   is_p,
            "premium_type": inferred if is_p else None,
            "method":       "model_inference",
        })

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
