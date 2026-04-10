"""
currency.py — Per-message currency detection for watch listings.

Handles mixed-currency groups where USD, HKD, EUR, AED etc. appear
in the same chat. Uses explicit symbol/code detection first, then
magnitude-based heuristics for bare $ amounts.
"""

import re
from typing import Optional
from loguru import logger


# ── Explicit Currency Patterns ────────────────────────────────
# Ordered by specificity: check specific patterns before generic ones.
# Each entry: (compiled_regex, currency_code)

CURRENCY_PATTERNS = [
    # Explicit codes/symbols BEFORE the number
    (re.compile(r'(?:HKD|HK\s*\$)\s*[\d,]+', re.I), 'HKD'),
    (re.compile(r'(?:SGD|S\s*\$)\s*[\d,]+', re.I), 'SGD'),
    (re.compile(r'(?:TWD|NT\s*\$)\s*[\d,]+', re.I), 'TWD'),
    (re.compile(r'(?:AUD|A\s*\$)\s*[\d,]+', re.I), 'AUD'),
    (re.compile(r'(?:CAD|C\s*\$)\s*[\d,]+', re.I), 'CAD'),
    (re.compile(r'(?:USD|US\s*\$)\s*[\d,]+', re.I), 'USD'),
    (re.compile(r'€\s*[\d,]+', re.I), 'EUR'),
    (re.compile(r'(?:EUR)\s*[\d,]+', re.I), 'EUR'),
    (re.compile(r'£\s*[\d,]+', re.I), 'GBP'),
    (re.compile(r'(?:GBP)\s*[\d,]+', re.I), 'GBP'),
    (re.compile(r'(?:AED)\s*[\d,]+', re.I), 'AED'),
    (re.compile(r'(?:CHF)\s*[\d,]+', re.I), 'CHF'),
    (re.compile(r'(?:MYR|RM)\s*[\d,]+', re.I), 'MYR'),
    (re.compile(r'(?:JPY|¥)\s*[\d,]+', re.I), 'JPY'),
    (re.compile(r'(?:CNY|RMB)\s*[\d,]+', re.I), 'CNY'),
    (re.compile(r'(?:SAR)\s*[\d,]+', re.I), 'SAR'),
    (re.compile(r'(?:QAR)\s*[\d,]+', re.I), 'QAR'),

    # Codes AFTER the number
    (re.compile(r'[\d,]+\s*(?:HKD)', re.I), 'HKD'),
    (re.compile(r'[\d,]+\s*(?:SGD)', re.I), 'SGD'),
    (re.compile(r'[\d,]+\s*(?:USD)', re.I), 'USD'),
    (re.compile(r'[\d,]+\s*(?:EUR)', re.I), 'EUR'),
    (re.compile(r'[\d,]+\s*(?:GBP)', re.I), 'GBP'),
    (re.compile(r'[\d,]+\s*(?:AED)', re.I), 'AED'),
    (re.compile(r'[\d,]+\s*(?:CHF)', re.I), 'CHF'),
    (re.compile(r'[\d,]+\s*(?:MYR)', re.I), 'MYR'),
    (re.compile(r'[\d,]+\s*(?:AUD)', re.I), 'AUD'),
    (re.compile(r'[\d,]+\s*(?:CAD)', re.I), 'CAD'),
]

# ── Magnitude-Based Heuristics ────────────────────────────────
# Typical watch price ranges by currency
CURRENCY_RANGES = {
    'USD': (3_000, 350_000),
    'HKD': (25_000, 3_000_000),
    'SGD': (5_000, 500_000),
    'EUR': (2_500, 300_000),
    'GBP': (2_000, 280_000),
    'AED': (10_000, 1_300_000),
    'MYR': (15_000, 1_500_000),
    'TWD': (80_000, 10_000_000),
    'JPY': (400_000, 50_000_000),
    'CHF': (2_500, 300_000),
    'CNY': (20_000, 2_500_000),
}

# Price for bare $ (no explicit code)
_BARE_DOLLAR_RE = re.compile(r'\$\s*([\d,]+(?:\.\d+)?)\s*([kK])?')


def detect_currency(message: str, group_default: str = 'USD') -> str:
    """
    Detect currency from message text with group default fallback.

    Strategy:
    1. Check explicit currency patterns (HK$, €, £, USD, etc.)
    2. For bare $: disambiguate by magnitude
    3. Fall back to group_default

    Args:
        message: Raw message text.
        group_default: Default currency for this group/chat.

    Returns:
        ISO 4217 currency code (e.g., 'USD', 'HKD', 'EUR').
    """
    # Pass 1: explicit patterns
    for pattern, currency in CURRENCY_PATTERNS:
        if pattern.search(message):
            return currency

    # Pass 2: bare $ with magnitude heuristic
    m = _BARE_DOLLAR_RE.search(message)
    if m:
        amount = float(m.group(1).replace(',', ''))
        if m.group(2):  # k/K suffix
            amount *= 1000

        return _disambiguate_bare_dollar(amount, group_default)

    return group_default


def _disambiguate_bare_dollar(amount: float, group_default: str) -> str:
    """Disambiguate bare $ amount using magnitude and group context."""
    usd_lo, usd_hi = CURRENCY_RANGES['USD']
    hkd_lo, hkd_hi = CURRENCY_RANGES['HKD']
    sgd_lo, sgd_hi = CURRENCY_RANGES['SGD']

    usd_ok = usd_lo * 0.5 <= amount <= usd_hi * 1.5
    hkd_ok = hkd_lo * 0.5 <= amount <= hkd_hi * 1.5
    sgd_ok = sgd_lo * 0.5 <= amount <= sgd_hi * 1.5

    # Clear winner by magnitude
    if amount > 350_000:
        # Too high for USD — likely HKD, MYR, TWD, etc.
        if group_default in ('HKD', 'SGD', 'MYR', 'TWD', 'JPY', 'CNY', 'AED'):
            return group_default
        return 'HKD'  # most common high-value currency in watch groups

    if amount < 2_000:
        # Too low for any watch — might be "14.5" meaning 14,500
        return group_default

    # If group default is HKD and amount fits HKD range, trust group
    if group_default == 'HKD' and hkd_ok:
        return 'HKD'
    if group_default == 'SGD' and sgd_ok:
        return 'SGD'

    # Amount in USD range, not in HKD-only range
    if usd_ok and not (amount > usd_hi and hkd_ok):
        return 'USD'

    return group_default


def detect_currency_smart(
    message: str,
    ref: str,
    group_default: str = 'USD',
    ref_price_ranges: dict = None,
) -> tuple[str, float]:
    """
    Enhanced currency detection using reference-specific price ranges.

    Cross-checks detected price against known ranges for the reference
    to resolve ambiguous cases.

    Args:
        message: Raw message text.
        ref: Watch reference number.
        group_default: Default currency for this group.
        ref_price_ranges: Dict of {ref: {'usd': (lo, hi), 'hkd': (lo, hi), ...}}.

    Returns:
        Tuple of (currency_code, confidence) where confidence is 0.0-1.0.
    """
    # First: try explicit detection
    for pattern, currency in CURRENCY_PATTERNS:
        if pattern.search(message):
            return currency, 0.95

    # Extract price amount
    m = _BARE_DOLLAR_RE.search(message)
    if not m:
        return group_default, 0.3

    amount = float(m.group(1).replace(',', ''))
    if m.group(2):
        amount *= 1000

    # Check against ref-specific ranges if available
    if ref_price_ranges and ref in ref_price_ranges:
        ranges = ref_price_ranges[ref]
        plausibility = {}
        for curr, (lo, hi) in ranges.items():
            if lo * 0.7 <= amount <= hi * 1.3:
                plausibility[curr] = True
            else:
                plausibility[curr] = False

        plausible = [c for c, ok in plausibility.items() if ok]
        implausible = [c for c, ok in plausibility.items() if not ok]

        if len(plausible) == 1:
            return plausible[0], 0.90
        elif len(plausible) == 0:
            # Nothing fits — flag and fall back
            logger.warning(
                f"Currency ambiguous for {ref}: ${amount:,.0f} doesn't fit any known range. "
                f"Falling back to {group_default}."
            )
            return group_default, 0.2
        # Multiple plausible — use group default if it's one of them
        if group_default.upper() in plausible:
            return group_default, 0.6

    # Fall back to magnitude heuristic
    currency = _disambiguate_bare_dollar(amount, group_default)
    confidence = 0.7 if currency != group_default else 0.5
    return currency, confidence


if __name__ == '__main__':
    tests = [
        ("126710BLNR BNIB $14,500", "USD", "USD"),
        ("126710BLNR BNIB HK$112,000", "USD", "HKD"),
        ("126710BLNR BNIB €13,200", "USD", "EUR"),
        ("126710BLNR BNIB 14500 USD", "USD", "USD"),
        ("$112,000 126710BLNR", "HKD", "HKD"),
        ("AED 52,000 126710BLNR", "USD", "AED"),
        ("RM 68,000 126710BLNR", "USD", "MYR"),
        ("$14.5k 126710BLNR", "USD", "USD"),
        ("126710BLNR 450000", "HKD", "HKD"),
        ("£12,500 Sub Date", "USD", "GBP"),
        ("SGD 19,500 Batman", "USD", "SGD"),
    ]

    print("Currency Detection Tests:")
    for msg, default, expected in tests:
        result = detect_currency(msg, default)
        status = "✅" if result == expected else "❌"
        print(f"  {status} '{msg[:40]:<40s}' default={default} → {result} (expected {expected})")
