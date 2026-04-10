"""
dedupe.py — Fuzzy deduplication engine for watch listings.

Two-pass dedup:
1. Exact fingerprint match within time window → keep latest
2. Fuzzy: same seller + same ref + price within 2% + within window → keep latest
"""

import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from loguru import logger


@dataclass(frozen=True)
class ListingFingerprint:
    ref: str
    price_bucket: int      # round to nearest $250
    seller_norm: str
    region: str
    condition: str
    dial: str


def _normalize_seller(name: str) -> str:
    """Aggressive seller name normalization for matching."""
    name = name.lower().strip()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name)
    for noise in ['watch', 'watches', 'timepieces', 'luxury', 'official',
                   'inc', 'llc', 'ltd', 'the', 'group', 'trading', 'dealer']:
        name = re.sub(rf'\b{noise}\b', '', name)
    return name.strip()


def fingerprint(listing: dict) -> ListingFingerprint:
    """Deterministic fingerprint for exact dedup."""
    return ListingFingerprint(
        ref=listing.get('ref', ''),
        price_bucket=round(listing.get('price_usd', 0) / 250) * 250,
        seller_norm=_normalize_seller(listing.get('seller', '')),
        region=listing.get('region', ''),
        condition=listing.get('condition', ''),
        dial=listing.get('dial', ''),
    )


def _parse_ts(listing: dict) -> Optional[datetime]:
    """Parse listing timestamp. Returns None if unparseable."""
    ts = listing.get('ts', '')
    if not ts:
        return None
    for fmt in ('%d/%m/%y %H.%M.%S', '%d/%m/%y', '%m/%d/%Y %H:%M',
                '%m/%d/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(ts.strip(), fmt)
        except (ValueError, IndexError):
            continue
    return None


def _within_hours(a: dict, b: dict, hours: int) -> bool:
    """Check if two listings are within N hours of each other."""
    ta = _parse_ts(a)
    tb = _parse_ts(b)
    if ta is None or tb is None:
        return True  # assume same window if can't parse
    return abs((ta - tb).total_seconds()) < hours * 3600


def dedupe_listings(listings: list[dict], window_hours: int = 72) -> list[dict]:
    """
    Two-pass deduplication.

    Pass 1: Exact fingerprint match within time window → keep latest.
    Pass 2: Fuzzy — same seller + same ref + price within 2% + within window → keep latest.

    Returns deduped list (freshest kept). Logs statistics.
    """
    if not listings:
        return []

    before = len(listings)

    # Sort by timestamp descending (keep freshest)
    listings_sorted = sorted(
        listings,
        key=lambda x: x.get('ts', ''),
        reverse=True,
    )

    # Pass 1: exact fingerprint
    seen_fp: dict[ListingFingerprint, dict] = {}
    pass1 = []
    exact_dupes = 0

    for l in listings_sorted:
        fp = fingerprint(l)
        if fp in seen_fp:
            existing = seen_fp[fp]
            if _within_hours(l, existing, window_hours):
                exact_dupes += 1
                continue  # skip older duplicate
        seen_fp[fp] = l
        pass1.append(l)

    # Pass 2: fuzzy (same seller, same ref, price ±2%)
    result = []
    seen_fuzzy: list[dict] = []
    fuzzy_dupes = 0

    for l in pass1:
        is_dup = False
        seller_norm = _normalize_seller(l.get('seller', ''))
        for prev in seen_fuzzy:
            if (prev['ref'] == l.get('ref', '')
                    and _normalize_seller(prev.get('seller', '')) == seller_norm
                    and prev.get('price_usd', 0) > 0
                    and abs(prev['price_usd'] - l.get('price_usd', 0)) / prev['price_usd'] < 0.02
                    and _within_hours(l, prev, window_hours)):
                is_dup = True
                fuzzy_dupes += 1
                break
        if not is_dup:
            seen_fuzzy.append(l)
            result.append(l)

    after = len(result)
    removed = before - after
    pct = (removed / before * 100) if before > 0 else 0

    logger.info(
        f"Dedup: {before} → {after} ({removed} removed, {pct:.1f}%) "
        f"[exact={exact_dupes}, fuzzy={fuzzy_dupes}]"
    )

    return result


def dedup_stats(listings: list[dict], window_hours: int = 72) -> dict:
    """Return dedup statistics without modifying the list."""
    before = len(listings)
    deduped = dedupe_listings(listings, window_hours)
    after = len(deduped)
    return {
        'before': before,
        'after': after,
        'removed': before - after,
        'reduction_pct': round((before - after) / before * 100, 1) if before > 0 else 0,
    }


if __name__ == '__main__':
    # Quick test
    sample = [
        {'ref': '126710BLNR', 'price_usd': 14500, 'seller': 'John Watch', 'region': 'US',
         'condition': 'BNIB', 'dial': 'Black', 'ts': '02/20/2026 10:00'},
        {'ref': '126710BLNR', 'price_usd': 14500, 'seller': 'John Watch', 'region': 'US',
         'condition': 'BNIB', 'dial': 'Black', 'ts': '02/20/2026 14:00'},
        {'ref': '126710BLNR', 'price_usd': 14600, 'seller': "John's Watches LLC", 'region': 'US',
         'condition': 'BNIB', 'dial': 'Black', 'ts': '02/20/2026 16:00'},
        {'ref': '126610LN', 'price_usd': 9500, 'seller': 'Jane Dealer', 'region': 'HK',
         'condition': 'BNIB', 'dial': 'Black', 'ts': '02/20/2026 12:00'},
    ]
    result = dedupe_listings(sample)
    print(f"Input: {len(sample)}, Output: {len(result)}")
    for r in result:
        print(f"  {r['ref']} ${r['price_usd']} {r['seller']} {r['ts']}")
