#!/usr/bin/env python3
"""
Wholesale Watch Price Analyzer v4 — Rolex-Only, Production Grade
================================================================

TABLE OF CONTENTS (search for ── markers):
  ── Configuration          Config loading (config.json + defaults)
  ── Data Assets            SKU DB, retail prices, ref catalog, seller aliases
  ── Group Name Norm        WhatsApp group name normalization
  ── Nickname Map           Batman/Pepsi/etc → ref mapping
  ── Ref Canonicalization   126500→126500LN, 126710+context→BLNR/BLRO
  ── Ref Validation         Validate extracted refs against known Rolex refs
  ── Currency               FX rates, group→currency mapping, sanity checks
  ── Number Parsing         safe_num, ref-as-price detection
  ── Price Extraction       Multi-pattern price parser with range/shorthand support
  ── Dial Extraction        Fixed dials, pattern matching, SKU validation
  ── Bracelet Detection     Pattern + default bracelet per ref
  ── Condition + Year       BNIB/Pre-owned/Like New logic, card date parsing
  ── Completeness           Full Set/W+C/Watch Only extraction
  ── Price Sanity           Chrono24/retail range validation
  ── Main Parser            WhatsApp message parsing, multi-ref splitting
  ── Build Index            Aggregate listings into per-ref index
  ── Excel Output           Multi-sheet Excel generation (BNIB, pre-owned, arb, etc)
  ── Outlier Filter         IQR-based outlier removal
  ── CLI Commands           parse, query, price, margin, deals, watch, refresh, etc
"""

import re, json, sys, os, hashlib
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent
WORKSPACE = BASE_DIR.parent

# ── Configuration ────────────────────────────────────────────
def _load_config():
    """Load config.json, falling back to defaults if missing."""
    cfg_path = BASE_DIR / 'config.json'
    defaults = {
        'exchange_rates': {'USD':1.0,'HKD':0.1282,'AED':0.272,'CAD':0.72,'EUR':1.08,'GBP':1.27,'SGD':0.75,'USDT':1.0},
        'import_fees': {'HK':{'tiers':[{'max_usd':10000,'fee':250},{'max_usd':30000,'fee':350},{'max_usd':75000,'fee':450},{'max_usd':150000,'fee':550},{'max_usd':999999999,'fee':700}]},'EU':{'wc_adder':300},'US':{'wc_adder':250}},
        'bnib_age_cap_months': 18,
        'stale_listing_days': 5,
        'outlier_iqr_multiplier': 1.5,
        'default_region': 'US',
        'recent_days_default': 5,
        'deal_discount_min_pct': 7,
        'deal_discount_max_pct': 40,
        'arbitrage_max_pct': 15,
        'spread_max_pct': 15,
    }
    if cfg_path.exists():
        try:
            with open(cfg_path, encoding='utf-8') as f:
                user_cfg = json.load(f)
            # Merge user config over defaults
            for k, v in user_cfg.items():
                if isinstance(v, dict) and isinstance(defaults.get(k), dict):
                    defaults[k].update(v)
                else:
                    defaults[k] = v
        except Exception as e:
            print(f"  ⚠️ Failed to load config.json: {e} — using defaults", file=sys.stderr)
    return defaults

CONFIG = _load_config()

def _load_json(path):
    if not path.exists(): return {}
    with open(path, encoding='utf-8') as f: return json.load(f)
