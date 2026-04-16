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
