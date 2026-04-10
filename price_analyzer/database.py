#!/usr/bin/env python3
"""
SQLite Database Schema & Core Operations for MK Opulence Watch Inventory.
Replaces Google Sheets as the primary data store.

Tables:
- watches: All inventory with unique auto-increment ID
- postings: My Postings data
- audit_log: Every change tracked
- shipping: Labels and tracking
- invoices: Zoho invoice links
"""

import sqlite3
import os
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

def now_et():
    """Return current time in ET as ISO string."""
    return datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S')

def now_et_iso():
    """Return current time in ET as ISO format string."""
    return datetime.now(ET).isoformat()

DB_PATH = Path(__file__).parent / 'watches.db'

def get_db(path=None):
    """Get a database connection with WAL mode and foreign keys enabled."""
    db_path = path or DB_PATH
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn=None):
    """Create all tables if they don't exist."""
    close = False
    if conn is None:
        conn = get_db()
        close = True

    conn.executescript("""
    CREATE TABLE IF NOT EXISTS watches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT NOT NULL DEFAULT '',
        paid_supplier INTEGER NOT NULL DEFAULT 0,
        bought_from TEXT NOT NULL DEFAULT '',
        sold_to TEXT NOT NULL DEFAULT '',
        cost_price REAL,
        sale_price REAL,
        sold_price REAL,
        at_store INTEGER NOT NULL DEFAULT 0,
        posted INTEGER NOT NULL DEFAULT 0,
        arrived INTEGER NOT NULL DEFAULT 0,
        sold INTEGER NOT NULL DEFAULT 0,
        shipped INTEGER NOT NULL DEFAULT 0,
        paid_buyer INTEGER NOT NULL DEFAULT 0,
        sale_date TEXT,
        card_date TEXT,
        serial TEXT NOT NULL DEFAULT '',
        wt INTEGER NOT NULL DEFAULT 0,
        buy_date TEXT,
        profit REAL,
        deleted INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS postings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        watch_id INTEGER,
        ref TEXT NOT NULL DEFAULT '',
        description TEXT NOT NULL DEFAULT '',
        caption TEXT NOT NULL DEFAULT '',
        price REAL,
        message_id TEXT,
        photo TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        posted_date TEXT,
        posted_at TEXT,
        dial TEXT,
        condition TEXT,
        year TEXT,
        extra_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (watch_id) REFERENCES watches(id)
    );

    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        watch_id INTEGER,
        field TEXT NOT NULL,
        old_value TEXT,
        new_value TEXT,
        source TEXT NOT NULL DEFAULT 'api',
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (watch_id) REFERENCES watches(id)
    );

    CREATE TABLE IF NOT EXISTS shipping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        watch_id INTEGER NOT NULL,
        tracking TEXT NOT NULL DEFAULT '',
        carrier TEXT NOT NULL DEFAULT '',
        ship_type TEXT NOT NULL DEFAULT '',
        address TEXT NOT NULL DEFAULT '',
        pdf_path TEXT NOT NULL DEFAULT '',
        extra_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (watch_id) REFERENCES watches(id)
    );

    CREATE TABLE IF NOT EXISTS invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        watch_id INTEGER,
        invoice_id TEXT NOT NULL DEFAULT '',
        invoice_number TEXT NOT NULL DEFAULT '',
        amount REAL,
        customer TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'created',
        extra_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (watch_id) REFERENCES watches(id)
    );

    CREATE INDEX IF NOT EXISTS idx_watches_deleted ON watches(deleted);
    CREATE INDEX IF NOT EXISTS idx_watches_sold ON watches(sold);
    CREATE INDEX IF NOT EXISTS idx_watches_description ON watches(description);
    CREATE INDEX IF NOT EXISTS idx_audit_watch ON audit_log(watch_id);
    CREATE INDEX IF NOT EXISTS idx_postings_watch ON postings(watch_id);
    CREATE INDEX IF NOT EXISTS idx_postings_status ON postings(status);
    CREATE INDEX IF NOT EXISTS idx_shipping_watch ON shipping(watch_id);
    CREATE INDEX IF NOT EXISTS idx_invoices_watch ON invoices(watch_id);

    -- Internal Invoice System (replaces Zoho)
    CREATE TABLE IF NOT EXISTS internal_invoices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        invoice_number TEXT NOT NULL UNIQUE,
        customer_name TEXT NOT NULL DEFAULT '',
        customer_address TEXT NOT NULL DEFAULT '',
        customer_email TEXT NOT NULL DEFAULT '',
        customer_phone TEXT NOT NULL DEFAULT '',
        items_json TEXT NOT NULL DEFAULT '[]',
        subtotal REAL NOT NULL DEFAULT 0,
        tax_rate REAL NOT NULL DEFAULT 0,
        tax_amount REAL NOT NULL DEFAULT 0,
        total REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'draft',
        notes TEXT NOT NULL DEFAULT '',
        watch_id INTEGER,
        payment_date TEXT,
        due_date TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (watch_id) REFERENCES watches(id)
    );
    CREATE INDEX IF NOT EXISTS idx_internal_invoices_status ON internal_invoices(status);
    CREATE INDEX IF NOT EXISTS idx_internal_invoices_customer ON internal_invoices(customer_name);
    CREATE INDEX IF NOT EXISTS idx_internal_invoices_number ON internal_invoices(invoice_number);
    """)
    conn.commit()
    if close:
        conn.close()


def dict_from_row(row):
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


def _yn(val):
    """Convert Yes/No/bool/int to integer 0/1."""
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, int):
        return 1 if val else 0
    s = str(val).strip().lower()
    return 1 if s in ('yes', 'y', 'true', '1') else 0


def _parse_cost(val):
    """Parse cost string like '$21,500.00' to float or None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if val else None
    s = str(val).replace('$', '').replace(',', '').strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def db_log(conn, watch_id, field, old_value, new_value, source='api'):
    """Log a single field change to audit_log."""
    old_str = str(old_value) if old_value is not None else ''
    new_str = str(new_value) if new_value is not None else ''
    if old_str == new_str:
        return
    conn.execute(
        "INSERT INTO audit_log (watch_id, field, old_value, new_value, source, timestamp) VALUES (?,?,?,?,?,?)",
        (watch_id, field, old_str, new_str, source, now_et_iso())
    )


def watch_to_dict(row):
    """Convert a watches row to a dict matching the old Bot Sheet format for compatibility."""
    if row is None:
        return None
    d = dict(row)
    # Map boolean ints back to Yes/No for API compatibility
    bool_fields = ['paid_supplier', 'at_store', 'posted', 'arrived', 'sold', 'shipped', 'paid_buyer', 'wt', 'deleted']
    for f in bool_fields:
        if f in d:
            d[f] = 'Yes' if d[f] else 'No'
    # Alias 'row' to 'id' for frontend compatibility
    if 'id' in d and 'row' not in d:
        d['row'] = d['id']
    return d


def watch_to_api_dict(row):
    """Convert a watches row to the format the dashboard API returns (bool values, not Yes/No)."""
    if row is None:
        return None
    d = dict(row)
    bool_fields = ['paid_supplier', 'at_store', 'posted', 'arrived', 'sold', 'shipped', 'paid_buyer', 'wt', 'deleted']
    for f in bool_fields:
        if f in d:
            d[f] = bool(d[f])
    # Alias 'row' to 'id' for frontend compatibility
    if 'id' in d and 'row' not in d:
        d['row'] = d['id']
    return d


# Initialize DB on import
if not DB_PATH.exists():
    init_db()
else:
    # Ensure tables exist (safe to call multiple times)
    try:
        conn = get_db()
        init_db(conn)
        conn.close()
    except Exception:
        pass
