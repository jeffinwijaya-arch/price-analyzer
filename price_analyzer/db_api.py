#!/usr/bin/env python3
"""
Database API Layer — Replaces botsheet_api.py functionality.
All inventory CRUD goes through here. Every write auto-logs to audit_log.
"""

import json
import re
import os
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

from database import (
    get_db, init_db, dict_from_row, _yn, _parse_cost,
    db_log, watch_to_dict, watch_to_api_dict, DB_PATH,
    now_et, now_et_iso, ET
)

WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwZ6V8EMmo7DrllvXvz5V99utIoFwi9sctGLnGfSXZfv9n7fj7iKvO4_VIyGIQTzFsQ/exec"

# Bot Sheet column mapping (for export)
FIELD_TO_COL = {
    'description': 'A', 'paid_supplier': 'B', 'bought_from': 'C', 'sold_to': 'D',
    'cost_price': 'E', 'sale_price': 'F', 'sold_price': 'G', 'at_store': 'H',
    'posted': 'I', 'arrived': 'J', 'sold': 'K', 'shipped': 'L',
    'paid_buyer': 'M', 'sale_date': 'N', 'card_date': 'O',
    'serial': 'P', 'wt': 'Q', 'buy_date': 'R', 'profit': 'S'
}

# Aliases from old field names to new DB field names
FIELD_ALIASES = {
    'paid': 'paid_supplier',
    'cost': 'cost_price',
    'date_on_card': 'card_date',
    'card_date': 'card_date',
    'bought_date': 'buy_date',
    'buy_date': 'buy_date',
    'paid_after_sold': 'paid_buyer',
    'watch_paid': 'paid_buyer',
    'wt': 'wt',
    'description': 'description',
    'bought_from': 'bought_from',
    'sold_to': 'sold_to',
    'cost_price': 'cost_price',
    'sale_price': 'sale_price',
    'sold_price': 'sold_price',
    'at_store': 'at_store',
    'posted': 'posted',
    'arrived': 'arrived',
    'sold': 'sold',
    'shipped': 'shipped',
    'sale_date': 'sale_date',
    'serial': 'serial',
    'profit': 'profit',
}

# Fields that are boolean (stored as 0/1)
BOOL_FIELDS = {'paid_supplier', 'at_store', 'posted', 'arrived', 'sold', 'shipped', 'paid_buyer', 'wt', 'deleted'}

# Fields that are numeric (stored as REAL)
NUMERIC_FIELDS = {'cost_price', 'sale_price', 'sold_price', 'profit'}


def _normalize_field(key):
    """Normalize field name to DB column name."""
    return FIELD_ALIASES.get(key, key)


def _normalize_value(field, value):
    """Normalize a value for the given field."""
    if field in BOOL_FIELDS:
        return _yn(value)
    if field in NUMERIC_FIELDS:
        return _parse_cost(value)
    if value is None:
        return ''
    return str(value)


# ═══════════════════════════════════════
# CRUD Operations
# ═══════════════════════════════════════

def add_watch(desc, cost, bought_from='', buy_date=None, source='api', **kwargs):
    """Add a new watch. Returns {'ok': True, 'id': <watch_id>}."""
    conn = get_db()
    try:
        fields = {
            'description': desc,
            'cost_price': _parse_cost(cost),
            'bought_from': bought_from,
            'buy_date': buy_date or datetime.now(ET).strftime('%Y-%m-%d'),
            'paid_supplier': 0,
            'at_store': 0,
            'posted': 0,
            'arrived': 0,
            'sold': 0,
            'shipped': 0,
            'paid_buyer': 0,
            'wt': 0,
            'assigned_shipper': 2,  # Eddy by default, always
        }
        # Apply extras
        for k, v in kwargs.items():
            nk = _normalize_field(k)
            fields[nk] = _normalize_value(nk, v)

        cols = ', '.join(fields.keys())
        placeholders = ', '.join(['?'] * len(fields))
        cur = conn.execute(
            f"INSERT INTO watches ({cols}) VALUES ({placeholders})",
            list(fields.values())
        )
        watch_id = cur.lastrowid

        # Audit log
        db_log(conn, watch_id, 'created', '', desc, source)
        conn.commit()
        return {'ok': True, 'id': watch_id, 'description': desc}
    except Exception as e:
        conn.rollback()
        return {'error': str(e)}
    finally:
        conn.close()


def update_watch(watch_id, source='api', **fields):
    """Update specific fields on a watch. Auto-logs changes."""
    conn = get_db()
    try:
        # Get current values
        row = conn.execute("SELECT * FROM watches WHERE id = ? AND deleted = 0", (watch_id,)).fetchone()
        if not row:
            return {'error': f'Watch {watch_id} not found'}

        current = dict(row)
        updates = {}
        for k, v in fields.items():
            nk = _normalize_field(k)
            # Skip unknown fields
            if nk not in current:
                continue
            nv = _normalize_value(nk, v)
            old_val = current[nk]
            updates[nk] = nv
            db_log(conn, watch_id, nk, old_val, nv, source)

        if not updates:
            return {'ok': True, 'id': watch_id, 'changes': 0}

        updates['updated_at'] = now_et_iso()
        set_clause = ', '.join(f"{k} = ?" for k in updates.keys())
        conn.execute(
            f"UPDATE watches SET {set_clause} WHERE id = ?",
            list(updates.values()) + [watch_id]
        )
        conn.commit()
        return {'ok': True, 'id': watch_id, 'changes': len(updates) - 1}  # -1 for updated_at
    except Exception as e:
        conn.rollback()
        return {'error': str(e)}
    finally:
        conn.close()


def mark_sold(watch_id, sold_to, sold_price, sale_date=None, source='api'):
    """Mark a watch as sold with all implied updates.
    NOTE: Does NOT auto-set arrived — arrival must be confirmed separately with photo/serial.
    Auto-deactivates any active posting in my_postings_log.json for this watch.
    """
    # Auto-assign Eddy (default shipper) unless already assigned
    conn = get_db()
    w = conn.execute('SELECT assigned_shipper FROM watches WHERE id=?', (watch_id,)).fetchone()
    extra = {}
    if w and not w[0]:
        extra['assigned_shipper'] = 2  # Eddy is default shipper for everything
    conn.close()
    
    result = update_watch(watch_id, source=source,
        sold_to=sold_to,
        sold_price=str(sold_price),
        sale_price=str(sold_price),
        sold='Yes',
        sale_date=sale_date or datetime.now(ET).strftime('%Y-%m-%d'),
        **extra,
    )
    # Notify shipper (Eddy) that watch is sold
    try:
        # Import from dashboard — has _notify_shipper with WhatsApp integration
        import importlib, sys
        # Get watch details for notification
        conn = get_db()
        w = conn.execute('SELECT description, serial, ship_from, assigned_shipper FROM watches WHERE id=?', (watch_id,)).fetchone()
        if w and w[3] != 'Luigi' and w[2] not in ('external', 'foreign'):
            import subprocess
            EDDY_WA = '17324899352@s.whatsapp.net'
            desc = w[0] or ''
            serial = w[1] or ''
            # Extract ref and date
            import re
            ref_m = re.search(r'\b(\d{5,6}[A-Z]*)\b', desc)
            ref = ref_m.group(1) if ref_m else desc[:20]
            date_m = re.search(r'(\d{1,2}/\d{4})', desc)
            date_str = date_m.group(1) if date_m else ''
            ser = f"(S/N: {serial})" if serial else ''
            msg = f"Jam TERJUAL: {ref} {date_str} {ser} ke {sold_to}. Siap-siap untuk shipping ya!"
            subprocess.run(['wacli', 'send', 'text', '--to', EDDY_WA, '--message', msg], capture_output=True, timeout=15)
        conn.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to notify shipper on sold: {e}")

    # Auto-deactivate posting in DB + delete from WhatsApp
    try:
        import json, os
        conn = get_db()
        posting = conn.execute('SELECT id, message_id FROM postings WHERE watch_id=? AND status="active"', (watch_id,)).fetchone()
        if posting:
            msg_id = posting[1]
            conn.execute('UPDATE postings SET status="sold" WHERE id=?', (posting[0],))
            conn.commit()
            # Delete from WhatsApp My Postings group
            if msg_id:
                try:
                    from whatsapp_postings import delete_message
                    delete_message(msg_id)
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(f"Failed to delete WA posting: {e}")
        conn.close()
    except Exception:
        pass
    # Legacy JSON log deactivation
    try:
        import json, os
        log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'my_postings_log.json')
        if os.path.exists(log_path):
            with open(log_path) as f:
                postings = json.load(f)
            changed = False
            for p in postings:
                wid = p.get('watch_id') or p.get('row')
                if wid and int(wid) == int(watch_id) and p.get('status', 'active') == 'active':
                    p['status'] = 'sold'
                    changed = True
            if changed:
                with open(log_path, 'w') as f:
                    json.dump(postings, f, indent=2)
    except Exception:
        pass
    return result


def sync_posting_price(watch_id):
    """Sync sale_price from active posting in my_postings_log.json.
    Called after any sale_price change to ensure DB and posting stay in sync."""
    try:
        import json, os
        log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'my_postings_log.json')
        if not os.path.exists(log_path):
            return
        with open(log_path) as f:
            postings = json.load(f)
        for p in postings:
            wid = p.get('watch_id') or p.get('row')
            if wid and int(wid) == int(watch_id) and p.get('status', 'active') == 'active':
                posting_price = p.get('price')
                if posting_price:
                    from database import get_db
                    db = get_db()
                    current = db.execute('SELECT sale_price FROM watches WHERE id=?', (int(watch_id),)).fetchone()
                    if current and current['sale_price'] and abs(float(current['sale_price']) - float(posting_price)) > 50:
                        db.execute('UPDATE watches SET sale_price=?, updated_at=datetime("now") WHERE id=?', 
                                   (float(posting_price), int(watch_id)))
                        db.execute('INSERT INTO audit_log (watch_id,field,old_value,new_value,source) VALUES (?,?,?,?,?)',
                                   (int(watch_id), 'sale_price', str(current['sale_price']), str(posting_price), 'posting_sync'))
                        db.commit()
                break
    except Exception:
        pass

def mark_arrived(watch_id, source='api'):
    """Mark a watch as arrived."""
    return update_watch(watch_id, source=source, arrived='Yes', at_store='No')


def mark_shipped(watch_id, tracking='', source='api'):
    """Mark a watch as shipped. Optionally save tracking."""
    result = update_watch(watch_id, source=source, shipped='Yes')
    if result.get('ok') and tracking:
        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO shipping (watch_id, tracking, carrier, ship_type, created_at) VALUES (?,?,?,?,?)",
                (watch_id, tracking, 'FedEx', 'fedex', now_et_iso())
            )
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()
    return result


def mark_posted(watch_id, sale_price=None, source='api'):
    """Mark a watch as posted for sale."""
    kwargs = {'posted': 'Yes'}
    if sale_price:
        kwargs['sale_price'] = str(sale_price)
    return update_watch(watch_id, source=source, **kwargs)


def mark_paid(watch_id, source='api'):
    """Mark payment received from buyer + sync linked invoice."""
    result = update_watch(watch_id, source=source, paid_buyer='Yes')
    # Sync: mark linked invoice as paid too
    try:
        conn = get_db()
        inv = conn.execute("SELECT id, status FROM internal_invoices WHERE watch_id = ? AND status != 'void' ORDER BY id DESC LIMIT 1", (watch_id,)).fetchone()
        if inv and inv['status'] != 'paid':
            import datetime
            now = datetime.now_et_iso()
            conn.execute("UPDATE internal_invoices SET status = 'paid', payment_date = ?, updated_at = ? WHERE id = ?", (now, now, inv['id']))
            conn.commit()
        conn.close()
    except Exception:
        pass
    return result


def mark_seller_paid(watch_id, source='api'):
    """Mark that supplier has been paid."""
    return update_watch(watch_id, source=source, paid_supplier='Yes')


def delete_watch(watch_id, source='api'):
    """Soft delete — marks deleted, keeps in audit."""
    return update_watch(watch_id, source=source, deleted=1)


# ═══════════════════════════════════════
# Query Operations
# ═══════════════════════════════════════

def get_watch(watch_id):
    """Get a single watch by ID."""
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM watches WHERE id = ?", (watch_id,)).fetchone()
        return watch_to_dict(row)
    finally:
        conn.close()


def get_watch_api(watch_id):
    """Get a single watch by ID in API format (bools not Yes/No)."""
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM watches WHERE id = ?", (watch_id,)).fetchone()
        return watch_to_api_dict(row)
    finally:
        conn.close()


def get_all_watches(filters=None, include_deleted=False):
    """Get all watches, with optional filters.
    
    filters dict can contain:
        sold: bool, arrived: bool, posted: bool, shipped: bool,
        ref: str (searches description), bought_from: str, sold_to: str
    """
    conn = get_db()
    try:
        where = []
        params = []
        if not include_deleted:
            where.append("deleted = 0")

        if filters:
            for key in ['sold', 'arrived', 'posted', 'shipped', 'at_store', 'paid_buyer', 'paid_supplier', 'wt']:
                if key in filters:
                    where.append(f"{key} = ?")
                    params.append(_yn(filters[key]))
            if 'ref' in filters:
                where.append("description LIKE ?")
                params.append(f"%{filters['ref']}%")
            if 'bought_from' in filters:
                where.append("bought_from LIKE ?")
                params.append(f"%{filters['bought_from']}%")
            if 'sold_to' in filters:
                where.append("sold_to LIKE ?")
                params.append(f"%{filters['sold_to']}%")

        where_clause = " AND ".join(where) if where else "1=1"
        rows = conn.execute(f"SELECT * FROM watches WHERE {where_clause} ORDER BY id", params).fetchall()
        return [watch_to_dict(r) for r in rows]
    finally:
        conn.close()


def get_all_watches_api(filters=None, include_deleted=False):
    """Same as get_all_watches but returns API format (bools)."""
    conn = get_db()
    try:
        where = []
        params = []
        if not include_deleted:
            where.append("deleted = 0")

        if filters:
            for key in ['sold', 'arrived', 'posted', 'shipped', 'at_store', 'paid_buyer', 'paid_supplier', 'wt']:
                if key in filters:
                    where.append(f"{key} = ?")
                    params.append(_yn(filters[key]))
            if 'ref' in filters:
                where.append("description LIKE ?")
                params.append(f"%{filters['ref']}%")
            if 'bought_from' in filters:
                where.append("bought_from LIKE ?")
                params.append(f"%{filters['bought_from']}%")
            if 'sold_to' in filters:
                where.append("sold_to LIKE ?")
                params.append(f"%{filters['sold_to']}%")

        where_clause = " AND ".join(where) if where else "1=1"
        rows = conn.execute(f"SELECT * FROM watches WHERE {where_clause} ORDER BY id", params).fetchall()
        return [watch_to_api_dict(r) for r in rows]
    finally:
        conn.close()


def find_watch(ref, month_year=None):
    """Find watches matching a ref (in description), optionally filtered by month/year."""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM watches WHERE description LIKE ? AND deleted = 0 ORDER BY id",
            (f"%{ref}%",)
        ).fetchall()
        results = []
        for r in rows:
            d = watch_to_dict(r)
            if month_year and month_year not in d.get('description', ''):
                continue
            results.append(d)
        return {'ok': True, 'matches': results, 'count': len(results)}
    finally:
        conn.close()


def get_audit_log(watch_id=None, limit=50):
    """Get audit log, optionally filtered by watch_id."""
    conn = get_db()
    try:
        if watch_id:
            rows = conn.execute(
                "SELECT * FROM audit_log WHERE watch_id = ? ORDER BY timestamp DESC LIMIT ?",
                (watch_id, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict_from_row(r) for r in rows]
    finally:
        conn.close()


# ═══════════════════════════════════════
# Postings Operations
# ═══════════════════════════════════════

def add_posting(watch_id=None, ref='', description='', caption='', price=None,
                message_id=None, photo='', status='active', posted_date=None,
                posted_at=None, dial='', condition='', year='', **extra):
    """Add a posting entry."""
    conn = get_db()
    try:
        extra_json = json.dumps(extra) if extra else None
        conn.execute(
            """INSERT INTO postings (watch_id, ref, description, caption, price, message_id, 
               photo, status, posted_date, posted_at, dial, condition, year, extra_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (watch_id, ref, description, caption, _parse_cost(price), str(message_id) if message_id else None,
             photo, status, posted_date, posted_at, dial, condition, year, extra_json)
        )
        conn.commit()
        return {'ok': True}
    except Exception as e:
        conn.rollback()
        return {'error': str(e)}
    finally:
        conn.close()


def get_postings(status=None):
    """Get postings, optionally filtered by status."""
    conn = get_db()
    try:
        if status:
            rows = conn.execute("SELECT * FROM postings WHERE status = ? ORDER BY id", (status,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM postings ORDER BY id").fetchall()
        return [dict_from_row(r) for r in rows]
    finally:
        conn.close()


def update_posting_status(posting_id, status):
    """Update posting status (active/sold/deleted)."""
    conn = get_db()
    try:
        conn.execute("UPDATE postings SET status = ? WHERE id = ?", (status, posting_id))
        conn.commit()
        return {'ok': True}
    except Exception as e:
        conn.rollback()
        return {'error': str(e)}
    finally:
        conn.close()


# ═══════════════════════════════════════
# Google Sheet Export (one-way sync)
# ═══════════════════════════════════════

def export_to_sheets():
    """Export all watches to Google Sheet (Sheet2) — full overwrite, no row-shift possible.
    Also exports postings to a separate tab."""
    try:
        watches = get_all_watches(include_deleted=False)
        # Build rows for Sheet2
        sheet_rows = []
        for w in watches:
            row = {}
            for field, col in FIELD_TO_COL.items():
                val = w.get(field, '')
                # Convert booleans back to Yes/No for sheet
                if field in BOOL_FIELDS:
                    val = 'Yes' if _yn(val) else 'No'
                row[col] = str(val) if val is not None else ''
            sheet_rows.append(row)

        # Call Apps Script to overwrite Sheet2
        payload = json.dumps({
            'action': 'bulkWrite',
            'sheet': 'Sheet2',
            'rows': sheet_rows
        })
        req = urllib.request.Request(
            WEBAPP_URL,
            data=payload.encode('utf-8'),
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode())
            return {'ok': True, 'rows_exported': len(sheet_rows), 'sheet_result': result}
        except Exception as e:
            return {'ok': False, 'error': f'Sheet export failed: {e}', 'rows_prepared': len(sheet_rows)}

    except Exception as e:
        return {'error': str(e)}


# ═══════════════════════════════════════
# Backup
# ═══════════════════════════════════════

def backup_db():
    """Create a backup of the database file."""
    import shutil
    backup_dir = Path(__file__).parent.parent / 'db_backups'
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(ET).strftime('%Y%m%d_%H%M%S')
    backup_path = backup_dir / f'watches_{timestamp}.db'
    shutil.copy2(str(DB_PATH), str(backup_path))

    # Cleanup: keep last 30 days
    backups = sorted(backup_dir.glob('watches_*.db'))
    if len(backups) > 30:
        for old in backups[:-30]:
            old.unlink()

    return {'ok': True, 'backup_path': str(backup_path)}


# ═══════════════════════════════════════
# CLI
# ═══════════════════════════════════════

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 db_api.py [test|dump|find <ref>|backup|export]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == 'test':
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM watches WHERE deleted = 0").fetchone()[0]
        conn.close()
        print(f"Database has {count} active watches")

    elif cmd == 'dump':
        watches = get_all_watches()
        # Output in format compatible with sheet_updater dump
        output = []
        for w in watches:
            output.append({
                'row': w['id'],  # Use ID as row for compatibility
                'description': w.get('description', ''),
                'paid': w.get('paid_supplier', 'No'),
                'bought_from': w.get('bought_from', ''),
                'sold_to': w.get('sold_to', ''),
                'cost_price': str(w.get('cost_price', '')) if w.get('cost_price') else '',
                'sale_price': str(w.get('sale_price', '')) if w.get('sale_price') else '',
                'sold_price': str(w.get('sold_price', '')) if w.get('sold_price') else '',
                'at_store': w.get('at_store', 'No'),
                'posted': w.get('posted', 'No'),
                'arrived': w.get('arrived', 'No'),
                'sold': w.get('sold', 'No'),
                'shipped': w.get('shipped', 'No'),
                'paid_after_sold': w.get('paid_buyer', 'No'),
                'sale_date': w.get('sale_date', ''),
                'date_on_card': w.get('card_date', ''),
                'serial': w.get('serial', ''),
                'wt': w.get('wt', 'No'),
                'buy_date': w.get('buy_date', ''),
                'bought_date': w.get('buy_date', ''),
                'profit': str(w.get('profit', '')) if w.get('profit') else '',
            })
        print(json.dumps(output, indent=2))

    elif cmd == 'find':
        ref = sys.argv[2] if len(sys.argv) > 2 else ''
        result = find_watch(ref)
        print(f"Found {result['count']} matches")
        for m in result['matches']:
            print(f"  ID {m['id']}: {m['description']} | Sold: {m['sold']}")

    elif cmd == 'backup':
        result = backup_db()
        print(json.dumps(result, indent=2))

    elif cmd == 'export':
        result = export_to_sheets()
        print(json.dumps(result, indent=2))

    else:
        print(f"Unknown command: {cmd}")
