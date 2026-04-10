"""
Bot Sheet API — Bridge between dashboard and Google Apps Script web app.
Handles inventory CRUD with business logic (lifecycle defaults, Telegram group posts).
"""

import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime

WEBAPP_URL = "https://script.google.com/macros/s/AKfycbwZ6V8EMmo7DrllvXvz5V99utIoFwi9sctGLnGfSXZfv9n7fj7iKvO4_VIyGIQTzFsQ/exec"

# Column mapping for human-readable field names
FIELD_MAP = {
    'description': 'A', 'paid': 'B', 'bought_from': 'C', 'sold_to': 'D',
    'cost_price': 'E', 'cost': 'E', 'sale_price': 'F', 'sold_price': 'G', 'at_store': 'H',
    'posted': 'I', 'arrived': 'J', 'sold': 'K', 'shipped': 'L',
    'paid_after_sold': 'M', 'sale_date': 'N', 'date_on_card': 'O',
    'serial': 'P', 'wt': 'Q', 'bought_date': 'R', 'buy_date': 'R', 'profit': 'S'
}

REVERSE_MAP = {v: k for k, v in FIELD_MAP.items()}
# Fix aliases: prefer canonical names for columns with multiple aliases
REVERSE_MAP['E'] = 'cost_price'
REVERSE_MAP['R'] = 'bought_date'

# Lifecycle defaults for new entries
LIFECYCLE_DEFAULTS = {
    'B': 'No',   # paid
    'H': 'No',   # at_store
    'I': 'No',   # posted
    'J': 'No',   # arrived
    'K': 'No',   # sold
    'L': 'No',   # shipped
    'M': 'No',   # paid_after_sold
    'Q': 'No',   # wt
}


def _call_webapp(action, **params):
    """Call the Apps Script web app."""
    params['action'] = action
    params['noSort'] = 'true'  # NEVER auto-sort — it scrambles row references
    # URL-encode fields if present
    if 'fields' in params and isinstance(params['fields'], dict):
        params['fields'] = json.dumps(params['fields'])
    
    qs = urllib.parse.urlencode(params)
    url = f"{WEBAPP_URL}?{qs}"
    
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {'error': str(e)}


def dump_all():
    """Get all inventory rows."""
    result = _call_webapp('dump')
    if not result.get('ok'):
        return result
    
    rows = []
    for r in result.get('rows', []):
        row = {'row': r['row']}
        for col_letter, field_name in REVERSE_MAP.items():
            val = r.get(col_letter, '')
            # Clean up Google Sheets date objects (come as strings)
            if isinstance(val, str):
                row[field_name] = val
            else:
                row[field_name] = str(val) if val else ''
        rows.append(row)
    
    return {'ok': True, 'rows': rows, 'count': len(rows)}


def add_watch(description, cost_price, bought_from, bought_date=None, **extras):
    """Add a new watch purchase. Auto-fills lifecycle defaults."""
    fields = dict(LIFECYCLE_DEFAULTS)  # Start with all defaults
    fields['A'] = description
    fields['E'] = str(cost_price)
    fields['C'] = bought_from
    fields['R'] = bought_date or datetime.now().strftime('%Y-%m-%d')
    
    # Apply any extras (using human-readable names)
    for key, val in extras.items():
        col = FIELD_MAP.get(key, key)  # Accept both 'sold_to' and 'D'
        if len(col) == 1 and col.isalpha():
            # Normalize booleans to Yes/No for Bot Sheet
            if isinstance(val, bool):
                val = 'Yes' if val else 'No'
            fields[col] = str(val)
    
    return _call_webapp('add', fields=fields)


def _verify_row(row, expected_desc=None):
    """Verify a row number still points to the expected watch.
    If expected_desc is given and doesn't match, re-find the correct row.
    Returns (actual_row, actual_desc) or (None, error_msg) on failure."""
    if not expected_desc:
        return row, None
    
    # Fetch current data at that row
    try:
        dump = dump_all()
        if not dump.get('ok'):
            return row, None  # Can't verify, proceed with original row
        
        rows_by_row = {r['row']: r for r in dump.get('rows', [])}
        current = rows_by_row.get(row)
        
        if current and expected_desc.strip().lower() in current.get('description', '').strip().lower():
            return row, current.get('description', '')
        
        # Row shifted — find by description
        for r in dump.get('rows', []):
            if expected_desc.strip().lower() in r.get('description', '').strip().lower():
                print(f"⚠️ Row shift detected: expected row {row} for '{expected_desc}', found at row {r['row']}")
                return r['row'], r.get('description', '')
        
        return None, f"Watch '{expected_desc}' not found in sheet"
    except Exception as e:
        print(f"Row verification failed: {e}")
        return row, None  # Proceed with original on error


def update_watch(row, expected_desc=None, **updates):
    """Update specific fields on a watch row.
    ALWAYS verifies row matches to prevent row-shift errors.
    If expected_desc not provided, auto-fetches current desc and warns."""
    if not expected_desc:
        # Auto-fetch for safety, but warn
        data = dump_all()
        rows = data.get("rows", []) if isinstance(data, dict) else data
        for r in rows:
            if r.get('row') == row:
                expected_desc = r.get('description', '')
                print(f"⚠️ NO DESC VERIFICATION: Writing to row {row} ({expected_desc}). Pass expected_desc to prevent row-shift errors!")
                break
    if expected_desc:
        actual_row, _ = _verify_row(row, expected_desc)
        if actual_row is None:
            return {'error': f'Row verification failed: watch not found for "{expected_desc}"'}
        if actual_row != row:
            print(f"🔄 ROW SHIFTED: '{expected_desc}' moved from row {row} → {actual_row}. Using correct row.")
        row = actual_row
    
    fields = {}
    for key, val in updates.items():
        col = FIELD_MAP.get(key, key)
        if len(col) == 1 and col.isalpha():
            if isinstance(val, bool):
                val = 'Yes' if val else 'No'
            fields[col] = str(val)
    
    if not fields:
        return {'error': 'No fields to update'}
    
    return _call_webapp('update', row=str(row), fields=fields)


def mark_sold(row, sold_to, sold_price, sale_date=None, expected_desc=None):
    """Mark a watch as sold with all implied updates.
    expected_desc is MANDATORY to prevent row-shift errors."""
    if not expected_desc:
        # Auto-fetch description for safety
        data = dump_all()
        rows = data.get("rows", []) if isinstance(data, dict) else data
        for r in rows:
            if r.get('row') == row:
                expected_desc = r.get('description', '')
                break
        if not expected_desc:
            return {'error': f'Cannot mark_sold without expected_desc and row {row} not found'}
    return update_watch(row, expected_desc=expected_desc,
        sold_to=sold_to,
        sold_price=str(sold_price),
        sale_price=str(sold_price),  # F = G if not set
        sold='Yes',
        posted='Yes',    # Must have been posted to sell
        arrived='Yes',   # Must have arrived to sell
        at_store='No',   # No longer at store
        sale_date=sale_date or datetime.now().strftime('%Y-%m-%d'),
    )


def mark_arrived(row, expected_desc=None):
    """Mark a watch as arrived."""
    if not expected_desc:
        data = dump_all()
        rows = data.get("rows", []) if isinstance(data, dict) else data
        for r in rows:
            if r.get('row') == row:
                expected_desc = r.get('description', '')
                break
    return update_watch(row, expected_desc=expected_desc, arrived='Yes', at_store='No')


def mark_shipped(row, expected_desc=None):
    """Mark a watch as shipped."""
    if not expected_desc:
        data = dump_all()
        rows = data.get("rows", []) if isinstance(data, dict) else data
        for r in rows:
            if r.get('row') == row:
                expected_desc = r.get('description', '')
                break
    return update_watch(row, expected_desc=expected_desc, shipped='Yes')


def mark_posted(row, sale_price=None, expected_desc=None):
    """Mark a watch as posted for sale."""
    if not expected_desc:
        data = dump_all()
        rows = data.get("rows", []) if isinstance(data, dict) else data
        for r in rows:
            if r.get('row') == row:
                expected_desc = r.get('description', '')
                break
    updates = {'posted': 'Yes'}
    if sale_price:
        updates['sale_price'] = str(sale_price)
    return update_watch(row, expected_desc=expected_desc, **updates)


def mark_paid(row, expected_desc=None):
    """Mark a watch as paid (payment received after sale)."""
    if not expected_desc:
        data = dump_all()
        rows = data.get("rows", []) if isinstance(data, dict) else data
        for r in rows:
            if r.get('row') == row:
                expected_desc = r.get('description', '')
                break
    return update_watch(row, expected_desc=expected_desc, paid_after_sold='Yes')


def delete_watch(row):
    """Delete a watch row."""
    return _call_webapp('delete', row=str(row))


def find_watch(ref, month_year=None):
    """Find watches matching a ref number, optionally filtered by month/year."""
    result = dump_all()
    if not result.get('ok'):
        return result
    
    matches = []
    for r in result['rows']:
        desc = r.get('description', '')
        if ref in desc:
            if month_year and month_year not in desc:
                continue
            matches.append(r)
    
    return {'ok': True, 'matches': matches, 'count': len(matches)}


# Quick test
if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        r = dump_all()
        print(f"Dump: {r.get('count', 'ERR')} rows")
        if r.get('ok'):
            print(f"First: {r['rows'][0]['description']}")
    elif len(sys.argv) > 1 and sys.argv[1] == 'find':
        ref = sys.argv[2] if len(sys.argv) > 2 else '126300'
        r = find_watch(ref)
        print(f"Found {r.get('count', 0)} matches for {ref}")
        for m in r.get('matches', []):
            print(f"  Row {m['row']}: {m['description']} | Sold: {m['sold']}")
    else:
        print("Usage: python3 botsheet_api.py test|find [ref]")
