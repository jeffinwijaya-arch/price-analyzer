"""
Smart bracelet validation for Rolex listings.

Instead of hardcoding every ref, this uses Rolex's reference number structure
and model family rules to determine valid bracelets.

Rolex ref structure:
- Material codes in the ref determine what bracelets are possible
- Model family (from first digits or model name) determines bracelet options
- Some suffixes/materials ONLY come on specific bracelets
"""

import re

# ── Material codes (middle digits of ref) ──
# These map to metals which constrain bracelet options
PRECIOUS_METALS = {'5', '8', '9'}  # 5=Everose, 8=Yellow Gold, 9=White Gold
# Digit 4 in a 6-digit ref often indicates material:
# x2xxxx = Steel, x3xxxx = Steel+Gold, x5xxxx = Everose, x8xxxx = YG, x9xxxx = WG

def _extract_material_digit(ref_digits):
    """Get the material-indicating digit from a Rolex ref."""
    if len(ref_digits) >= 4:
        return ref_digits[1]  # Second digit often indicates material class
    return None


def _load_bracelet_map():
    """Load the official Rolex bracelet map (scraped from rolex.com)."""
    import os
    map_path = os.path.join(os.path.dirname(__file__), 'rolex_bracelet_map.json')
    try:
        import json
        with open(map_path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

_BRACELET_MAP = None

def _get_bracelet_map():
    global _BRACELET_MAP
    if _BRACELET_MAP is None:
        _BRACELET_MAP = _load_bracelet_map()
    return _BRACELET_MAP


def get_valid_bracelets(ref, model=''):
    """
    Return set of valid bracelets for a given ref + model, or None if we can't determine.
    
    Returns:
        set of valid bracelet strings, or None if any bracelet is plausible
    """
    ref = (ref or '').strip().upper()
    model = (model or '').strip()
    model_lower = model.lower()
    
    # ══════════════════════════════════════════════
    # OFFICIAL MAP: Check rolex.com scraped data first
    # ══════════════════════════════════════════════
    ref_clean_for_map = ref.replace('-', '').replace(' ', '')
    ref_digits_for_map = re.match(r'^(\d+)', ref_clean_for_map)
    ref_num = ref_digits_for_map.group(1) if ref_digits_for_map else ''
    
    bmap = _get_bracelet_map()
    if ref_num in bmap:
        bracelets = bmap[ref_num]
        if len(bracelets) == 1:
            return {bracelets[0]}
        else:
            # Multiple valid bracelets — don't enforce
            return None
    
    # Strip ref to digits + letters
    ref_clean = ref.replace('-', '').replace(' ', '')
    # Extract just the numeric prefix
    ref_digits = re.match(r'^(\d+)', ref_clean)
    ref_digits = ref_digits.group(1) if ref_digits else ''
    ref_suffix = ref_clean[len(ref_digits):]  # Letters after digits
    
    # ══════════════════════════════════════════════
    # RULE 1: Day-Date → ALWAYS President
    # ══════════════════════════════════════════════
    if 'day-date' in model_lower or 'daydate' in model_lower:
        return {'President'}
    
    # Day-Date refs: 118xxx, 128xxx, 228xxx where material digit is 2,3,5,8,9
    # But also 18xxx (older). Key: Day-Date refs start with 118, 128, 228, 18
    if ref_digits.startswith(('118', '128', '228')) and len(ref_digits) == 6:
        mat = ref_digits[3]  # 4th digit
        # Day-Date materials: 1=platinum, 2=steel(doesn't exist for DD), 3=two-tone(rare), 5=Everose, 8=YG, 9=WG
        if mat in ('1', '5', '8', '9', '3'):
            return {'President'}
    
    # ══════════════════════════════════════════════
    # RULE 2: Daytona precious metal → material determines bracelet
    # ══════════════════════════════════════════════
    if 'daytona' in model_lower or 'cosmograph' in model_lower:
        return _daytona_bracelets(ref_digits, ref_suffix)
    
    # Daytona refs: 116500, 116505, 116508, 116509, 116515, 116518, 116519, 116520
    #               126500, 126503, 126505, 126508, 126509, 126515, 126518, 126519, 126529
    if ref_digits.startswith(('1165', '1265')) and len(ref_digits) == 6:
        return _daytona_bracelets(ref_digits, ref_suffix)
    
    # ══════════════════════════════════════════════
    # RULE 3: Yacht-Master → multiple valid bracelets
    # ══════════════════════════════════════════════
    # YM42 comes in Oysterflex AND Oyster (titanium/newer versions)
    # YM40 Everose (116655, 126655) → Oysterflex only
    # YM40 steel/two-tone (126621, 126622, 116622) → Oyster only
    # YM37 → Oyster or Oysterflex depending on material
    # Don't force YM42 — let data determine
    if 'yacht-master' in model_lower or ref_digits.startswith(('2266', '2267', '1166')):
        # Only enforce when there's exactly one valid option
        if ref_clean.startswith(('116655', '126655')):
            return {'Oysterflex'}  # Everose 40mm → always Oysterflex
        if ref_clean.startswith(('116622', '126621', '126622', '268621', '268622')):
            return {'Oyster'}  # Steel/two-tone 40mm/37mm → always Oyster
        # Everything else (YM42, YM II, etc.) → multiple valid options
        return None
    
    # ══════════════════════════════════════════════
    # RULE 4: Sky-Dweller → Jubilee, Oyster, OR Oysterflex
    # ══════════════════════════════════════════════
    # Sky-Dweller comes in all three bracelet types depending on ref/material
    # Don't enforce — too variable across the range
    if 'sky-dweller' in model_lower or ref_digits.startswith(('326', '336')):
        return None
    
    # ══════════════════════════════════════════════
    # RULE 6: Submariner / Sea-Dweller → Oyster (steel/two-tone)
    # ══════════════════════════════════════════════
    if ref_digits.startswith(('1166', '1261', '1266')) and len(ref_digits) == 6:
        mat = ref_digits[3]
        if mat in ('1', '0'):  # Steel
            return {'Oyster'}
        # Gold subs can have Oysterflex in newer models
    
    # ══════════════════════════════════════════════
    # RULE 7: Explorer → ALWAYS Oyster
    # ══════════════════════════════════════════════
    if 'explorer' in model_lower:
        return {'Oyster'}
    if ref_digits.startswith(('124', '214', '216', '224', '226')) and 'explorer' in model_lower:
        return {'Oyster'}
    
    # ══════════════════════════════════════════════
    # RULE 8: Oyster Perpetual → ALWAYS Oyster
    # ══════════════════════════════════════════════
    if 'oyster perpetual' in model_lower and 'datejust' not in model_lower:
        return {'Oyster'}
    # Only match known OP refs — NOT Datejust (126334, 126300, etc.)
    # OP refs: 124300, 126000, 134300, 276200, 277200
    op_refs = ('124300', '126000', '134300', '276200', '277200', '114300', '116000')
    if any(ref_digits.startswith(r) for r in op_refs):
        return {'Oyster'}
    
    # ══════════════════════════════════════════════
    # RULE 9: Milgauss → ALWAYS Oyster
    # ══════════════════════════════════════════════
    if 'milgauss' in model_lower or ref_clean.startswith('116400'):
        return {'Oyster'}
    
    # ══════════════════════════════════════════════
    # RULE 10: Air King → ALWAYS Oyster
    # ══════════════════════════════════════════════
    if 'air king' in model_lower or ref_clean.startswith(('126900', '116900')):
        return {'Oyster'}
    
    # Datejust, GMT, Lady-Datejust → multiple valid bracelets, don't correct
    return None


def _daytona_bracelets(ref_digits, ref_suffix):
    """
    Determine valid bracelets for Daytona refs.
    
    Key rules:
    - Steel Daytonas → Oyster always
    - New gen (126xxx) precious metal → Oysterflex always
    - Old gen (116xxx) precious metal:
      - With LN/other suffixes → Oysterflex (post-2017 Cerachrom models)
      - Plain ref (no suffix) → Leather (pre-2017)
    - Platinum → Oysterflex
    - YG old gen (116508, 116528) → Oyster (metal bracelet)
    """
    if len(ref_digits) < 6:
        return None
    
    generation = ref_digits[:3]  # 116 = old gen, 126 = new gen
    last2 = ref_digits[-2:]  # Material indicator
    
    # ── Steel Daytonas → Oyster ──
    # 116500, 126500, 116520, 126520
    if last2 in ('00', '20'):
        return {'Oyster'}
    
    # ── Platinum → Oysterflex ──
    if last2 == '06':
        return {'Oysterflex'}
    
    # ── New generation (126xxx) precious metal → ALL Oysterflex ──
    # 126515, 126518, 126519 and all their suffix variants (G, LN, etc.)
    if generation == '126' and last2 in ('15', '18', '19'):
        # Exception: 126518LN confirmed Oysterflex, 126518G confirmed Oysterflex
        # ALL new-gen precious metal Daytonas come on Oysterflex
        return {'Oysterflex'}
    
    # ── Old generation (116xxx) precious metal ──
    if generation == '116':
        # Everose (xx6515)
        if last2 == '15':
            if ref_suffix:  # 116515LN, 116515A, etc. → Oysterflex
                return {'Oysterflex'}
            else:
                return {'Leather'}  # Plain 116515 → leather
        
        # White Gold (xx6519)
        if last2 == '19':
            if ref_suffix:  # 116519LN, 116519G, etc. → Oysterflex
                return {'Oysterflex'}
            else:
                return {'Leather'}  # Plain 116519 → leather
        
        # Yellow Gold (xx6508, xx6518, xx6528)
        if last2 in ('08', '18', '28'):
            if 'LN' in ref_suffix:
                return {'Oysterflex'}  # 116518LN → Oysterflex
            else:
                return {'Oyster'}  # 116508, 116518, 116528 → Oyster (gold bracelet)
        
        # White Gold on Oyster (xx6509)
        if last2 == '09':
            return {'Oyster'}
    
    # Diamond/special Daytonas (116589, 116595, 116598, 116599, etc.)
    # Gem-set, variable bracelets — don't enforce
    return None


def fix_bracelet(listing):
    """
    Fix bracelet on a single listing if we can determine the correct one.
    
    Returns:
        True if bracelet was fixed, False otherwise
    """
    ref = listing.get('ref', '')
    model = listing.get('model', '')
    current = (listing.get('bracelet', '') or '').strip()
    
    valid = get_valid_bracelets(ref, model)
    
    if valid is None:
        return False  # Can't determine, leave as-is
    
    if len(valid) == 1:
        correct = list(valid)[0]
        if current != correct:
            listing['bracelet'] = correct
            listing['bracelet_fixed'] = True
            return True
    elif current and current not in valid:
        # Current bracelet is invalid but multiple are valid — 
        # we can't pick the right one, but flag it
        # For now, don't change it
        return False
    
    return False


def fix_all_bracelets(listings):
    """Fix bracelets on all listings. Returns count of fixes."""
    fixed = 0
    for l in listings:
        if fix_bracelet(l):
            fixed += 1
    return fixed


def validate_listing(listing):
    """
    Broader validation: check if a listing's ref makes sense with its attributes.
    Returns list of issues found (empty = OK).
    """
    issues = []
    ref = listing.get('ref', '')
    model = listing.get('model', '')
    bracelet = listing.get('bracelet', '')
    price = listing.get('price_usd', 0)
    
    valid_bracelets = get_valid_bracelets(ref, model)
    if valid_bracelets and bracelet and bracelet not in valid_bracelets:
        issues.append(f"bracelet '{bracelet}' invalid for {ref} (expected: {valid_bracelets})")
    
    return issues


if __name__ == '__main__':
    """Test the rules against actual data."""
    import json
    from collections import Counter, defaultdict
    from pathlib import Path
    
    listings_file = Path(__file__).parent / 'rolex_listings.json'
    with open(listings_file) as f:
        data = json.load(f)
    
    fixed = 0
    fix_details = Counter()
    for l in data:
        ref = l.get('ref', '')
        old_bracelet = l.get('bracelet', '')
        if fix_bracelet(l):
            fixed += 1
            fix_details[f"{ref}: {old_bracelet} → {l['bracelet']}"] += 1
    
    print(f"Total fixes: {fixed}")
    for detail, count in fix_details.most_common(30):
        print(f"  {count:>4}x  {detail}")
