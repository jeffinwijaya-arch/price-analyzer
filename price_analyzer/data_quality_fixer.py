#!/usr/bin/env python3
"""
Data Quality Fixer
==================
Automatically fixes known data quality issues in the price analyzer.
Based on analysis from TODO_v5.md and ANALYSIS_REPORT_v2.md.
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent

class DataQualityFixer:
    """Fixes common data quality issues in parsed listings."""
    
    def __init__(self):
        self.load_reference_data()
        self.load_seller_aliases()
        self.fixes_applied = defaultdict(int)
        self.rejected_listings = []
    
    def load_reference_data(self):
        """Load reference validation data."""
        try:
            with open(BASE_DIR / 'reference_data.json', 'r') as f:
                data = json.load(f)
                self.ref_valid_dials = data.get('valid_dials', {})
                self.ref_valid_bracelets = data.get('valid_bracelets', {})
                print(f"✅ Loaded validation data for {len(self.ref_valid_dials)} references")
        except FileNotFoundError:
            print("⚠️ reference_data.json not found - dial validation disabled")
            self.ref_valid_dials = {}
            self.ref_valid_bracelets = {}
    
    def load_seller_aliases(self):
        """Load seller alias mappings."""
        try:
            with open(BASE_DIR / 'seller_aliases.json', 'r') as f:
                self.seller_aliases = json.load(f)
                print(f"✅ Loaded {len(self.seller_aliases)} seller aliases")
        except FileNotFoundError:
            # Create default aliases from TODO analysis
            self.seller_aliases = {
                "Elara": ["cici", "+852 6236 1307"],
                "LEO": ["LAN", "tuzik", "Annie", "+852 6615 6762", "+852 6819 8802", "+852 6555 1058"],
                "Hugh": ["+8619388924279", "+852 5203 7160"],
                "KEN": ["+852 6706 7869"],
                "Henson": ["Chris", "+852 6765 0492", "+852 6068 1901"],
                "Mina": ["+852 9648 5603"],
                "Cotton": ["+852 6379 5543", "+852 5209 6148"],
                "kevin": ["+86 172 8848 9071"],
                "poggi": ["+852 6534 5866"],
                "natalia": ["+39 351 239 5744"],
                "shijuan": ["+852 5203 1462"],
                "Mia": ["+852 5950 8219"],
                "Eve": ["+86 137 1157 8875"],
                "zoe": ["+86 153 3743 6034"],
                "Don.S": ["+852 6350 6004"],
                "Flavia": ["+39 351 153 7973"],
                "eybv": ["+852 5209 2519"]
            }
            # Save for future use
            with open(BASE_DIR / 'seller_aliases.json', 'w') as f:
                json.dump(self.seller_aliases, f, indent=2)
            print(f"✅ Created seller aliases with {len(self.seller_aliases)} mappings")
    
    def normalize_seller_name(self, seller: str) -> str:
        """Normalize seller name using alias mappings."""
        if not seller:
            return seller
        
        # Check if this seller is an alias for someone else
        for canonical_name, aliases in self.seller_aliases.items():
            if seller in aliases or seller == canonical_name:
                self.fixes_applied['seller_normalized'] += 1
                return canonical_name
        
        # Clean up phone number formats
        if re.match(r'^\+?\d+[\d\s\-]+$', seller):
            # This looks like a phone number - normalize format
            clean_phone = re.sub(r'[\s\-]', '', seller)
            if not clean_phone.startswith('+'):
                clean_phone = '+' + clean_phone
            return clean_phone
        
        return seller
    
    def fix_impossible_dial_combinations(self, listing: Dict) -> Optional[Dict]:
        """
        Fix or reject listings with impossible dial/ref combinations.
        Based on analysis showing 43+ listings for 126500LN with wrong dials.
        """
        ref = listing.get('ref')
        dial = listing.get('dial')
        
        if not ref or not dial:
            return listing
        
        valid_dials = self.ref_valid_dials.get(ref, [])
        
        # If we have validation data and dial is invalid
        if valid_dials and dial not in valid_dials:
            # Known problematic combinations from analysis
            problematic_combinations = {
                '126500LN': ['Green Ombré', 'Puzzles', 'Onyx', 'Mint Green', 'Tiffany'],
                '228235': ['Black', 'White', 'Grey', 'Brown'],  # DD40 RG
                '126508': ['Gold', 'Grey', 'Blue'],  # Daytona YG
                '126334G': ['Champagne', 'vi MOP'],  # DJ 41 Steel Diamond
                '228238': ['Tiffany Blue']  # Some are clearly wrong prices
            }
            
            if ref in problematic_combinations and dial in problematic_combinations[ref]:
                self.fixes_applied['impossible_dial_rejected'] += 1
                self.rejected_listings.append({
                    'reason': 'impossible_dial',
                    'ref': ref,
                    'dial': dial,
                    'valid_dials': valid_dials,
                    'listing': listing
                })
                return None  # Reject this listing
            
            # Try to fix common dial misinterpretations
            dial_fixes = {
                'Gold': 'Champagne',  # Gold case material → Champagne dial
                'Steel': None,  # Case material, not dial
                'Rose Gold': None,
                'Yellow Gold': None,
            }
            
            if dial in dial_fixes:
                new_dial = dial_fixes[dial]
                if new_dial and new_dial in valid_dials:
                    listing['dial'] = new_dial
                    self.fixes_applied['dial_corrected'] += 1
                else:
                    listing['dial'] = None
                    self.fixes_applied['dial_cleared'] += 1
        
        return listing
    
    def fix_future_date_bug(self, listing: Dict) -> Dict:
        """
        Fix N-serial card year future date bug.
        Analysis shows 1,708 listings have impossible future dates.
        """
        year = listing.get('year')
        if not year:
            return listing
        
        # Match MM/YYYY format
        date_match = re.match(r'^(\d{2})/(\d{4})$', year)
        if not date_match:
            return listing
        
        month, year_num = date_match.groups()
        month_int = int(month)
        year_int = int(year_num)
        
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # If year is in future, or year is current but month is >2 months in future
        if year_int > current_year or (year_int == current_year and month_int > current_month + 2):
            # Likely N-serial bug - adjust to previous year
            corrected_year = year_int - 1
            listing['year'] = f"{month}/{corrected_year}"
            self.fixes_applied['future_date_corrected'] += 1
        
        return listing
    
    def fix_obvious_price_errors(self, listing: Dict) -> Optional[Dict]:
        """
        Fix obvious price errors based on analysis.
        Example: 128238 with $8,400 price when others are $85k-$100k.
        """
        ref = listing.get('ref')
        price = listing.get('price_usd')
        
        if not ref or not price:
            return listing
        
        # Known reference price ranges (rough validation)
        expected_ranges = {
            '228238': (80000, 120000),   # Day-Date 40 YG
            '228235': (45000, 70000),    # Day-Date 40 RG  
            '126500LN': (27000, 40000),  # Daytona SS Ceramic
            '126508': (55000, 80000),    # Daytona YG
            '126334G': (15000, 25000),   # DJ 41 Steel Diamond
            '116500LN': (30000, 45000),  # Daytona SS (prev gen)
        }
        
        expected_range = expected_ranges.get(ref)
        if expected_range:
            min_price, max_price = expected_range
            
            # Price is way too low (likely wrong ref association)
            if price < min_price * 0.3:
                self.fixes_applied['price_too_low_rejected'] += 1
                self.rejected_listings.append({
                    'reason': 'price_too_low',
                    'ref': ref,
                    'price': price,
                    'expected_range': expected_range,
                    'listing': listing
                })
                return None
            
            # Price is way too high (likely wrong currency or typo)
            elif price > max_price * 2:
                # Check if it might be in wrong currency (e.g., HKD listed as USD)
                if price > max_price * 7 and price < max_price * 9:
                    # Likely HKD mistaken as USD (7.8x multiplier)
                    corrected_price = price / 7.8
                    if min_price <= corrected_price <= max_price:
                        listing['price_usd'] = corrected_price
                        listing['currency'] = 'USD'
                        self.fixes_applied['currency_corrected'] += 1
                else:
                    self.fixes_applied['price_too_high_rejected'] += 1
                    self.rejected_listings.append({
                        'reason': 'price_too_high',
                        'ref': ref,
                        'price': price,
                        'expected_range': expected_range,
                        'listing': listing
                    })
                    return None
        
        return listing
    
    def fix_duplicate_listings(self, listings: List[Dict]) -> List[Dict]:
        """
        Remove duplicate listings based on improved deduplication.
        Uses seller normalization and better matching.
        """
        seen_keys = set()
        unique_listings = []
        
        for listing in listings:
            # Normalize seller first
            original_seller = listing.get('seller', '')
            normalized_seller = self.normalize_seller_name(original_seller)
            listing['seller'] = normalized_seller
            
            # Create dedup key
            ref = listing.get('ref', '')
            price = listing.get('price_usd', 0)
            dial = listing.get('dial', '')
            
            # Round price to nearest 100 for fuzzy matching
            price_rounded = round(price / 100) * 100 if price else 0
            
            dedup_key = f"{ref}:{price_rounded}:{dial}:{normalized_seller}"
            
            if dedup_key in seen_keys:
                self.fixes_applied['duplicates_removed'] += 1
            else:
                seen_keys.add(dedup_key)
                unique_listings.append(listing)
        
        return unique_listings
    
    def process_listings(self, listings: List[Dict]) -> List[Dict]:
        """Process all listings through quality fixes."""
        fixed_listings = []
        
        print(f"🔧 Processing {len(listings)} listings for quality fixes...")
        
        for listing in listings:
            # Apply all fixes in sequence
            fixed_listing = listing.copy()
            
            # Fix seller names first
            fixed_listing['seller'] = self.normalize_seller_name(fixed_listing.get('seller', ''))
            
            # Fix impossible dial combinations (may reject listing)
            fixed_listing = self.fix_impossible_dial_combinations(fixed_listing)
            if fixed_listing is None:
                continue  # Listing was rejected
            
            # Fix future date bug
            fixed_listing = self.fix_future_date_bug(fixed_listing)
            
            # Fix obvious price errors (may reject listing)
            fixed_listing = self.fix_obvious_price_errors(fixed_listing)
            if fixed_listing is None:
                continue  # Listing was rejected
            
            fixed_listings.append(fixed_listing)
        
        # Remove duplicates (after all other fixes)
        fixed_listings = self.fix_duplicate_listings(fixed_listings)
        
        return fixed_listings
    
    def generate_fix_report(self) -> str:
        """Generate a report of all fixes applied."""
        lines = []
        lines.append("DATA QUALITY FIX REPORT")
        lines.append("=" * 50)
        lines.append(f"Timestamp: {datetime.now().isoformat()}")
        lines.append("")
        
        if self.fixes_applied:
            lines.append("✅ FIXES APPLIED:")
            for fix_type, count in self.fixes_applied.items():
                lines.append(f"  • {fix_type.replace('_', ' ').title()}: {count}")
            lines.append("")
        
        if self.rejected_listings:
            lines.append(f"🗑️ REJECTED LISTINGS: {len(self.rejected_listings)}")
            rejection_reasons = defaultdict(int)
            for rejected in self.rejected_listings:
                rejection_reasons[rejected['reason']] += 1
            
            for reason, count in rejection_reasons.items():
                lines.append(f"  • {reason.replace('_', ' ').title()}: {count}")
            lines.append("")
        
        # Show some examples of rejected listings
        if self.rejected_listings:
            lines.append("📋 REJECTION EXAMPLES:")
            for rejected in self.rejected_listings[:5]:
                ref = rejected['listing'].get('ref', 'Unknown')
                reason = rejected['reason']
                if reason == 'impossible_dial':
                    dial = rejected['dial']
                    lines.append(f"  • {ref} with '{dial}' dial (invalid)")
                elif reason in ['price_too_low', 'price_too_high']:
                    price = rejected['price']
                    lines.append(f"  • {ref} at ${price:,.0f} ({reason.replace('_', ' ')})")
        
        total_applied = sum(self.fixes_applied.values())
        lines.append(f"📊 SUMMARY: {total_applied} fixes applied, {len(self.rejected_listings)} listings rejected")
        
        return "\n".join(lines)


def main():
    """Main function to run data quality fixes on current dataset."""
    # Load current listings
    try:
        with open(BASE_DIR / 'rolex_listings.json', 'r') as f:
            listings = json.load(f)
        print(f"📊 Loaded {len(listings)} listings")
    except FileNotFoundError:
        print("❌ rolex_listings.json not found")
        return
    
    # Initialize fixer
    fixer = DataQualityFixer()
    
    # Process listings
    fixed_listings = fixer.process_listings(listings)
    
    # Generate report
    report = fixer.generate_fix_report()
    print("\n" + report)
    
    # Save fixed listings
    backup_path = BASE_DIR / f'rolex_listings_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(backup_path, 'w') as f:
        json.dump(listings, f, indent=2)
    
    with open(BASE_DIR / 'rolex_listings.json', 'w') as f:
        json.dump(fixed_listings, f, indent=2)
    
    print(f"\n✅ Fixed dataset saved ({len(fixed_listings)} listings)")
    print(f"📁 Original backed up to: {backup_path.name}")
    
    # Save rejected listings for analysis
    if fixer.rejected_listings:
        rejected_path = BASE_DIR / f'rejected_listings_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(rejected_path, 'w') as f:
            json.dump(fixer.rejected_listings, f, indent=2)
        print(f"🗑️ Rejected listings saved to: {rejected_path.name}")


if __name__ == "__main__":
    main()