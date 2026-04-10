"""Apple Vision OCR wrapper — uses macOS built-in Vision framework via Swift.
Free, local, no API tokens. Much better than EasyOCR for warranty cards."""

import subprocess
import json
import os

SWIFT_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'apple_ocr.swift')

def ocr_warranty_card(image_path: str) -> dict:
    """OCR a warranty card image using Apple Vision.
    
    Returns dict with: ref, serial, card_date, has_wt, full_text, line_count
    """
    try:
        result = subprocess.run(
            ['swift', SWIFT_SCRIPT, image_path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout.strip())
            return data
        else:
            return {'error': result.stderr.strip() or 'OCR failed', 'ref': '', 'serial': '', 'card_date': '', 'has_wt': False}
    except subprocess.TimeoutExpired:
        return {'error': 'OCR timeout', 'ref': '', 'serial': '', 'card_date': '', 'has_wt': False}
    except Exception as e:
        return {'error': str(e), 'ref': '', 'serial': '', 'card_date': '', 'has_wt': False}


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        result = ocr_warranty_card(sys.argv[1])
        print(json.dumps(result, indent=2))
    else:
        print("Usage: python3 apple_ocr.py <image_path>")
