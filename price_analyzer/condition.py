"""
condition.py — Score-based condition and completeness parsing.

Uses weighted pattern matching with negation awareness to classify
watch listing condition (BNIB/Unworn/Like New/Pre-owned) and
completeness (Full Set/B&P/Watch Only).
"""

import re
from dataclasses import dataclass, field


@dataclass
class ConditionResult:
    condition: str          # BNIB | Unworn | Like New | Pre-owned | Unknown
    completeness: str       # Full Set | B&P | Watch Only | Unknown
    confidence: float       # 0.0 - 1.0
    signals: list[str] = field(default_factory=list)  # what triggered classification


# ── Condition Signals ─────────────────────────────────────────
# (pattern, condition, score, negatable)
# Higher score wins. Negatable signals are skipped if preceded by negation words.

CONDITION_SIGNALS = [
    # Brand New In Box — highest confidence
    (r'\bBNIB\b', 'BNIB', 10, False),
    (r'\bbrand\s*new\b', 'BNIB', 9, True),
    (r'\bnew\s*old\s*stock\b', 'BNIB', 9, False),
    (r'\bNOS\b', 'BNIB', 9, False),
    (r'\bsealed\b', 'BNIB', 8, True),
    (r'\bstickers?\s*(?:on|intact|still|present)\b', 'BNIB', 7, True),
    (r'\bplastics?\s*(?:on|intact|still)\b', 'BNIB', 7, True),
    (r'\bfactory\s*sealed\b', 'BNIB', 9, False),
    (r'\bunsized\b', 'BNIB', 6, True),

    # Unworn
    (r'\bunworn\b', 'Unworn', 8, True),
    (r'\bnever\s*worn\b', 'Unworn', 8, False),
    (r'\bUW\b', 'Unworn', 7, False),
    (r'\bnever\s*sized\b', 'Unworn', 6, False),
    (r'\b0\s*wear\b', 'Unworn', 7, False),

    # Like New
    (r'\blike\s*new\b', 'Like New', 6, False),
    (r'\bLN\b', 'Like New', 6, False),
    (r'\bmint\s*condition\b', 'Like New', 5, False),
    (r'\bmint\b', 'Like New', 5, True),
    (r'\b(?:9\.5|9\.8|9\.9)/10\b', 'Like New', 5, False),
    (r'\bvirtually\s*new\b', 'Like New', 5, False),
    (r'\bnearly\s*new\b', 'Like New', 5, False),

    # Pre-owned (various grades)
    (r'\bexcellent\s*(?:condition)?\b', 'Pre-owned', 4, False),
    (r'\bvery\s*good\s*(?:condition)?\b', 'Pre-owned', 3, False),
    (r'\bgood\s*(?:condition)?\b', 'Pre-owned', 2, False),
    (r'\bpre\s*-?\s*owned\b', 'Pre-owned', 3, False),
    (r'\bused\b', 'Pre-owned', 3, True),
    (r'\bworn\b', 'Pre-owned', 2, True),
    (r'\bpolished\b', 'Pre-owned', 2, False),
    (r'\bscratche?s?\b', 'Pre-owned', 2, False),
    (r'\bmarks?\b', 'Pre-owned', 1, True),
    (r'\bfaded\b', 'Pre-owned', 2, False),
    (r'\bservice(?:d)?\b', 'Pre-owned', 1, False),
    (r'\b[78]/10\b', 'Pre-owned', 3, False),
]

# ── Completeness Signals ──────────────────────────────────────
# (pattern, completeness, score)

COMPLETENESS_SIGNALS = [
    (r'\bfull\s*set\b', 'Full Set', 10),
    (r'\bFS\b', 'Full Set', 8),
    (r'\bdouble\s*box\b', 'Full Set', 7),
    (r'\bcomplete\s*set\b', 'Full Set', 9),
    (r'\bcomplete\b', 'Full Set', 4),
    (r'\bbox\s*(?:&|and|,)\s*papers?\b', 'B&P', 8),
    (r'\bB\s*[&+]\s*P\b', 'B&P', 8),
    (r'\bBnP\b', 'B&P', 8),
    (r'\bwith\s*(?:all\s*)?(?:box|card|papers?|accessories|everything)\b', 'B&P', 5),
    (r'\bwith\s*everything\b', 'Full Set', 7),
    (r'\bcard\s*dated\b', 'B&P', 4),
    (r'\bwarranty\s*card\b', 'B&P', 4),
    (r'\bgreen\s*card\b', 'B&P', 4),
    (r'\binner\s*(?:&|and)\s*outer\s*box\b', 'B&P', 5),
    (r'\bwatch\s*only\b', 'Watch Only', 8),
    (r'\bWO\b', 'Watch Only', 7),
    (r'\bhead\s*only\b', 'Watch Only', 6),
    (r'\bno\s*(?:box|papers?|card)\b', 'Watch Only', 5),
    (r'\bwithout\s*(?:box|papers?|card)\b', 'Watch Only', 5),
    (r'\bmissing\s*(?:box|papers?|card)\b', 'Watch Only', 5),
    (r'\bbare\s*watch\b', 'Watch Only', 6),
]

# ── Negation Detection ────────────────────────────────────────
NEGATION_RE = re.compile(
    r'\b(no|not|never|without|w/o|don\'t|doesn\'t|isn\'t|wasn\'t|aren\'t|weren\'t|hardly|barely)\b',
    re.I
)

# Special: "un" prefix is NOT negation for "unworn" — it's the actual condition word
# But "not unworn" IS negation. Handled by checking negation before the full match.


def parse_condition(text: str) -> ConditionResult:
    """
    Score-based condition and completeness extraction with negation awareness.

    The highest-scoring matching signal wins for both condition and completeness.
    Negatable signals are skipped if a negation word appears in the 40 characters
    preceding the match.

    Args:
        text: Raw listing text.

    Returns:
        ConditionResult with condition, completeness, confidence, and signals.
    """
    text_lower = text.lower()
    signals = []

    # ── Condition scoring ──
    best_cond = 'Unknown'
    best_cond_score = 0

    for pattern, condition, score, negatable in CONDITION_SIGNALS:
        m = re.search(pattern, text, re.I)
        if not m:
            continue

        # Check for negation in the 40 chars before the match
        if negatable:
            prefix_start = max(0, m.start() - 40)
            prefix = text_lower[prefix_start:m.start()]
            if NEGATION_RE.search(prefix):
                signals.append(f'NEG:{condition}:{pattern}')
                continue

        if score > best_cond_score:
            best_cond_score = score
            best_cond = condition
            signals.append(f'+{condition}({score}):{pattern}')

    # ── Completeness scoring ──
    best_comp = 'Unknown'
    best_comp_score = 0

    for pattern, comp, score in COMPLETENESS_SIGNALS:
        m = re.search(pattern, text, re.I)
        if not m:
            continue

        if score > best_comp_score:
            best_comp_score = score
            best_comp = comp
            signals.append(f'+{comp}({score}):{pattern}')

    # ── Confidence ──
    # Max possible: condition=10 + completeness=10 = 20
    confidence = min(1.0, (best_cond_score + best_comp_score) / 18.0)

    return ConditionResult(
        condition=best_cond,
        completeness=best_comp,
        confidence=confidence,
        signals=signals,
    )


def condition_emoji(condition: str) -> str:
    """Return emoji for a condition level."""
    return {
        'BNIB': '🏷️',
        'Unworn': '✨',
        'Like New': '👌',
        'Pre-owned': '⏱️',
        'Unknown': '❓',
    }.get(condition, '❓')


def completeness_emoji(completeness: str) -> str:
    """Return emoji for a completeness level."""
    return {
        'Full Set': '📦',
        'B&P': '📄',
        'Watch Only': '⌚',
        'Unknown': '❓',
    }.get(completeness, '❓')


if __name__ == '__main__':
    tests = [
        ("BNIB full set stickers on", "BNIB", "Full Set"),
        ("Unworn, no stickers, B&P", "Unworn", "B&P"),
        ("not unworn but excellent condition with box", "Pre-owned", "B&P"),
        ("LN with everything", "Like New", "Full Set"),
        ("Watch only, polished, no box no papers", "Pre-owned", "Watch Only"),
        ("BNIB FS sealed 2024 card", "BNIB", "Full Set"),
        ("Pre-owned, serviced, complete set", "Pre-owned", "Full Set"),
        ("Brand new never worn full set double box", "BNIB", "Full Set"),
        ("used watch head only scratches on case", "Pre-owned", "Watch Only"),
        ("mint condition B&P card dated 2025", "Like New", "B&P"),
        ("NOS factory sealed", "BNIB", "Unknown"),
        ("9.5/10 with box and papers", "Like New", "B&P"),
        ("no stickers, unsized, full set", "Unworn", "Full Set"),
    ]

    print("Condition Parsing Tests:")
    for text, exp_cond, exp_comp in tests:
        result = parse_condition(text)
        cond_ok = "✅" if result.condition == exp_cond else "❌"
        comp_ok = "✅" if result.completeness == exp_comp else "❌"
        print(f"  {cond_ok}{comp_ok} '{text[:50]:<50s}' → {result.condition:12s} | {result.completeness:12s} (conf={result.confidence:.2f})")
        if result.condition != exp_cond or result.completeness != exp_comp:
            print(f"       Expected: {exp_cond:12s} | {exp_comp:12s}")
            print(f"       Signals: {result.signals}")
