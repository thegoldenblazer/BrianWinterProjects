"""
RunGood Poker Series - Player Name Resolution
Normalizes player names and uses fuzzy matching to merge duplicates.
"""

import json
import re
from collections import defaultdict
from pathlib import Path

from thefuzz import fuzz

TOURNAMENTS_FILE = Path("data/tournaments.json")
RESOLVED_FILE = Path("data/tournaments_resolved.json")
NAME_MATCHES_FILE = Path("data/name_matches.json")
OVERRIDES_FILE = Path("data/name_overrides.json")

FUZZY_THRESHOLD = 88  # minimum similarity score to consider a match


def normalize_name(name: str) -> str:
    """Normalize a player name to Title Case, strip junk."""
    name = name.strip()
    # Remove trailing periods
    name = name.rstrip(".")
    # Collapse multiple spaces
    name = re.sub(r"\s+", " ", name)
    # Convert to Title Case
    name = name.title()
    # Fix common Title Case artifacts (Mc, O', etc.)
    name = re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), name)
    name = re.sub(r"\bO'([a-z])", lambda m: "O'" + m.group(1).upper(), name)
    name = re.sub(r"\bD'([a-z])", lambda m: "D'" + m.group(1).upper(), name)
    return name


def build_name_index(tournaments: list[dict]) -> dict[str, int]:
    """Build an index of all unique normalized names and their occurrence counts."""
    counts = defaultdict(int)
    for t in tournaments:
        for r in t["results"]:
            norm = normalize_name(r["player"])
            counts[norm] += 1
    return dict(counts)


def find_fuzzy_groups(name_counts: dict[str, int]) -> list[list[str]]:
    """Find groups of names that are likely the same person."""
    names = sorted(name_counts.keys())
    merged = set()
    groups = []

    for i, name_a in enumerate(names):
        if name_a in merged:
            continue

        group = [name_a]
        for j in range(i + 1, len(names)):
            name_b = names[j]
            if name_b in merged:
                continue

            score = fuzz.ratio(name_a, name_b)
            if score >= FUZZY_THRESHOLD:
                group.append(name_b)
                merged.add(name_b)

            # Also check if one name is a substring/abbreviation of the other
            # e.g., "Michael R" vs "Michael Ryan"
            parts_a = name_a.split()
            parts_b = name_b.split()
            if len(parts_a) >= 2 and len(parts_b) >= 2:
                if parts_a[0] == parts_b[0]:  # Same first name
                    # Check if last name is abbreviated
                    if (len(parts_a[-1]) <= 2 or len(parts_b[-1]) <= 2):
                        # One has an abbreviation, check first name match
                        if parts_a[-1][0] == parts_b[-1][0]:
                            if name_b not in group:
                                group.append(name_b)
                                merged.add(name_b)

        if len(group) > 1:
            merged.add(name_a)
            groups.append(group)

    return groups


def pick_canonical(group: list[str], name_counts: dict[str, int]) -> str:
    """Pick the canonical name from a group: prefer the most frequent, longest non-abbreviated name."""
    # Filter out abbreviated names (single-letter last names)
    full_names = [n for n in group if len(n.split()[-1]) > 2]
    candidates = full_names if full_names else group

    # Pick the most frequent
    return max(candidates, key=lambda n: (name_counts.get(n, 0), len(n)))


def build_mapping(groups: list[list[str]], name_counts: dict[str, int]) -> dict[str, str]:
    """Build a mapping from variant names to canonical names."""
    mapping = {}
    for group in groups:
        canonical = pick_canonical(group, name_counts)
        for name in group:
            if name != canonical:
                mapping[name] = canonical
    return mapping


def resolve_names():
    """Main entry point: normalize names and build fuzzy matching groups."""
    with open(TOURNAMENTS_FILE, "r", encoding="utf-8") as f:
        tournaments = json.load(f)

    print(f"Loaded {len(tournaments)} tournaments")

    # Normalize all names in place
    for t in tournaments:
        for r in t["results"]:
            r["player"] = normalize_name(r["player"])

    # Build name index
    name_counts = build_name_index(tournaments)
    print(f"Found {len(name_counts)} unique player names")

    # Find fuzzy groups
    groups = find_fuzzy_groups(name_counts)
    print(f"Found {len(groups)} potential name merge groups")

    # Build mapping
    mapping = build_mapping(groups, name_counts)

    # Load manual overrides if they exist
    if OVERRIDES_FILE.exists():
        with open(OVERRIDES_FILE, "r", encoding="utf-8") as f:
            overrides = json.load(f)
        mapping.update(overrides)
        # Resolve transitive chains: if A->B and B->C, make A->C
        changed = True
        while changed:
            changed = False
            for k, v in mapping.items():
                if v in mapping and mapping[v] != v:
                    mapping[k] = mapping[v]
                    changed = True
        print(f"Applied {len(overrides)} manual overrides")

    # Save proposed matches for review
    review_data = []
    for group in groups:
        canonical = pick_canonical(group, name_counts)
        review_data.append({
            "canonical": canonical,
            "variants": [n for n in group if n != canonical],
            "counts": {n: name_counts.get(n, 0) for n in group},
        })

    with open(NAME_MATCHES_FILE, "w", encoding="utf-8") as f:
        json.dump(review_data, f, indent=2, ensure_ascii=False)
    print(f"Saved proposed merges to {NAME_MATCHES_FILE}")

    # Apply mapping to tournaments
    changes = 0
    for t in tournaments:
        for r in t["results"]:
            if r["player"] in mapping:
                r["player"] = mapping[r["player"]]
                changes += 1

    # Recount after merging
    final_counts = build_name_index(tournaments)
    print(f"After merging: {len(final_counts)} unique players ({changes} name changes applied)")

    # Save resolved tournaments
    with open(RESOLVED_FILE, "w", encoding="utf-8") as f:
        json.dump(tournaments, f, indent=2, ensure_ascii=False)

    print(f"Saved resolved tournaments to {RESOLVED_FILE}")


if __name__ == "__main__":
    resolve_names()
