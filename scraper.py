"""
RunGood Poker Series - Tournament Results Scraper
Scrapes tournament results from https://www.rungood.com/blogs/news-1
Caches raw HTML to data/raw/ to avoid re-fetching.
"""

import json
import os
import re
import hashlib
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE_URL = "https://www.rungood.com/blogs/news-1"
RAW_DIR = Path("data/raw")
OUTPUT_FILE = Path("data/tournaments.json")
DELAY = 1.5  # seconds between requests

# Patterns for pages to skip
SKIP_TITLE_PATTERNS = [
    re.compile(r"tag\s+team", re.IGNORECASE),
    re.compile(r"redraw", re.IGNORECASE),
    re.compile(r"seating?\s+assignments?", re.IGNORECASE),
    re.compile(r"chip\s+counts?", re.IGNORECASE),
    re.compile(r"cancelled", re.IGNORECASE),
    re.compile(r"free\s+to\s+play", re.IGNORECASE),
    re.compile(r"trivia\s+for\s+toys", re.IGNORECASE),
    re.compile(r"keep\s+the\s+lights\s+on", re.IGNORECASE),
    re.compile(r"registration\s+list", re.IGNORECASE),
    re.compile(r"is\s+back", re.IGNORECASE),
    re.compile(r"update\s+on\b", re.IGNORECASE),
    re.compile(r"crowns\b.*casino\s+champ", re.IGNORECASE),
    re.compile(r"top\s+100\s+in\s+casino", re.IGNORECASE),
    re.compile(r"^(?:updated\s+)?(?:casino\s+champ\s+)?point\s+standings", re.IGNORECASE),
]

session = requests.Session()
session.headers.update({
    "User-Agent": "RunGoodELO/1.0 (Tournament Rating Research)"
})


def cache_path(url: str) -> Path:
    """Generate a cache file path for a URL."""
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return RAW_DIR / f"{url_hash}.html"


def fetch(url: str, *, force: bool = False) -> str:
    """Fetch a URL with caching and rate limiting.

    If ``force`` is True the cache is bypassed and overwritten with a fresh fetch.
    Used for index pages, which change as new posts are added.
    """
    cached = cache_path(url)
    if cached.exists() and not force:
        return cached.read_text(encoding="utf-8")

    time.sleep(DELAY)
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    html = resp.text
    cached.write_text(html, encoding="utf-8")
    return html


def get_tournament_urls() -> list[str]:
    """Crawl all index pages and extract tournament blog post URLs.

    Walks pages starting at 1 and stops when a page yields no new article
    links (i.e. we've gone past the last real page). Index pages are always
    re-fetched fresh so newly published tournaments are picked up.
    """
    urls = []
    page_num = 0
    pbar = tqdm(desc="Crawling index pages")
    while True:
        page_num += 1
        page_url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
        pbar.update(1)

        html = fetch(page_url, force=True)
        soup = BeautifulSoup(html, "lxml")

        page_links = soup.select("h2 a[href*='/blogs/news-1/']")
        if not page_links:
            break

        added_this_page = 0
        for link in page_links:
            href = link.get("href", "")
            if href:
                full_url = urljoin("https://www.rungood.com", href)
                if full_url not in urls:
                    urls.append(full_url)
                    added_this_page += 1

        # All links on this page were already collected from earlier pages —
        # treat as end of pagination to avoid an infinite loop on a misbehaving site.
        if added_this_page == 0:
            break

    pbar.close()
    print(f"Found {len(urls)} tournament URLs across {page_num} index page(s)")
    return urls


def should_skip(title: str) -> bool:
    """Check if a page should be skipped based on title."""
    for pattern in SKIP_TITLE_PATTERNS:
        if pattern.search(title):
            return True
    return False


def parse_money(text: str) -> float:
    """Parse a money string like '$11,270' or '$11,270.00' to float."""
    if not text:
        return 0.0
    cleaned = re.sub(r"[^0-9.]", "", text.strip())
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def detect_columns(header_cells: list[str]) -> dict:
    """Detect which column index maps to place, player, prize."""
    mapping = {}
    for i, cell in enumerate(header_cells):
        cell_lower = cell.strip().lower()
        if cell_lower in ("place", "#", "rank"):
            mapping["place"] = i
        elif cell_lower in ("player", "name", "player name", "nickname"):
            mapping["player"] = i
        elif cell_lower in ("first", "first name"):
            mapping["first"] = i
        elif cell_lower in ("last", "last name"):
            mapping["last"] = i
        elif cell_lower in ("prize", "payout", "payouts", "prizes", "amount", "winnings"):
            mapping["prize"] = i
        elif "prize" in cell_lower or "payout" in cell_lower or "winning" in cell_lower:
            mapping["prize"] = i
        elif cell_lower == "points":
            mapping["points"] = i
        elif "chip count" in cell_lower or cell_lower == "chips":
            mapping["chip_count"] = i
    return mapping


def detect_columns_from_data(rows: list[list[str]], num_cols: int) -> dict:
    """Infer column roles from data when there's no header row."""
    if num_cols < 3:
        return {}

    # Score each column by checking multiple rows
    col_scores = {i: {"place": 0, "prize": 0, "player": 0} for i in range(num_cols)}

    for row in rows[:5]:
        if len(row) < num_cols:
            continue
        for i in range(num_cols):
            cell = row[i].strip()
            if re.match(r"^\d{1,3}$", cell) and int(cell) < 500:
                col_scores[i]["place"] += 1
            if re.match(r"^\$[\d,]+", cell):
                col_scores[i]["prize"] += 1
            if re.match(r"^[A-Za-z\s\-\'.]+$", cell) and len(cell) > 2:
                col_scores[i]["player"] += 1

    mapping = {}
    # Assign each role to the column with the highest score
    for role in ("place", "prize", "player"):
        best_col = max(range(num_cols), key=lambda i: col_scores[i][role])
        if col_scores[best_col][role] > 0:
            mapping[role] = best_col

    # Ensure no two roles share a column
    assigned = set()
    final = {}
    for role in ("place", "player", "prize"):
        col = mapping.get(role)
        if col is not None and col not in assigned:
            final[role] = col
            assigned.add(col)

    return final


def is_results_table(rows: list[list[str]], col_map: dict) -> bool:
    """Verify this table actually contains tournament results."""
    if "place" not in col_map or "player" not in col_map:
        return False

    # Check that at least some rows have numeric place values
    place_col = col_map["place"]
    numeric_places = 0
    for row in rows:
        if place_col < len(row):
            if re.match(r"^\d{1,3}$", row[place_col].strip()):
                numeric_places += 1

    return numeric_places >= 3


def parse_results_table(table_el) -> list[dict]:
    """Parse an HTML table element into a list of result dicts."""
    rows_raw = []
    header_cells = []

    # Check for thead
    thead = table_el.find("thead")
    if thead:
        for th in thead.find_all(["th", "td"]):
            header_cells.append(th.get_text(strip=True))

    # Get all body rows
    tbody = table_el.find("tbody") or table_el
    for tr in tbody.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if cells:
            rows_raw.append(cells)

    if not rows_raw:
        return []

    num_cols = max(len(r) for r in rows_raw)

    # Try to detect columns from header first
    col_map = {}
    header_width = 0
    if header_cells:
        col_map = detect_columns(header_cells)
        header_width = len(header_cells)
    
    # If first row looks like a header, try that
    if not col_map or ("player" not in col_map and "first" not in col_map):
        first_row = rows_raw[0]
        potential_header = detect_columns(first_row)
        if "player" in potential_header or "first" in potential_header:
            col_map = potential_header
            header_width = len(first_row)
            rows_raw = rows_raw[1:]  # Remove header row

            # Handle spacer columns: if header has more cells than data rows,
            # re-detect from non-blank header cells so indices match data
            if rows_raw:
                data_widths = [len(r) for r in rows_raw[:10] if r]
                typical_width = min(data_widths) if data_widths else 0
                if len(first_row) > typical_width and typical_width >= 3:
                    filtered_header = [c for c in first_row if c.strip()]
                    if len(filtered_header) <= typical_width:
                        col_map = detect_columns(filtered_header)
                        header_width = len(filtered_header)

    # If still no mapping, infer from data
    if not col_map or ("player" not in col_map and "first" not in col_map):
        col_map = detect_columns_from_data(rows_raw, num_cols)
        header_width = 0  # no header, data-based detection

    # Detect first/last name split: a "Name" or "Player Name" column might
    # actually span two data columns (first + last names).
    if "player" in col_map and "first" not in col_map and rows_raw:
        player_idx = col_map["player"]
        data_widths = [len(r) for r in rows_raw[:10] if r]
        typical_data_width = max(data_widths) if data_widths else 0

        # Check if data at player_idx looks like single-word entries (first or last names)
        sample_players = [r[player_idx].strip() for r in rows_raw[:10]
                          if len(r) > player_idx and r[player_idx].strip()]
        looks_like_split = (sample_players and
                            all(" " not in p for p in sample_players) and
                            all(len(p) < 15 for p in sample_players))

        if looks_like_split:
            next_idx = player_idx + 1
            if next_idx < typical_data_width:
                sample_next = [r[next_idx].strip() for r in rows_raw[:10]
                               if len(r) > next_idx and r[next_idx].strip()]
                next_is_names = (sample_next and
                                 all(re.match(r'^[A-Za-z\-\'\. ]+$', n) for n in sample_next))
                if next_is_names:
                    # Convert to first/last split (player is first, next is last)
                    col_map["first"] = player_idx
                    col_map["last"] = next_idx
                    del col_map["player"]
                    # Only shift subsequent columns if data has more columns
                    # than the header anticipated (header-based detection only)
                    if header_width > 0 and typical_data_width > header_width:
                        for role in list(col_map.keys()):
                            if role in ("first", "last"):
                                continue
                            if col_map[role] > player_idx:
                                col_map[role] += 1
                else:
                    # Next column is NOT names — check previous column
                    prev_idx = player_idx - 1
                    if prev_idx >= 0 and prev_idx not in col_map.values():
                        sample_prev = [r[prev_idx].strip() for r in rows_raw[:10]
                                       if len(r) > prev_idx and r[prev_idx].strip()]
                        prev_is_names = (sample_prev and
                                         all(re.match(r'^[A-Za-z\-\'\. ]+$', n) for n in sample_prev))
                        if prev_is_names:
                            # player_idx won scoring (likely first names),
                            # prev column holds last names
                            col_map["first"] = player_idx
                            col_map["last"] = prev_idx
                            del col_map["player"]

    # Skip chip count tables (no prize data)
    if "chip_count" in col_map and "prize" not in col_map:
        return []

    # Skip points-only tables (standings, not results)
    if "points" in col_map and "prize" not in col_map:
        return []

    # Handle first/last name split
    has_split_name = "first" in col_map and "last" in col_map
    if has_split_name and "player" not in col_map:
        col_map["player"] = col_map["first"]  # placeholder for validation

    if not is_results_table(rows_raw, col_map):
        return []

    results = []
    for row in rows_raw:
        if len(row) < max(col_map.values(), default=0) + 1:
            continue

        place_str = row[col_map.get("place", 0)].strip() if "place" in col_map else ""

        if has_split_name:
            first_str = row[col_map["first"]].strip() if col_map["first"] < len(row) else ""
            last_str = row[col_map["last"]].strip() if col_map["last"] < len(row) else ""
            player_str = f"{first_str} {last_str}".strip()
        else:
            player_str = row[col_map.get("player", 1)].strip() if "player" in col_map else ""

        prize_str = row[col_map.get("prize", 2)].strip() if "prize" in col_map else ""

        # Validate place is a number
        if not re.match(r"^\d{1,3}$", place_str):
            continue

        # Skip empty player names
        if not player_str or len(player_str) < 2:
            continue

        # Skip entries where player name looks like a dollar amount
        if re.match(r"^\$[\d,]+\.?\d*$", player_str):
            continue

        results.append({
            "place": int(place_str),
            "player": player_str,
            "prize": parse_money(prize_str),
        })

    return results


def parse_metadata(soup: BeautifulSoup, title: str) -> dict:
    """Extract tournament metadata from the page."""
    # Get the article body text
    article = soup.select_one("article") or soup.select_one(".article") or soup
    text = article.get_text(" ", strip=True)

    metadata = {
        "name": title,
        "venue": "",
        "date": "",
        "buy_in": 0,
        "entrants": 0,
        "prizepool": 0,
        "places_paid": 0,
    }

    # Buy in
    buy_in_match = re.search(r"Buy\s*in:\s*\$?([\d,]+)", text)
    if buy_in_match:
        metadata["buy_in"] = parse_money(buy_in_match.group(1))

    # Number of entrants
    entrants_match = re.search(r"Number\s+of\s+Entrants:\s*([\d,]+)", text)
    if entrants_match:
        metadata["entrants"] = int(entrants_match.group(1).replace(",", ""))

    # Prizepool
    prizepool_match = re.search(r"Prizepool:\s*\$?([\d,]+)", text)
    if prizepool_match:
        metadata["prizepool"] = parse_money(prizepool_match.group(1))

    # Places paid
    places_match = re.search(r"Places\s+Paid:\s*(\d+)", text)
    if places_match:
        metadata["places_paid"] = int(places_match.group(1))

    # Date from the page
    date_match = re.search(
        r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s*\d{4})",
        text
    )
    if date_match:
        metadata["date"] = date_match.group(1).strip()

    # Start Date (some pages use this format)
    start_date_match = re.search(r"Start\s+Date:\s*(\d{2}/\d{2}/\d{4})", text)
    if start_date_match:
        metadata["date"] = start_date_match.group(1)

    # Venue from tags
    for tag_link in soup.select("a[href*='/tagged/']"):
        tag_text = tag_link.get_text(strip=True)
        if tag_text and "rgps" not in tag_text.lower():
            metadata["venue"] = tag_text
            break

    return metadata


def fetch_pdf_url(soup: BeautifulSoup) -> str | None:
    """Find a Shopify-hosted PDF link on the page."""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower() and "shopify" in href.lower():
            return href
    return None


def fetch_pdf_bytes(url: str) -> bytes:
    """Download a PDF, using cache."""
    cached = cache_path(url + ".pdf")
    if cached.exists():
        return cached.read_bytes()
    time.sleep(DELAY)
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    cached.write_bytes(resp.content)
    return resp.content


def parse_pdf_results(pdf_bytes: bytes) -> list[dict]:
    """Parse tournament results from a PDF file."""
    import pdfplumber
    import io

    pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    all_lines = []
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            all_lines.extend(text.strip().split("\n"))

    if not all_lines:
        return []

    results = []

    # Detect format from header/content
    header_line = ""
    for line in all_lines:
        if re.match(r"place\s", line, re.IGNORECASE):
            header_line = line.lower()
            break

    if "last name" in header_line and "first name" in header_line:
        # bestbet format: Place Last First Hometown State $Payout
        for line in all_lines:
            if re.search(r"entries|prize\s*pool|total", line, re.IGNORECASE):
                continue
            m = re.match(r"^(\d{1,3})\s+(.+?)\s+\$([0-9,]+)", line)
            if not m:
                continue
            place = int(m.group(1))
            middle = m.group(2).strip()
            prize = float(m.group(3).replace(",", ""))
            # Middle is "LastName FirstName City State"
            parts = middle.split()
            if len(parts) >= 2:
                # Handle suffixes: "Martinez jr Dimas" → last=Martinez, suffix=jr, first=Dimas
                idx = 0
                last_name = parts[idx]
                idx += 1
                suffix = ""
                if idx < len(parts) and parts[idx].lower() in ("jr", "sr", "ii", "iii", "iv"):
                    suffix = parts[idx]
                    idx += 1
                first_name = parts[idx] if idx < len(parts) else ""
                player = f"{first_name} {last_name}"
                if suffix:
                    player += f" {suffix}"
            else:
                player = middle
            results.append({"place": place, "player": player, "prize": prize})

    elif "player name" in header_line or "player#" in header_line:
        # Downstream format: Place [Player#] FULL_NAME ALIAS $Payout City ST
        for line in all_lines:
            if re.search(r"total\s*players|entries|prize\s*pool", line, re.IGNORECASE):
                continue
            # Clean OCR artifacts: leading digits stuck to names (e.g. "8ANDRE ALLEN")
            cleaned = re.sub(r"^(\d{1,3})\s*\d*([A-Z])", r"\1 \2", line)
            m = re.match(r"^(\d{1,3})\s+(.+?)\s+\$([0-9,]+)", cleaned)
            if not m:
                continue
            place = int(m.group(1))
            middle = m.group(2).strip()
            prize_str = m.group(3)
            # Clean prize of stuck-on city text (e.g. "$60,972KANSAS")
            prize = float(re.match(r"[0-9,]+", prize_str).group().replace(",", ""))
            # Middle is "FULL_NAME ALIAS" — name is first two words
            parts = middle.split()
            if len(parts) >= 2:
                player = f"{parts[0]} {parts[1]}"
            else:
                player = middle
            results.append({"place": place, "player": player.title(), "prize": prize})

    else:
        # Iowa format: Place $Amount Name  (or just "1 $8,360.00 Larry Aldag")
        for line in all_lines:
            m = re.match(r"^(\d{1,3})\s+\$([0-9,.]+)\s+(.+)$", line)
            if not m:
                continue
            place = int(m.group(1))
            prize = float(m.group(2).replace(",", ""))
            player = m.group(3).strip()
            results.append({"place": place, "player": player, "prize": prize})

    return results


def parse_tournament_page(url: str) -> list[dict]:
    """Parse a single tournament page. Returns a list of tournaments (usually 1, sometimes multiple)."""
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    # Get page title
    title_el = soup.select_one("h1")
    title = title_el.get_text(strip=True) if title_el else ""

    if not title:
        return []

    # Check if we should skip this page
    if should_skip(title):
        return []

    # Find all tables
    tables = soup.find_all("table")
    if not tables:
        # Fallback: check for PDF link
        pdf_url = fetch_pdf_url(soup)
        if pdf_url:
            try:
                pdf_bytes = fetch_pdf_bytes(pdf_url)
                results = parse_pdf_results(pdf_bytes)
                if results:
                    metadata = parse_metadata(soup, title)
                    tournament_id = hashlib.md5(url.encode()).hexdigest()[:12]
                    return [{
                        "tournament_id": tournament_id,
                        "url": url,
                        **metadata,
                        "results": results,
                    }]
            except Exception:
                pass
        return []

    metadata = parse_metadata(soup, title)
    tournaments = []

    for i, table in enumerate(tables):
        results = parse_results_table(table)
        if not results:
            continue

        # Skip tables that look like casino champ point standings
        # Detect by: extra "points" column (4th col with small integers)
        tbody = table.find("tbody") or table
        raw_rows = []
        for tr in tbody.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if cells:
                raw_rows.append(cells)
        is_standings = False
        if raw_rows:
            num_cols = max(len(r) for r in raw_rows)
            if num_cols >= 4:
                col_map = detect_columns_from_data(raw_rows, num_cols)
                mapped_cols = set(col_map.values())
                for c in range(num_cols):
                    if c in mapped_cols:
                        continue
                    vals = [r[c].strip("' \t") for r in raw_rows[:20] if len(r) > c and r[c].strip()]
                    if vals and all(re.match(r"^\d{1,3}(\.\d+)?$", v) for v in vals):
                        is_standings = True
                        break
        if is_standings:
            continue

        if results and results[0]["place"] == 1:
            # Check if places are generally sequential (tournament results)
            places = [r["place"] for r in results]
            if places == sorted(places):
                tournament_id = hashlib.md5(f"{url}_{i}".encode()).hexdigest()[:12]
                tournament = {
                    "tournament_id": tournament_id,
                    "url": url,
                    **metadata,
                    "results": results,
                }
                if i > 0:
                    # Multiple tournaments on same page - try to find sub-heading
                    tournament["name"] = f"{title} (Table {i + 1})"
                tournaments.append(tournament)

    return tournaments


def scrape_all():
    """Main entry point: scrape all tournament results."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Step 1: Get all tournament URLs
    print("Step 1: Crawling index pages...")
    urls = get_tournament_urls()

    # Step 2: Parse each tournament page
    print(f"\nStep 2: Parsing {len(urls)} tournament pages...")
    all_tournaments = []
    skipped = 0
    errors = 0

    for url in tqdm(urls, desc="Parsing tournaments"):
        try:
            tournaments = parse_tournament_page(url)
            if tournaments:
                all_tournaments.extend(tournaments)
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            tqdm.write(f"  Error parsing {url}: {e}")

    # Step 3: Deduplicate tournaments (some events have both a story post and a results post)
    # Load name overrides for signature normalization
    overrides_file = Path("data/name_overrides.json")
    overrides = {}
    if overrides_file.exists():
        overrides = json.loads(overrides_file.read_text(encoding="utf-8"))

    def _normalize_name(name):
        """Normalize a player name for dedup: apply overrides then lowercase."""
        n = overrides.get(name, name)
        return n.lower()

    seen_sigs = {}
    deduped = []
    dupes_removed = 0
    for t in all_tournaments:
        if len(t["results"]) >= 3:
            sig = tuple((_normalize_name(r["player"]), r["prize"]) for r in t["results"][:3])
        else:
            sig = (t.get("url", ""),)  # unique fallback

        if sig in seen_sigs:
            # Keep the one with more results
            existing = seen_sigs[sig]
            if len(t["results"]) > len(existing["results"]):
                deduped.remove(existing)
                deduped.append(t)
                seen_sigs[sig] = t
            dupes_removed += 1
        else:
            seen_sigs[sig] = t
            deduped.append(t)

    all_tournaments = deduped

    # Step 4: Apply manual result corrections (website data entry errors)
    corrections_file = Path("data/result_corrections.json")
    corrections_applied = 0
    if corrections_file.exists():
        corrections = json.loads(corrections_file.read_text(encoding="utf-8"))
        for corr in corrections:
            match = corr["match"]
            for t in all_tournaments:
                name_ok = match.get("name_contains", "") in t.get("name", "")
                date_ok = match.get("date", "") == t.get("date", "")
                if name_ok and date_ok:
                    # Capture first-place prize before any modifications
                    first_prize = t["results"][0]["prize"] if t["results"] else 0
                    # Remove player if specified
                    if "remove_player" in corr:
                        t["results"] = [r for r in t["results"] if r["player"] != corr["remove_player"]]
                    # Set first place if specified
                    if "set_first_place" in corr:
                        winner_name = corr["set_first_place"]
                        # Check if winner is already in results (just needs place fix)
                        found = False
                        for r in t["results"]:
                            if r["player"] == winner_name:
                                r["place"] = 1
                                found = True
                                break
                        if not found:
                            # Insert winner at place 1 with the original first-place prize
                            t["results"].insert(0, {"place": 1, "player": winner_name, "prize": first_prize})
                    corrections_applied += 1
                    break

    # Step 5: Save results
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_tournaments, f, indent=2, ensure_ascii=False)

    # Summary
    total_players = sum(len(t["results"]) for t in all_tournaments)
    print(f"\n{'='*50}")
    print(f"Scraping complete!")
    print(f"  Tournaments parsed: {len(all_tournaments)}")
    print(f"  Duplicates removed: {dupes_removed}")
    if corrections_applied:
        print(f"  Corrections applied: {corrections_applied}")
    print(f"  Total player results: {total_players}")
    print(f"  Pages skipped: {skipped}")
    print(f"  Errors: {errors}")
    print(f"  Output: {OUTPUT_FILE}")
    print(f"{'='*50}")


if __name__ == "__main__":
    scrape_all()
