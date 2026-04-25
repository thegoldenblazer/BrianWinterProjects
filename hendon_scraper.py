"""
Hendon Mob Scraper (local only)
================================
Hendon Mob (pokerdb.thehendonmob.com) is protected by Cloudflare's
JavaScript challenge â€” basic HTTP scrapers (requests, cloudscraper)
get a 403. This script uses Playwright with a real Chromium browser
to pass the challenge, then scrapes career cashes / total live earnings.

Results are cached in ``data/hendon_cache.json`` so the deployed Flask
site (which cannot run a browser on Vercel) just reads the JSON.

Usage
-----
    # First-time setup
    pip install playwright
    playwright install chromium

    # Scrape every player in a GES_PMS-style CSV that isn't cached yet
    python hendon_scraper.py --csv "GES_PMS_Tournament_Past_Entries_xxx.csv"

    # Scrape specific names
    python hendon_scraper.py --names "Daniel Negreanu" "Phil Ivey"

    # Force refresh (ignore cache)
    python hendon_scraper.py --csv file.csv --refresh

    # Run headless (default opens a visible window for the first run so
    # Cloudflare's challenge can solve cleanly; switch to headless once
    # cookies are warm)
    python hendon_scraper.py --csv file.csv --headless
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "data" / "hendon_cache.json"
IGNORE_FILE = ROOT / "data" / "hendon_ignore.json"
PROFILE_DIR = ROOT / "data" / "browser_profile"  # persistent Cloudflare cookies
DEBUG_DIR = ROOT / "data" / "hendon_debug"

# Search uses the MAIN domain â€” results have div.db-gallery__item with
# anchor tags whose href points to player.php?a=r&n=PLAYER_ID
SEARCH_URL = "https://www.thehendonmob.com/search/?q={query}"
PROFILE_URL = "https://pokerdb.thehendonmob.com/player.php?a=r&n={pid}"


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------
def _norm_key(first: str, last: str) -> str:
    """Normalize a (first, last) pair into a stable cache key."""
    def _clean(s: str) -> str:
        s = unicodedata.normalize("NFKD", s or "")
        s = s.encode("ascii", "ignore").decode("ascii")
        s = re.sub(r"[^a-zA-Z]", "", s).lower()
        return s
    return f"{_clean(last)}|{_clean(first)}"


def load_cache() -> dict:
    if CACHE_FILE.exists():
        with CACHE_FILE.open("r", encoding="utf-8-sig") as f:
            return json.load(f)
    return {"version": 1, "players": {}}


def save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False, sort_keys=True)


def lookup(cache: dict, first: str, last: str) -> dict | None:
    return cache.get("players", {}).get(_norm_key(first, last))


def load_ignore_keys() -> set[str]:
    """Load the set of normalized 'last|first' keys to never look up."""
    if not IGNORE_FILE.exists():
        return set()
    try:
        with IGNORE_FILE.open("r", encoding="utf-8-sig") as f:
            data = json.load(f)
        return set(data.get("keys", []))
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# CSV parsing (GES_PMS style)
# ---------------------------------------------------------------------------
def parse_ges_pms_csv(path: Path) -> list[tuple[str, str]]:
    """Return a list of (first, last) tuples from a GES_PMS export.

    The first row is a free-text title; the second row is the header.
    """
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        all_rows = list(reader)

    if not all_rows:
        return rows

    # Find the header row by looking for "Last name" / "First name"
    header_idx = 0
    for i, row in enumerate(all_rows[:5]):
        joined = ",".join(c.strip().lower() for c in row)
        if "last name" in joined and "first name" in joined:
            header_idx = i
            break

    header = [c.strip().lower() for c in all_rows[header_idx]]
    try:
        last_idx = header.index("last name")
        first_idx = header.index("first name")
    except ValueError:
        # Fall back to assuming columns 0=last, 1=first
        last_idx, first_idx = 0, 1

    for row in all_rows[header_idx + 1:]:
        if len(row) <= max(last_idx, first_idx):
            continue
        last = row[last_idx].strip()
        first = row[first_idx].strip()
        if first and last:
            rows.append((first, last))
    return rows


# ---------------------------------------------------------------------------
# Playwright scraping
# ---------------------------------------------------------------------------
def _parse_money(text: str) -> int | None:
    if not text:
        return None
    cleaned = re.sub(r"[^\d]", "", text)
    return int(cleaned) if cleaned else None


def _is_cloudflare(page) -> bool:
    try:
        title = (page.title() or "").lower()
    except Exception:
        # Page is mid-navigation; treat as still-challenged so caller waits.
        return True
    return ("just a moment" in title
            or "attention required" in title
            or "verifying you are human" in title
            or "checking your browser" in title)


def _wait_through_cloudflare(page, max_seconds: int = 60) -> bool:
    """Poll the page until Cloudflare is gone (or we time out)."""
    for _ in range(max_seconds):
        try:
            if not _is_cloudflare(page):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _scrape_player(page, first: str, last: str, *, debug: bool = False) -> dict | None:
    """Search Hendon Mob (main domain) for player ID, then fetch profile.

    Search page: https://www.thehendonmob.com/search/?q=...
      â†’ results in <div class="db-gallery__item"> containing
        <a href="...player.php?a=r&n=PLAYER_ID">Name</a>
    Profile:     https://pokerdb.thehendonmob.com/player.php?a=r&n=PLAYER_ID
    """
    query = f"{first} {last}".strip()
    page.goto(SEARCH_URL.format(query=query.replace(" ", "+")),
              wait_until="domcontentloaded", timeout=60000)
    _wait_through_cloudflare(page, 60)

    # Wait briefly for results, but don't fail if absent (player may not exist)
    try:
        page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass

    html = page.content()

    # Extract every player ID on the search page in DOM order.
    ids = re.findall(r"player\.php\?[^\"'>]*?n=(\d+)", html)
    if not ids:
        if debug:
            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            slug = re.sub(r"[^a-zA-Z0-9]+", "_", f"{last}_{first}")[:60]
            (DEBUG_DIR / f"search_{slug}.html").write_text(html, encoding="utf-8")
        return None

    # Take the first ID and prefer one with a matching name in the link text.
    player_id = ids[0]
    matched_name = f"{first} {last}"
    try:
        link_loc = page.locator(f"a[href*='n={player_id}']").first
        if link_loc.count():
            txt = (link_loc.inner_text() or "").strip()
            if txt:
                matched_name = txt
    except Exception:
        pass

    profile_url = PROFILE_URL.format(pid=player_id)
    page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
    _wait_through_cloudflare(page, 60)

    body_text = page.inner_text("body")

    # Parse Total Earnings â€” Hendon Mob shows "Total Earnings: $X,XXX" near the top.
    earnings = None
    for pattern in (
        r"Total\s+Earnings[^\$]*\$\s*([\d,]+)",
        r"Total\s+Live\s+Earnings[^\$]*\$\s*([\d,]+)",
        r"All[-\s]?Time\s+Money\s+List[^\$]*\$\s*([\d,]+)",
        r"Career\s+Earnings[^\$]*\$\s*([\d,]+)",
    ):
        m = re.search(pattern, body_text, re.IGNORECASE)
        if m:
            earnings = _parse_money(m.group(1))
            break
    if earnings is None:
        # Fallback: largest plausible dollar amount on the page
        candidates = []
        for raw in re.findall(r"\$\s*([\d,]{4,})", body_text):
            v = _parse_money(raw)
            if v and 100 <= v <= 100_000_000:
                candidates.append(v)
        if candidates:
            earnings = max(candidates)

    # Cashes: count rows in the results table (each row with a $ buy-in)
    cashes = None
    for label in (r"Number\s+of\s+Cashes", r"Total\s+Cashes"):
        m = re.search(rf"{label}\s*[:\-]?\s*(\d[\d,]*)", body_text, re.IGNORECASE)
        if m:
            cashes = int(m.group(1).replace(",", ""))
            break

    if debug and earnings is None:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", f"{last}_{first}")[:60]
        (DEBUG_DIR / f"profile_{slug}.html").write_text(
            page.content(), encoding="utf-8")

    return {
        "matched_name": matched_name or f"{first} {last}",
        "player_id": player_id,
        "profile_url": profile_url,
        "total_earnings": earnings,
        "cashes": cashes,
        "fetched_at": _now_iso(),
    }


def scrape_players(
    players: Iterable[tuple[str, str]],
    *,
    headless: bool = False,
    refresh: bool = False,
    delay: float = 2.0,
    debug: bool = False,
    warmup: bool = True,
) -> None:
    # Try patchright first (stealth Playwright fork that bypasses Cloudflare bot
    # detection), then fall back to vanilla playwright.
    try:
        from patchright.sync_api import sync_playwright  # type: ignore
        flavor = "patchright"
    except ImportError:
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
            flavor = "playwright"
        except ImportError:
            print("ERROR: neither patchright nor playwright is installed.", file=sys.stderr)
            print("  pip install patchright && python -m patchright install chromium", file=sys.stderr)
            sys.exit(1)

    cache = load_cache()
    cache.setdefault("players", {})
    ignore_keys = load_ignore_keys()

    # Drop any cached entries that are now on the ignore list (e.g. a known
    # bad-match was added after the cache was built).
    removed = [k for k in list(cache["players"]) if k in ignore_keys]
    for k in removed:
        del cache["players"][k]
    if removed:
        print(f"Removed {len(removed)} cached entry/entries on ignore list: {removed}")
        save_cache(cache)

    todo = []
    for first, last in players:
        key = _norm_key(first, last)
        if key in ignore_keys:
            continue
        if not refresh and key in cache["players"]:
            rec = cache["players"][key]
            # Re-try previous "not found" / error entries automatically
            if rec.get("total_earnings") is not None or rec.get("cashes") is not None:
                continue
        todo.append((first, last, key))

    if not todo:
        print("All players already cached with data. Use --refresh to force a re-scrape.")
        return

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Scraping {len(todo)} player(s) from Hendon Mob (using {flavor})...")
    print(f"Browser profile: {PROFILE_DIR}  (cookies persist across runs)")

    with sync_playwright() as p:
        # Patchright's recommended config: no extra args, no custom user-agent
        # (it sets stealth-friendly defaults automatically). For vanilla
        # playwright we still nudge it with the AutomationControlled flag.
        ctx_kwargs = dict(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            viewport={"width": 1280, "height": 900},
            channel="chrome",  # use installed Chrome if available (less detectable)
            no_viewport=False,
        )
        if flavor == "playwright":
            ctx_kwargs["args"] = ["--disable-blink-features=AutomationControlled"]
            ctx_kwargs["user_agent"] = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )

        try:
            context = p.chromium.launch_persistent_context(**ctx_kwargs)
        except Exception:
            # Fall back without channel="chrome" if installed Chrome isn't
            # locatable — patchright's bundled chromium still works.
            ctx_kwargs.pop("channel", None)
            context = p.chromium.launch_persistent_context(**ctx_kwargs)
        page = context.pages[0] if context.pages else context.new_page()

        # Warmup: visit BOTH domains (search uses www, profile uses pokerdb)
        # so Cloudflare cookies are cached for both before scraping starts.
        if warmup:
            print("\nWarmup: visiting Hendon Mob domains.")
            print("If you see a Cloudflare challenge, solve it now")
            print("(cookies will persist for all subsequent searches).")
            for warmup_url in ("https://www.thehendonmob.com/",
                               "https://pokerdb.thehendonmob.com/"):
                page.goto(warmup_url, wait_until="domcontentloaded", timeout=60000)
                if _is_cloudflare(page):
                    print(f"  CF challenge on {warmup_url} \u2014 you have up to 120s.")
                    _wait_through_cloudflare(page, 120)
            print("Warmup complete. Starting scrape.\n")

        for i, (first, last, key) in enumerate(todo, 1):
            print(f"  [{i}/{len(todo)}] {first} {last} ... ", end="", flush=True)
            try:
                result = _scrape_player(page, first, last, debug=debug)
            except Exception as exc:  # noqa: BLE001
                print(f"FAILED ({exc!s})")
                cache["players"][key] = {
                    "matched_name": None,
                    "profile_url": None,
                    "total_earnings": None,
                    "cashes": None,
                    "error": str(exc),
                    "fetched_at": _now_iso(),
                }
                save_cache(cache)
                time.sleep(delay)
                continue

            if not result:
                print("not found")
                cache["players"][key] = {
                    "matched_name": None,
                    "profile_url": None,
                    "total_earnings": None,
                    "cashes": None,
                    "fetched_at": _now_iso(),
                }
            else:
                earn = result.get("total_earnings")
                print(f"${earn:,}" if earn else "no earnings parsed")
                cache["players"][key] = result

            save_cache(cache)  # save after each â€” robust to crashes
            time.sleep(delay)

        context.close()

    print(f"Done. Cache: {CACHE_FILE}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape Hendon Mob earnings into a local cache.")
    ap.add_argument("--csv", type=Path, help="GES_PMS-style CSV with First/Last name columns.")
    ap.add_argument("--names", nargs="*", help='Free-form names like "Daniel Negreanu".')
    ap.add_argument("--headless", action="store_true", help="Run browser headless.")
    ap.add_argument("--refresh", action="store_true", help="Re-scrape even if cached.")
    ap.add_argument("--delay", type=float, default=2.0, help="Seconds between players.")
    ap.add_argument("--debug", action="store_true",
                    help="Save HTML of pages where scraping fails (data/hendon_debug/).")
    ap.add_argument("--no-warmup", action="store_true",
                    help="Skip the homepage warmup step (use after the first successful run).")
    args = ap.parse_args()

    players: list[tuple[str, str]] = []
    if args.csv:
        players.extend(parse_ges_pms_csv(args.csv))
    if args.names:
        for n in args.names:
            parts = n.strip().split(None, 1)
            if len(parts) == 2:
                players.append((parts[0], parts[1]))
            elif len(parts) == 1:
                players.append((parts[0], ""))

    if not players:
        ap.error("Provide --csv and/or --names.")

    scrape_players(players, headless=args.headless, refresh=args.refresh,
                   delay=args.delay, debug=args.debug, warmup=not args.no_warmup)


if __name__ == "__main__":
    main()

