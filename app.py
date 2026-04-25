"""
RunGood Poker Series - TrueSkill Leaderboard Web App
Flask application serving player rankings and profiles.
"""

import bisect
import csv
import io
import json
import os
import re
import statistics
import unicodedata
from pathlib import Path
from urllib.parse import unquote

from flask import Flask, render_template, request, jsonify
from thefuzz import fuzz

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5 MB upload cap

_ROOT = Path(__file__).resolve().parent
RATINGS_FILE = _ROOT / "data" / "ratings.json"
HENDON_CACHE_FILE = _ROOT / "data" / "hendon_cache.json"
HENDON_IGNORE_FILE = _ROOT / "data" / "hendon_ignore.json"


def _parse_display_date(raw):
    """Normalize any date string to 'Mon DD, YYYY' format."""
    from datetime import datetime as _dt
    for fmt in ("%m/%d/%Y", "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            return _dt.strptime(raw.strip(), fmt).strftime("%b %d, %Y")
        except (ValueError, AttributeError):
            continue
    return raw  # fallback: return as-is


@app.template_filter("fmtdate")
def fmtdate_filter(value):
    """Jinja filter: {{ some_date | fmtdate }}"""
    return _parse_display_date(value)


@app.template_filter("elo")
def elo_filter(value):
    """Jinja filter: {{ rating | elo }} — multiplies by 100 and rounds."""
    try:
        return "{:,.0f}".format(float(value) * 100)
    except (ValueError, TypeError):
        return value

# Load ratings data at startup
_data = None


def get_data():
    global _data
    if _data is None:
        with open(RATINGS_FILE, "r", encoding="utf-8") as f:
            _data = json.load(f)
    return _data


def reload_data():
    global _data
    _data = None
    return get_data()


@app.route("/")
def leaderboard():
    data = get_data()
    min_events = request.args.get("min_events", 3, type=int)
    sort_by = request.args.get("sort", "conservative_rating")
    order = request.args.get("order", "desc")
    search = request.args.get("q", "").strip().lower()
    page = request.args.get("page", 1, type=int)
    per_page = 50

    players = data["players"]

    # Filter by minimum events
    players = [p for p in players if p["tournaments_played"] >= min_events]

    # Search filter
    if search:
        players = [p for p in players if search in p["player"].lower()]

    # Sort
    valid_sorts = [
        "conservative_rating", "tournaments_played", "wins", "player",
        "total_winnings",
    ]
    if sort_by not in valid_sorts:
        sort_by = "conservative_rating"

    reverse = order == "desc"
    if sort_by == "player":
        reverse = not reverse  # alphabetical default is ascending

    players.sort(key=lambda p: p.get(sort_by, 0), reverse=reverse)

    # Re-rank after filtering
    for i, p in enumerate(players):
        p["display_rank"] = i + 1

    # Compute aggregate stats from filtered list
    max_rating = max((p["conservative_rating"] for p in players), default=0)
    total_winnings_sum = sum(p.get("total_winnings", 0) for p in players)

    # Pagination
    total = len(players)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_players = players[start:start + per_page]

    return render_template(
        "leaderboard.html",
        players=page_players,
        total_players=total,
        total_tournaments=data["total_tournaments"],
        total_winnings=total_winnings_sum,
        top_rating=round(max_rating * 100),
        max_rating=max_rating * 100,
        generated_at=data["generated_at"],
        min_events=min_events,
        sort_by=sort_by,
        order=order,
        search=search,
        page=page,
        total_pages=total_pages,
    )


@app.route("/player/<path:name>")
def player_profile(name):
    data = get_data()
    name = unquote(name)

    player = None
    for p in data["players"]:
        if p["player"] == name:
            player = p
            break

    if not player:
        # Fuzzy-match suggestions
        suggestions = []
        for p in data["players"]:
            score = fuzz.partial_ratio(name.lower(), p["player"].lower())
            if score >= 60:
                suggestions.append((score, p["player"]))
        suggestions.sort(key=lambda x: x[0], reverse=True)
        suggestion_names = [s[1] for s in suggestions[:6]]
        return render_template("404.html", name=name, suggestions=suggestion_names), 404

    return render_template(
        "player.html",
        player=player,
        player_history=player.get("history", []),
    )


@app.route("/api/search")
def api_search():
    data = get_data()
    q = request.args.get("q", "").strip().lower()
    if len(q) < 2:
        return jsonify([])

    matches = []
    for p in data["players"]:
        if q in p["player"].lower():
            matches.append({
                "name": p["player"],
                "rating": round(p["conservative_rating"] * 100),
                "cashes": p["tournaments_played"],
            })
            if len(matches) >= 10:
                break

    return jsonify(matches)


@app.route("/compare")
def compare():
    data = get_data()
    names = request.args.getlist("players")
    # Limit to 4 players
    names = [unquote(n) for n in names[:4]]

    players = []
    for name in names:
        for p in data["players"]:
            if p["player"] == name:
                players.append(p)
                break

    histories = {}
    for p in players:
        histories[p["player"]] = p.get("history", [])

    return render_template(
        "compare.html",
        players=players,
        histories=histories,
    )


@app.route("/table-scout")
def table_scout():
    return render_template("table_scout.html")


@app.route("/api/fuzzy-match")
def api_fuzzy_match():
    """Return close matches for a player name, including exact and 1-letter-off suggestions."""
    data = get_data()
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"exact": None, "suggestions": []})

    q_lower = q.lower()

    # Check for exact match first
    for p in data["players"]:
        if p["player"].lower() == q_lower:
            return jsonify({
                "exact": {
                    "name": p["player"],
                    "rating": round(p["conservative_rating"] * 100),
                    "cashes": p["tournaments_played"],
                },
                "suggestions": [],
            })

    # Find close matches using fuzzy matching
    candidates = []
    for p in data["players"]:
        score = fuzz.ratio(q_lower, p["player"].lower())
        if score >= 82:
            candidates.append((score, p))

    candidates.sort(key=lambda x: x[0], reverse=True)
    suggestions = [
        {
            "name": c[1]["player"],
            "rating": round(c[1]["conservative_rating"] * 100),
            "cashes": c[1]["tournaments_played"],
            "score": c[0],
        }
        for c in candidates[:5]
    ]

    return jsonify({"exact": None, "suggestions": suggestions})


@app.route("/api/table-scout", methods=["POST"])
def api_table_scout():
    """Compute table difficulty rating and return player info for up to 8 players."""
    data = get_data()
    body = request.get_json(silent=True) or {}
    names = body.get("players", [])

    if not names or not isinstance(names, list):
        return jsonify({"error": "Provide a list of player names"}), 400

    names = [n.strip() for n in names[:8] if isinstance(n, str) and n.strip()]
    if not names:
        return jsonify({"error": "Provide at least one player name"}), 400

    # Compute median rating for unknown players
    all_ratings = [p["conservative_rating"] for p in data["players"]]
    median_rating = statistics.median(all_ratings)

    # Build player lookup (case-insensitive)
    player_lookup = {}
    for p in data["players"]:
        player_lookup[p["player"].lower()] = p

    results = []
    table_ratings = []

    for name in names:
        name_lower = name.lower()
        player_data = player_lookup.get(name_lower)

        if player_data:
            rating = player_data["conservative_rating"]
            table_ratings.append(rating)
            results.append({
                "name": player_data["player"],
                "known": True,
                "rating": round(rating * 100),
                "mu": round(player_data["mu"] * 100),
                "sigma": round(player_data["sigma"] * 100),
                "rank": player_data["rank"],
                "tournaments_played": player_data["tournaments_played"],
                "wins": player_data["wins"],
                "total_winnings": player_data["total_winnings"],
                "best_finish": player_data["best_finish"],
                "last_active": player_data["last_active"],
                "history": player_data.get("history", []),
            })
        else:
            table_ratings.append(median_rating)
            results.append({
                "name": name,
                "known": False,
                "rating": round(median_rating * 100),
            })

    # Compute table difficulty (0-100 scale) using percentile-based scoring.
    # Each player's rating is mapped to their percentile in the full database,
    # then the table difficulty is the average percentile across all seats.
    sorted_ratings = sorted(all_ratings)
    total_players = len(sorted_ratings)

    def rating_to_percentile(r):
        # bisect to find where this rating falls in the sorted list
        pos = bisect.bisect_left(sorted_ratings, r)
        return (pos / total_players) * 100 if total_players else 50

    avg_table_rating = statistics.mean(table_ratings)
    table_percentiles = [rating_to_percentile(r) for r in table_ratings]
    difficulty = round(statistics.mean(table_percentiles), 1)
    difficulty = max(0, min(100, difficulty))

    return jsonify({
        "players": results,
        "table_difficulty": difficulty,
        "avg_rating": round(avg_table_rating * 100),
        "median_db_rating": round(median_rating * 100),
    })


# ---------------------------------------------------------------------------
# Hendon Mob cash-sort feature
# ---------------------------------------------------------------------------
_hendon_cache = None
_hendon_ignore = None


def _load_hendon_ignore() -> set:
    global _hendon_ignore
    if _hendon_ignore is None:
        try:
            with open(HENDON_IGNORE_FILE, "r", encoding="utf-8-sig") as f:
                _hendon_ignore = set(json.load(f).get("keys", []))
        except (FileNotFoundError, json.JSONDecodeError):
            _hendon_ignore = set()
    return _hendon_ignore


def _load_hendon_cache():
    global _hendon_cache
    if _hendon_cache is None:
        try:
            with open(HENDON_CACHE_FILE, "r", encoding="utf-8-sig") as f:
                _hendon_cache = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            _hendon_cache = {"version": 1, "players": {}}
        # Drop any cached entries that are on the ignore list.
        ignore = _load_hendon_ignore()
        if ignore:
            players = _hendon_cache.get("players", {})
            for k in list(players):
                if k in ignore:
                    players.pop(k, None)
    return _hendon_cache


def _hendon_key(first: str, last: str) -> str:
    def _clean(s: str) -> str:
        s = unicodedata.normalize("NFKD", s or "")
        s = s.encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-zA-Z]", "", s).lower()
    return f"{_clean(last)}|{_clean(first)}"


def _parse_uploaded_csv(text: str) -> list[dict]:
    """Parse a GES_PMS-style CSV. Returns list of {first,last} dicts in order."""
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return []

    # Find header row containing "Last name" / "First name"
    header_idx = 0
    for i, row in enumerate(rows[:5]):
        joined = ",".join(c.strip().lower() for c in row)
        if "last name" in joined and "first name" in joined:
            header_idx = i
            break

    header = [c.strip().lower() for c in rows[header_idx]]
    try:
        last_idx = header.index("last name")
        first_idx = header.index("first name")
    except ValueError:
        last_idx, first_idx = 0, 1

    out = []
    seen = set()
    ignore = _load_hendon_ignore()
    for row in rows[header_idx + 1:]:
        if len(row) <= max(last_idx, first_idx):
            continue
        last = row[last_idx].strip()
        first = row[first_idx].strip()
        if not (first and last):
            continue
        key = _hendon_key(first, last)
        if key in seen or key in ignore:
            continue
        seen.add(key)
        out.append({"first": first, "last": last, "key": key})
    return out


@app.route("/cash-sort", methods=["GET"])
def cash_sort_page():
    return render_template("cash_sort.html")


@app.route("/api/cash-sort", methods=["POST"])
def api_cash_sort():
    """Receive a CSV upload, sort by Hendon Mob total live earnings (desc)."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded (field 'file' missing)."}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename."}), 400

    try:
        raw = f.read().decode("utf-8-sig", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": f"Could not read file: {exc}"}), 400

    parsed = _parse_uploaded_csv(raw)
    if not parsed:
        return jsonify({"error": "No player rows found. Expected GES_PMS CSV with 'Last name' / 'First name' columns."}), 400

    cache = _load_hendon_cache().get("players", {})

    ranked, missing = [], []
    for entry in parsed:
        rec = cache.get(entry["key"])
        if rec and rec.get("total_earnings") is not None:
            ranked.append({
                "first": entry["first"],
                "last": entry["last"],
                "matched_name": rec.get("matched_name") or f"{entry['first']} {entry['last']}",
                "profile_url": rec.get("profile_url"),
                "total_earnings": rec.get("total_earnings"),
                "cashes": rec.get("cashes"),
            })
        else:
            missing.append({
                "first": entry["first"],
                "last": entry["last"],
                "in_cache": rec is not None,
            })

    ranked.sort(key=lambda p: p["total_earnings"] or 0, reverse=True)
    for i, p in enumerate(ranked, 1):
        p["rank"] = i

    return jsonify({
        "filename": f.filename,
        "total_entrants": len(parsed),
        "ranked_count": len(ranked),
        "missing_count": len(missing),
        "ranked": ranked,
        "missing": missing,
    })


if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG", "false").lower() == "true", port=5000)
