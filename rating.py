"""
RunGood Poker Series - TrueSkill Rating Engine
Processes tournament results chronologically and computes Bayesian skill ratings.
"""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import trueskill

RESOLVED_FILE = Path("data/tournaments_resolved.json")
RATINGS_FILE = Path("data/ratings.json")

# TrueSkill environment settings
# mu=25, sigma=8.33 (25/3), beta=4.17 (25/6), tau=0.083 (25/300)
# draw_probability=0 since poker tournaments have clear ordinal finishes
ENV = trueskill.TrueSkill(
    mu=25.0,
    sigma=25.0 / 3,
    beta=25.0 / 6,
    tau=25.0 / 300,
    draw_probability=0.0,
)
ENV.make_as_global()


def parse_date(date_str: str) -> datetime:
    """Parse date strings from tournament data."""
    if not date_str:
        return datetime.min

    # Try MM/DD/YYYY format
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except ValueError:
        pass

    # Try "Mon DD, YYYY" format
    try:
        return datetime.strptime(date_str.strip(), "%b %d, %Y")
    except ValueError:
        pass

    # Try other common formats
    for fmt in ("%B %d, %Y", "%m-%d-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue

    return datetime.min


def compute_ratings():
    """Main entry point: compute TrueSkill ratings from resolved tournament data."""
    with open(RESOLVED_FILE, "r", encoding="utf-8") as f:
        tournaments = json.load(f)

    print(f"Loaded {len(tournaments)} tournaments")

    # Sort tournaments chronologically (oldest first)
    tournaments.sort(key=lambda t: parse_date(t.get("date", "")))

    # Player state tracking
    player_ratings = {}  # name -> trueskill.Rating
    player_history = defaultdict(list)  # name -> [{date, mu, sigma, tournament, place}]
    player_stats = defaultdict(lambda: {
        "tournaments_played": 0,
        "total_winnings": 0.0,
        "best_finish": 999,
        "wins": 0,
        "last_active": "",
    })

    processed = 0
    skipped = 0

    for tournament in tournaments:
        results = tournament.get("results", [])
        if len(results) < 2:
            skipped += 1
            continue

        date_str = tournament.get("date", "")
        name = tournament.get("name", "Unknown")
        entrants = tournament.get("entrants", 0)
        num_cashed = len(results)

        # Estimate field size if not available (~15% cash rate)
        if not entrants or entrants < num_cashed:
            entrants = max(round(num_cashed / 0.15), num_cashed + 1)
        non_cashers = entrants - num_cashed

        # Build the rating groups for TrueSkill
        # Each "team" is a single player; ranking is by finish position
        rating_groups = []
        ranks = []
        players_in_event = []

        for r in results:
            player_name = r["player"]
            place = r["place"]

            # Get or create player rating
            if player_name not in player_ratings:
                player_ratings[player_name] = ENV.create_rating()

            rating_groups.append((player_ratings[player_name],))
            ranks.append(place - 1)  # TrueSkill expects 0-indexed ranks
            players_in_event.append(player_name)

            # Update stats
            stats = player_stats[player_name]
            stats["tournaments_played"] += 1
            stats["total_winnings"] += r.get("prize", 0)
            stats["best_finish"] = min(stats["best_finish"], place)
            if place == 1:
                stats["wins"] += 1
            stats["last_active"] = date_str

        # Add phantom players representing non-cashers
        # They share the rank just below the last casher (all tied for last)
        # This ensures sigma reduction reflects the full field size,
        # not just the ~12.5% who cashed
        last_rank = max(ranks) + 1 if ranks else num_cashed
        for _ in range(non_cashers):
            rating_groups.append((ENV.create_rating(),))
            ranks.append(last_rank)
            players_in_event.append(None)  # phantom marker

        # Run TrueSkill rating update
        try:
            new_ratings = ENV.rate(rating_groups, ranks=ranks)
        except Exception as e:
            print(f"  TrueSkill error on '{name}': {e}")
            skipped += 1
            continue

        # Apply updated ratings (skip phantoms)
        for i, player_name in enumerate(players_in_event):
            if player_name is None:
                continue  # phantom player
            new_rating = new_ratings[i][0]
            old_rating = player_ratings[player_name]

            player_ratings[player_name] = new_rating

            # Record history
            player_history[player_name].append({
                "date": date_str,
                "mu": round(new_rating.mu, 4),
                "sigma": round(new_rating.sigma, 4),
                "conservative": round(new_rating.mu - 3 * new_rating.sigma, 4),
                "tournament": name,
                "place": results[i]["place"],
                "prize": results[i].get("prize", 0),
                "mu_change": round(new_rating.mu - old_rating.mu, 4),
            })

        processed += 1

    print(f"Processed {processed} tournaments ({skipped} skipped)")
    print(f"Rated {len(player_ratings)} players")

    # Build final output
    leaderboard = []
    for player_name, rating in player_ratings.items():
        stats = player_stats[player_name]
        conservative = rating.mu - 3 * rating.sigma
        leaderboard.append({
            "player": player_name,
            "mu": round(rating.mu, 4),
            "sigma": round(rating.sigma, 4),
            "conservative_rating": round(conservative, 4),
            "tournaments_played": stats["tournaments_played"],
            "total_winnings": round(stats["total_winnings"], 2),
            "wins": stats["wins"],
            "best_finish": stats["best_finish"],
            "last_active": stats["last_active"],
            "history": player_history[player_name],
        })

    # Sort by conservative rating descending
    leaderboard.sort(key=lambda p: p["conservative_rating"], reverse=True)

    # Add rank
    for i, player in enumerate(leaderboard):
        player["rank"] = i + 1

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "total_tournaments": processed,
        "total_players": len(leaderboard),
        "players": leaderboard,
    }

    with open(RATINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Print top 20
    print(f"\n{'='*70}")
    print(f"{'Rank':<6}{'Player':<30}{'Rating':<10}{'μ':<10}{'σ':<10}{'Events':<8}{'Wins':<6}")
    print(f"{'='*70}")
    for p in leaderboard[:20]:
        print(
            f"{p['rank']:<6}"
            f"{p['player']:<30}"
            f"{p['conservative_rating']:<10.2f}"
            f"{p['mu']:<10.2f}"
            f"{p['sigma']:<10.2f}"
            f"{p['tournaments_played']:<8}"
            f"{p['wins']:<6}"
        )
    print(f"{'='*70}")
    print(f"\nSaved {len(leaderboard)} player ratings to {RATINGS_FILE}")


if __name__ == "__main__":
    compute_ratings()
