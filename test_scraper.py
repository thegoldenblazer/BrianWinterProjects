"""Quick test of the scraper on a few sample pages."""
from scraper import parse_tournament_page

# Test 1: Recent format (2026) - Place | Player | Prize
print("=== Test 1: Recent format (2026) ===")
url = "https://www.rungood.com/blogs/news-1/dean-wade-wins-graton-passport-400-big-stack-13-850"
results = parse_tournament_page(url)
print(f"Tournaments found: {len(results)}")
if results:
    t = results[0]
    print(f"Name: {t['name']}")
    print(f"Entrants: {t['entrants']}, Buy-in: {t['buy_in']}")
    print(f"Results count: {len(t['results'])}")
    for r in t["results"][:5]:
        print(f"  {r['place']:>3}. {r['player']:<30} ${r['prize']:>10,.0f}")

# Test 2: Older format (2021) - Place | Payout | Name (ALL CAPS)
print("\n=== Test 2: Older format (2021) ===")
url2 = "https://www.rungood.com/blogs/news-1/result-of-240-black-chip-bounty"
results2 = parse_tournament_page(url2)
print(f"Tournaments found: {len(results2)}")
if results2:
    t = results2[0]
    print(f"Name: {t['name']}")
    print(f"Results count: {len(t['results'])}")
    for r in t["results"][:5]:
        print(f"  {r['place']:>3}. {r['player']:<30} ${r['prize']:>10,.0f}")

# Test 3: Multi-tournament page (2 tournaments on one page)
print("\n=== Test 3: Multi-tournament page ===")
url3 = "https://www.rungood.com/blogs/news-1/results-of-rgps-jamul-casino-185-deepstack-200-double-green-chip-and-updated-casino-champ-point-standings"
results3 = parse_tournament_page(url3)
print(f"Tournaments found: {len(results3)}")
for i, t in enumerate(results3):
    print(f"  Table {i+1}: {len(t['results'])} players")
    if t["results"]:
        print(f"    1st: {t['results'][0]['player']}")

# Test 4: Tag team should be skipped
print("\n=== Test 4: Tag Team (should skip) ===")
url4 = "https://www.rungood.com/blogs/news-1/sheila-raines-and-george-raines-wins-graton-passport-300-tag-team-2-310"
results4 = parse_tournament_page(url4)
print(f"Tournaments found: {len(results4)} (expected 0)")

# Test 5: Small field (7 players)
print("\n=== Test 5: Small field ===")
url5 = "https://www.rungood.com/blogs/news-1/reginald-stapleton-wins-tunica-passport-200-five-card-plo-2-665"
results5 = parse_tournament_page(url5)
print(f"Tournaments found: {len(results5)}")
if results5:
    t = results5[0]
    print(f"Results count: {len(t['results'])}")
    for r in t["results"]:
        print(f"  {r['place']:>3}. {r['player']:<30} ${r['prize']:>10,.0f}")

print("\n=== All tests complete ===")
