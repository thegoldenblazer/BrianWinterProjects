import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from app import app


class LeaderboardTimeframeTests(unittest.TestCase):
    def test_recent_results_support_all_display_date_formats(self):
        now = datetime.now()
        for date_format in ("%m/%d/%Y", "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
            with self.subTest(date_format=date_format):
                self.assert_player_visible(now.strftime(date_format), True)

    def test_old_or_invalid_results_are_excluded(self):
        old = datetime.now() - timedelta(days=400)
        for date in (old.strftime("%m/%d/%Y"), old.strftime("%b %d, %Y"), "", None, 123, "invalid"):
            with self.subTest(date=date):
                self.assert_player_visible(date, False)

    def assert_player_visible(self, date, expected):
        player = {
            "player": "Fall Refresh Regression Player",
            "rank": 1,
            "mu": 25,
            "sigma": 5,
            "conservative_rating": 10,
            "tournaments_played": 1,
            "total_winnings": 100,
            "wins": 1,
            "best_finish": 1,
            "last_active": date,
            "history": [{"date": date, "place": 1, "prize": 100}],
        }
        data = {
            "players": [player],
            "total_tournaments": 1,
            "generated_at": datetime.now().isoformat(),
        }
        with patch("app.get_data", return_value=data):
            response = app.test_client().get("/leaderboard?timeframe=12m&min_events=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(player["player"].encode() in response.data, expected)


if __name__ == "__main__":
    unittest.main()
