import unittest

import main


class MainDemoQuestionTests(unittest.TestCase):
    """Question 10: final integration demo."""

    def test_run_demo_returns_checkpoint_and_visible_rows(self) -> None:
        # The demo should tell the full story an end user would see after running the program.
        payload = main.run_demo()

        self.assertEqual(payload["database"], "exercise_db")
        self.assertEqual(payload["schema"], "analytics")
        self.assertEqual(payload["table"], "events")
        self.assertEqual(payload["visible_rows"], [
            {"event_id": 1, "category": "click", "value": 10},
            {"event_id": 3, "category": "purchase", "value": None},
            {"event_id": 4, "category": "view", "value": 20},
        ])
        self.assertEqual(payload["checkpoint"]["table_name"], "events")
        self.assertEqual(payload["checkpoint"]["total_rows"], 4)


if __name__ == "__main__":
    unittest.main()
