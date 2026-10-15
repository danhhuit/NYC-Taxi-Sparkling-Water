"""Unit tests for the local dashboard without starting an HTTP server."""

from __future__ import annotations

import unittest

from src.app.server import (
    InputError,
    artifact_status,
    build_index_html,
    load_analytics,
    load_prediction_models,
    load_summary,
    normalize_prediction_input,
)


class DashboardTest(unittest.TestCase):
    def test_summary_has_final_results(self) -> None:
        summary = load_summary()
        self.assertEqual(summary["dataset"]["clean_rows"], 46_714_819)
        self.assertEqual(len(summary["splits"]), 3)
        self.assertGreaterEqual(len(summary["models"]), 5)
        self.assertEqual(summary["mojo"]["null_predictions"], 0)

    def test_datetime_features_follow_spark_dayofweek(self) -> None:
        features = normalize_prediction_input(
            {
                "pickup_location": 161,
                "dropoff_location": 236,
                "passenger_count": 1,
                "pickup_datetime": "2025-11-08T09:15",
                "route_trip_count": 10000,
                "route_avg_duration": 15,
                "hour_route_avg_duration": 16,
            }
        )
        self.assertEqual(features["pickup_hour"], 9)
        self.assertEqual(features["pickup_day_of_week"], 7)
        self.assertEqual(features["is_weekend"], 1)
        self.assertEqual(features["pickup_month"], 11)

    def test_invalid_zone_is_rejected(self) -> None:
        with self.assertRaises(InputError):
            normalize_prediction_input(
                {
                    "pickup_location": 0,
                    "dropoff_location": 236,
                    "passenger_count": 1,
                    "pickup_datetime": "2025-11-08T09:15",
                    "route_trip_count": 10000,
                    "route_avg_duration": 15,
                    "hour_route_avg_duration": 16,
                }
            )

    def test_required_artifacts_exist(self) -> None:
        self.assertTrue(all(artifact_status().values()))

    def test_ui_has_monochrome_loading_states(self) -> None:
        html = build_index_html()
        self.assertIn('id="loadingOverlay"', html)
        self.assertIn("button-spinner", html)
        self.assertIn('class="sidebar"', html)
        self.assertIn('id="themeToggle"', html)
        self.assertIn('id="languageToggle"', html)
        self.assertIn('"Segoe UI Variable"', html)
        self.assertIn("verticalBarChart", html)
        self.assertIn('id="sidebarToggle"', html)
        self.assertIn('id="modelFilter"', html)
        self.assertIn('data-metric="all"', html)
        self.assertIn('id="prediction_model"', html)
        self.assertIn('id="historyTable"', html)
        self.assertEqual(html.count("data-history-pagination"), 2)
        self.assertEqual(html.count("data-history-page-size"), 4)
        self.assertIn('data-info="features"', html)
        self.assertIn(".all-metrics-grid { display: grid; gap: 14px; grid-template-columns: 1fr; }", html)
        data_section = html.index('<section class="section" id="data">')
        prediction_section = html.index('<section class="section" id="prediction">')
        self.assertGreater(html.index('id="monthFilter"'), data_section)
        self.assertGreater(html.index('class="kpi-grid"'), data_section)
        self.assertNotEqual(data_section, prediction_section)
        self.assertIn("#prediction { order: 2;", html)
        self.assertIn("#data { order: 3;", html)
        self.assertIn("@keyframes grow-bar-y", html)
        self.assertNotIn('href="#pipeline"', html)
        self.assertNotIn("logo-mark", html)
        self.assertNotIn('"Century Gothic"', html)

    def test_all_comparison_models_are_available_for_prediction(self) -> None:
        models = load_prediction_models()
        self.assertEqual(
            {model["key"] for model in models},
            {"drf", "gbm_grid", "gbm", "automl", "deep_learning"},
        )
        self.assertTrue(all(model["available"] for model in models))
        self.assertTrue(all(model["rmse"] is not None for model in models))

    def test_analytics_payload_covers_full_project(self) -> None:
        analytics = load_analytics()
        self.assertEqual(len(analytics["monthly"]), 12)
        self.assertEqual(len(analytics["splits"]), 3)
        self.assertEqual(len(analytics["feature_importance"]), 10)
        self.assertEqual(
            set(analytics["errors"]),
            {
                "hour",
                "day_of_week",
                "pickup_borough",
                "dropoff_borough",
                "duration_range",
                "route",
            },
        )


if __name__ == "__main__":
    unittest.main()
