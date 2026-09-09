import unittest

import duckdb

from workload.pipelines.history import (
    resolved_select_list,
    validate_part_sequence,
)


class HistoryRulesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = duckdb.connect()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def invalid_rows(self, table, condition, rows=100_000, transaction_rows=1_000_000):
        select_list = resolved_select_list(table, transaction_rows)
        query = f"""
            SELECT count(*) FROM (
                SELECT {select_list}
                FROM range(1, {rows + 1}) AS generated(i)
            ) generated
            WHERE {condition}
        """
        return self.connection.execute(query).fetchone()[0]

    def test_risk_labels_follow_probability(self):
        self.assertEqual(
            self.invalid_rows(
                "risk_analytics",
                """"ML_Risk" != CASE
                    WHEN round("FraudProb", 3) >= 0.7 THEN 'high'
                    WHEN round("FraudProb", 3) >= 0.3 THEN 'medium'
                    ELSE 'low' END""",
            ),
            0,
        )
        self.assertEqual(
            self.invalid_rows(
                "RiskModelPredictions",
                """risk_category_predicted != CASE
                    WHEN round(fraud_probability, 3) >= 0.7 THEN 'high'
                    WHEN round(fraud_probability, 3) >= 0.3 THEN 'medium'
                    ELSE 'low' END""",
            ),
            0,
        )

    def test_sessions_are_causal(self):
        condition = """
            cart_removals_count > cart_additions_count
            OR (checkout_completed AND NOT checkout_initiated)
            OR (bounce_indicator AND (
                checkout_initiated OR checkout_completed
                OR cart_additions_count > 0
            ))
        """
        self.assertEqual(
            self.invalid_rows("BuyerSessionAnalytics", condition),
            0,
        )

    def test_payment_states_are_consistent(self):
        condition = """
            (processing_stage = 'failed' AND (
                amount_processed != 0 OR decline_reason IS NULL OR retry_count < 1
            ))
            OR (processing_stage = 'settled' AND (
                abs(amount_processed - amount_requested) > 0.01
                OR decline_reason IS NOT NULL OR retry_count != 0
            ))
        """
        self.assertEqual(
            self.invalid_rows("PaymentProcessingEvents", condition),
            0,
        )

    def test_resume_rejects_gap_and_overlap(self):
        valid = [
            {"part_number": 0, "row_start": 1, "row_count": 100},
            {"part_number": 1, "row_start": 101, "row_count": 50},
        ]
        validate_part_sequence("transactions", valid)
        for invalid in (
            [
                {"part_number": 0, "row_start": 1, "row_count": 100},
                {"part_number": 2, "row_start": 101, "row_count": 50},
            ],
            [
                {"part_number": 0, "row_start": 1, "row_count": 100},
                {"part_number": 1, "row_start": 100, "row_count": 50},
            ],
        ):
            with self.assertRaises(RuntimeError):
                validate_part_sequence("transactions", invalid)


if __name__ == "__main__":
    unittest.main()
