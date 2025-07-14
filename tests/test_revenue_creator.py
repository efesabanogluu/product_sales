import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime
import sqlite3
import revenue_creator 

class TestDataPipeline(unittest.TestCase):

    def test_create_date_range(self):
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 3)
        result = revenue_creator.create_date_range(start_date, end_date)
        expected_dates = pd.date_range(start=start_date, end=end_date)
        pd.testing.assert_index_equal(result, expected_dates)

    def test_generate_all_combinations(self):
        product_df = pd.DataFrame({"sku_id": [1, 2]})
        date_list = pd.date_range("2025-01-01", "2025-01-02")
        result = revenue_creator.generate_all_combinations(product_df, date_list)
        expected_rows = 4  # 2 products * 2 dates
        self.assertEqual(len(result), expected_rows)
        self.assertListEqual(list(result.columns), ["sku_id", "date_id"])

    def test_aggregate_sales(self):
        data = {
            "sku_id": [1, 1, 2],
            "orderdate_utc": ["2025-01-01 10:00:00", "2025-01-01 15:00:00", "2025-01-02 12:00:00"],
            "sales": [2, 3, 5]
        }
        sales_df = pd.DataFrame(data)
        sales_df["orderdate_utc"] = pd.to_datetime(sales_df["orderdate_utc"])
        result = revenue_creator.aggregate_sales(sales_df)
        self.assertEqual(result.loc[result["sku_id"] == 1, "sales"].values[0], 5)  # 2+3=5 sales for sku 1 on 1 Jan
        self.assertEqual(result.loc[result["sku_id"] == 2, "sales"].values[0], 5)

    def test_build_revenue_table(self):
        product_df = pd.DataFrame({"sku_id": [1], "price": [10]})
        date_list = pd.date_range("2025-01-01", "2025-01-01")
        all_combinations = revenue_creator.generate_all_combinations(product_df, date_list)
        agg_sales = pd.DataFrame({"sku_id": [1], "date_id": [pd.Timestamp("2025-01-01")], "sales": [3]})
        result = revenue_creator.build_revenue_table(all_combinations, product_df, agg_sales)
        self.assertIn("revenue", result.columns)
        self.assertEqual(result.loc[0, "revenue"], 30)  # 10 price * 3 sales

    @patch('revenue_creator.sqlite3.connect')
    def test_connect_db_success(self, mock_connect):
        mock_connect.return_value = MagicMock(spec=sqlite3.Connection)
        conn = revenue_creator.connect_db('fake_path.db')
        self.assertIsNotNone(conn)

    @patch('revenue_creator.sqlite3.connect', side_effect=sqlite3.Error("Failed to connect"))
    def test_connect_db_failure(self, mock_connect):
        with self.assertRaises(sqlite3.Error):
            revenue_creator.connect_db('fake_path.db')

if __name__ == "__main__":
    unittest.main()
