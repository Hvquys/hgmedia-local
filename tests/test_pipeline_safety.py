import unittest
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from src.extractors.api_extractor import ApiExtractor
from src.extractors.sql_extractor import SQLExtractor
from src.loaders.staging_loader import StagingLoader
from src.sql_utils import split_qualified_table


class ApiExtractorTests(unittest.TestCase):
    @staticmethod
    def _response(payload):
        response = Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        return response

    @patch("src.extractors.api_extractor.requests.get")
    def test_pagination_returns_every_row_for_bronze(self, request_get):
        request_get.side_effect = [
            self._response(
                {"data": [{"id": 1}], "hasMore": True, "scrollId": "next"}
            ),
            self._response(
                {"data": [{"id": 2}], "hasMore": False, "scrollId": None}
            ),
        ]
        extractor = ApiExtractor(
            {
                "source_id": "sale",
                "api_url": "https://example.test/sales",
                "data_key": "data",
                "has_more_key": "hasMore",
                "scroll_key": "scrollId",
                "stream_to_staging": False,
            }
        )

        result = extractor.extract()

        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.dataframe["id"].tolist(), [1, 2])
        self.assertEqual(result.dataframe["_source_id"].tolist(), ["sale", "sale"])
        self.assertFalse(result.source_meta["parallel"])
        self.assertEqual(result.source_meta["pages"], 2)

    def test_parallel_failure_rejects_partial_batch(self):
        extractor = ApiExtractor(
            {
                "source_id": "sale",
                "api_url": "https://example.test/sales",
                "parallel_by": "month",
                "parallel_workers": 2,
                "api_date_from_param": "fromDate",
                "api_date_to_param": "toDate",
            }
        )
        extractor._get_month_ranges = Mock(
            return_value=[
                ("2026-01-01", "2026-02-01"),
                ("2026-02-01", "2026-03-01"),
            ]
        )

        def fetch(params, month_label=""):
            if params["fromDate"] == "2026-02-01":
                raise TimeoutError("upstream timeout")
            return pd.DataFrame([{"id": 1}]), 1

        extractor._fetch_pages = Mock(side_effect=fetch)

        with self.assertLogs("src.extractors.api_extractor", level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, "không tạo batch một phần"):
                extractor.extract()

    def test_parallel_success_combines_months_in_date_order(self):
        extractor = ApiExtractor(
            {
                "source_id": "sale",
                "api_url": "https://example.test/sales",
                "parallel_by": "month",
                "parallel_workers": 2,
                "api_date_from_param": "fromDate",
                "api_date_to_param": "toDate",
            }
        )
        extractor._get_month_ranges = Mock(
            return_value=[
                ("2026-01-01", "2026-02-01"),
                ("2026-02-01", "2026-03-01"),
            ]
        )

        def fetch(params, month_label=""):
            month = params["fromDate"][:7]
            return pd.DataFrame([{"month": month}]), 1

        extractor._fetch_pages = Mock(side_effect=fetch)

        result = extractor.extract()

        self.assertEqual(result.dataframe["month"].tolist(), ["2026-01", "2026-02"])
        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.source_meta["months"], 2)

    def test_streaming_mode_is_rejected(self):
        extractor = ApiExtractor(
            {
                "source_id": "sale",
                "api_url": "https://example.test/sales",
                "stream_to_staging": True,
            }
        )
        with self.assertRaisesRegex(ValueError, "Bronze"):
            extractor.extract()


class SQLExtractorTests(unittest.TestCase):
    def setUp(self):
        self.connection = {
            "type": "postgresql",
            "default_schema": "public",
        }
        self.config = {
            "source_id": "orders",
            "source_type": "sql",
            "connection": "source_db",
            "schema": "public",
            "source_table": "orders",
            "watermark_column": "updated_at",
            "incremental": True,
        }

    def test_watermark_is_bound_not_interpolated(self):
        query, parameters = SQLExtractor(
            self.config,
            self.connection,
        )._build_query("2026-01-01' OR 1=1 --")

        self.assertIn('"updated_at" > :watermark_value', str(query))
        self.assertNotIn("OR 1=1", str(query))
        self.assertEqual(parameters["watermark_value"], "2026-01-01' OR 1=1 --")

    def test_month_filter_is_validated_and_bound(self):
        config = {**self.config, "month_filter": "2026-02"}
        query, parameters = SQLExtractor(config, self.connection)._build_query()

        self.assertIn(":month_start", str(query))
        self.assertIn(":month_end", str(query))
        self.assertEqual(parameters["month_start"].strftime("%Y-%m-%d"), "2026-02-01")
        self.assertEqual(parameters["month_end"].strftime("%Y-%m-%d"), "2026-03-01")

    def test_invalid_identifier_is_rejected(self):
        config = {**self.config, "source_table": "orders; DROP TABLE orders"}
        with self.assertRaisesRegex(ValueError, "source_table"):
            SQLExtractor(config, self.connection)._build_query()


class StagingLoaderTests(unittest.TestCase):
    def test_qualified_table_validation(self):
        self.assertEqual(split_qualified_table("staging.orders"), ("staging", "orders"))
        for invalid in ("orders", "a.b.c", "staging.orders;drop"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    split_qualified_table(invalid)

    @patch.object(pd.DataFrame, "to_sql")
    def test_atomic_replace_uses_temp_table_and_transaction(self, to_sql):
        loader = StagingLoader.__new__(StagingLoader)
        loader.engine = MagicMock()
        transaction = loader.engine.begin.return_value.__enter__.return_value
        exists_result = Mock()
        exists_result.scalar_one.return_value = True
        transaction.execute.return_value = exists_result
        dataframe = pd.DataFrame({"id": [1], "_batch_id": ["batch"]})

        loader._replace_atomically(dataframe, "staging", "orders")

        to_sql.assert_called_once()
        statements = [
            call.args[0]
            for call in transaction.exec_driver_sql.call_args_list
        ]
        self.assertTrue(any(statement.startswith("TRUNCATE TABLE") for statement in statements))
        self.assertTrue(any(statement.startswith("INSERT INTO") for statement in statements))
        self.assertTrue(any(statement.startswith("DROP TABLE") for statement in statements))


if __name__ == "__main__":
    unittest.main()
