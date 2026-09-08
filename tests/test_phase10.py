import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from src.extractors.csv_extractor import CsvExtractor
from src.tasks.extract_task import run_extract


class CsvExtractorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.csv_path = Path(self.temp_dir.name) / "sales.csv"
        self.csv_path.write_text(
            "sale_id,amount,sale_date\n"
            "S001,100,2026-09-01\n"
            "S002,200,2026-09-02\n",
            encoding="utf-8",
        )
        self.config = {
            "source_id": "phase6_sales",
            "source_type": "csv",
            "file_path": str(self.csv_path),
            "sep": ",",
            "incremental": True,
            "watermark_column": "sale_date",
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_checksum_detects_unchanged_and_changed_file(self):
        extractor = CsvExtractor(self.config)
        first_result = extractor.extract()

        self.assertFalse(extractor.has_changed(first_result.checksum))

        with self.csv_path.open("a", encoding="utf-8") as file:
            file.write("S003,300,2026-09-03\n")

        self.assertTrue(extractor.has_changed(first_result.checksum))

    def test_incremental_extract_returns_only_new_watermark_rows(self):
        result = CsvExtractor(self.config).extract(
            watermark_filter="2026-09-01",
        )

        self.assertEqual(result.row_count, 1)
        self.assertEqual(result.dataframe["sale_id"].tolist(), ["S002"])
        self.assertEqual(result.watermark_value, "2026-09-02 00:00:00")


class ExtractTaskTests(unittest.TestCase):
    @patch("src.tasks.extract_task.get_extractor")
    @patch("src.tasks.extract_task.MinIOClient")
    @patch("src.tasks.extract_task.SourceRegistry")
    def test_unchanged_csv_is_skipped(
        self,
        registry_class,
        minio_class,
        get_extractor,
    ):
        registry_class.return_value.get_last_success.return_value = {
            "checksum": "same-checksum",
            "watermark_value": "2026-09-02 00:00:00",
        }
        extractor = Mock()
        extractor.has_changed.return_value = False
        get_extractor.return_value = extractor

        result = run_extract(
            {
                "source_id": "phase6_sales",
                "source_type": "csv",
                "incremental": True,
            }
        )

        self.assertIsNone(result)
        extractor.has_changed.assert_called_once_with("same-checksum")
        minio_class.return_value.upload_dataframe.assert_not_called()

    @patch("src.tasks.extract_task.get_extractor")
    @patch("src.tasks.extract_task.MinIOClient")
    @patch("src.tasks.extract_task.SourceRegistry")
    def test_incremental_csv_uses_previous_watermark(
        self,
        registry_class,
        minio_class,
        get_extractor,
    ):
        registry = registry_class.return_value
        registry.get_last_success.return_value = {
            "checksum": "old-checksum",
            "watermark_value": "2026-09-02 00:00:00",
        }
        extractor = Mock()
        extractor.has_changed.return_value = True
        extractor.extract.return_value = Mock(
            dataframe=pd.DataFrame({"sale_id": ["S003"]}),
            row_count=1,
            checksum="new-checksum",
            watermark_value="2026-09-03 00:00:00",
            source_meta={},
        )
        get_extractor.return_value = extractor
        minio = minio_class.return_value
        minio.make_batch_id.return_value = "phase6_sales_batch"
        minio.upload_dataframe.return_value = "s3://raw/batch.parquet"

        result = run_extract(
            {
                "source_id": "phase6_sales",
                "source_type": "csv",
                "incremental": True,
            }
        )

        extractor.extract.assert_called_once_with(
            watermark_filter="2026-09-02 00:00:00"
        )
        self.assertEqual(result["batch_id"], "phase6_sales_batch")
        self.assertIsNone(result["load_mode_override"])
        registry.register_extracted.assert_called_once()

    @patch("src.tasks.extract_task.get_extractor")
    @patch("src.tasks.extract_task.MinIOClient")
    @patch("src.tasks.extract_task.SourceRegistry")
    def test_old_row_change_falls_back_to_full_snapshot(
        self,
        registry_class,
        minio_class,
        get_extractor,
    ):
        registry_class.return_value.get_last_success.return_value = {
            "checksum": "old-checksum",
            "watermark_value": "2026-09-02 00:00:00",
        }
        incremental_result = Mock(
            dataframe=pd.DataFrame(),
            row_count=0,
            checksum="new-checksum",
            watermark_value="2026-09-02 00:00:00",
            source_meta={},
        )
        full_result = Mock(
            dataframe=pd.DataFrame({"sale_id": ["S001", "S002"]}),
            row_count=2,
            checksum="new-checksum",
            watermark_value="2026-09-02 00:00:00",
            source_meta={},
        )
        extractor = Mock()
        extractor.has_changed.return_value = True
        extractor.extract.side_effect = [incremental_result, full_result]
        get_extractor.return_value = extractor
        minio = minio_class.return_value
        minio.make_batch_id.return_value = "phase6_sales_batch"
        minio.upload_dataframe.return_value = "s3://raw/batch.parquet"

        result = run_extract(
            {
                "source_id": "phase6_sales",
                "source_type": "csv",
                "incremental": True,
            }
        )

        self.assertEqual(extractor.extract.call_count, 2)
        self.assertEqual(result["load_mode_override"], "truncate")


if __name__ == "__main__":
    unittest.main()
