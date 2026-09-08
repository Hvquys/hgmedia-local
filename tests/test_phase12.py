import unittest
from unittest.mock import ANY, Mock, patch

from src.tasks.data_quality_task import DataQualityError, run_data_quality


LOAD_RESULT = {
    "source_id": "phase6_sales",
    "target_table": "staging.phase6_sales",
    "batch_id": "phase6_sales_batch",
    "status": "loaded",
}

RULE_CONFIG = {
    "enabled": True,
    "target_table": "staging.phase6_sales",
    "fail_pipeline_on": ["critical"],
    "rules": [{"id": "row_count"}],
}


class AirflowDataQualityTaskTests(unittest.TestCase):
    @patch("src.tasks.data_quality_task.validate_dataframe")
    @patch("src.tasks.data_quality_task.load_staging_dataframe")
    @patch("src.tasks.data_quality_task.get_source_rules")
    @patch("src.tasks.data_quality_task.DataQualityRepository")
    def test_passed_run_keeps_airflow_lineage(
        self,
        repository_class,
        get_source_rules,
        load_staging_dataframe,
        validate_dataframe,
    ):
        get_source_rules.return_value = RULE_CONFIG
        load_staging_dataframe.return_value = Mock()
        validate_dataframe.return_value = [
            {"rule_id": "row_count", "severity": "critical", "success": True}
        ]

        result = run_data_quality(
            LOAD_RESULT,
            airflow_dag_id="phase12_sales_e2e",
            airflow_dag_run_id="manual__phase12",
        )

        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["batch_id"], "phase6_sales_batch")
        create_kwargs = repository_class.return_value.create_run.call_args.kwargs
        self.assertEqual(create_kwargs["airflow_dag_id"], "phase12_sales_e2e")
        self.assertEqual(create_kwargs["airflow_dag_run_id"], "manual__phase12")
        repository_class.return_value.finish_run.assert_called_once_with(
            validation_run_id=result["validation_run_id"],
            status="passed",
            total_rules=1,
            passed_rules=1,
            failed_rules=0,
        )

    @patch("src.tasks.data_quality_task.validate_dataframe")
    @patch("src.tasks.data_quality_task.load_staging_dataframe")
    @patch("src.tasks.data_quality_task.get_source_rules")
    @patch("src.tasks.data_quality_task.DataQualityRepository")
    def test_blocking_failure_is_preserved_before_task_fails(
        self,
        repository_class,
        get_source_rules,
        load_staging_dataframe,
        validate_dataframe,
    ):
        get_source_rules.return_value = RULE_CONFIG
        load_staging_dataframe.return_value = Mock()
        validate_dataframe.return_value = [
            {"rule_id": "row_count", "severity": "critical", "success": False}
        ]

        with self.assertRaises(DataQualityError):
            run_data_quality(
                LOAD_RESULT,
                airflow_dag_id="phase12_sales_e2e",
                airflow_dag_run_id="manual__failed",
            )

        repository_class.return_value.finish_run.assert_called_once_with(
            validation_run_id=ANY,
            status="failed",
            total_rules=1,
            passed_rules=0,
            failed_rules=1,
        )


if __name__ == "__main__":
    unittest.main()
