import ast
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(
                f"Duplicate YAML key {key!r} at line {key_node.start_mark.line + 1}"
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _assignment(path: Path, variable_name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == variable_name
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"Missing assignment {variable_name} in {path}")


class ProjectConfigurationTests(unittest.TestCase):
    def test_yaml_files_do_not_contain_duplicate_keys(self):
        yaml_files = sorted((ROOT / "config").glob("*.yaml"))
        yaml_files += sorted((ROOT / "dwh_dbt").glob("*.yml"))
        yaml_files += sorted((ROOT / "dwh_dbt" / "models").rglob("*.yml"))

        for path in yaml_files:
            with self.subTest(path=path.relative_to(ROOT)):
                yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueKeyLoader)

    def test_database_dag_picker_matches_database_config(self):
        configured = yaml.safe_load(
            (ROOT / "config" / "db_sources.yaml").read_text(encoding="utf-8")
        )["db_sources"]
        configured_by_connection = {}
        for source in configured:
            configured_by_connection.setdefault(source["connection"], set()).add(
                source["source_id"]
            )

        picker = _assignment(
            ROOT / "src" / "dags" / "el_database_dag.py",
            "ALL_SOURCES_BY_CONNECTION",
        )
        picker = {key: set(value) for key, value in picker.items()}
        self.assertEqual(picker, configured_by_connection)

    def test_google_sheet_dag_picker_matches_config(self):
        configured = yaml.safe_load(
            (ROOT / "config" / "google_sheet_sources.yaml").read_text(
                encoding="utf-8"
            )
        )["google_sheet_sources"]
        configured_ids = {source["source_id"] for source in configured}
        picker_ids = set(
            _assignment(
                ROOT / "src" / "dags" / "el_google_sheet_dag.py",
                "ALL_SOURCES",
            )
        )
        self.assertEqual(picker_ids, configured_ids)

    def test_dbt_dag_picker_matches_sql_models(self):
        dag_path = ROOT / "src" / "dags" / "dbt_run_dag.py"
        picker_ids = set()
        for variable_name in (
            "SILVER_DIM_MODELS",
            "SILVER_FACT_MODELS",
            "INTERMEDIATE_MODELS",
            "GOLD_MODELS",
        ):
            picker_ids.update(_assignment(dag_path, variable_name))

        model_ids = {
            path.stem for path in (ROOT / "dwh_dbt" / "models").rglob("*.sql")
        }
        self.assertEqual(picker_ids, model_ids)

    def test_credentials_are_not_defaulted_in_runtime_config(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("${AIRFLOW_ADMIN_PASSWORD:?", compose)
        self.assertIn("${AIRFLOW_JWT_SECRET:?", compose)
        self.assertNotIn("phase11-local-jwt-secret", compose)

        runtime_files = [
            ROOT / "src" / "connections.py",
            ROOT / "src" / "dags" / "dbt_run_dag.py",
            ROOT / "dwh_dbt" / "profiles.yml",
        ]
        prohibited = ("Inda1234", "hgmedia@123", "PLACEHOLDER_PASS")
        for path in runtime_files:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.relative_to(ROOT)):
                for value in prohibited:
                    self.assertNotIn(value, text)


if __name__ == "__main__":
    unittest.main()
