from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = PROJECT_ROOT / "config" / "data_quality_rules.yaml"


def load_rule_config() -> dict:
    if not RULES_PATH.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file DQ rules: {RULES_PATH}"
        )

    with RULES_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def get_source_rules(source_id: str) -> dict:
    config = load_rule_config()
    rule_groups = config.get("data_quality_rules", {})

    source_rules = rule_groups.get(source_id)

    if source_rules is None:
        raise KeyError(
            f"Không tìm thấy DQ rules cho source_id: {source_id}"
        )

    return source_rules
