"""Small helpers for validating and quoting configured SQL identifiers."""

import re


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str = "identifier") -> str:
    if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{label} không hợp lệ: {value!r}")
    return value


def split_qualified_table(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or value.count(".") != 1:
        raise ValueError(f"Tên bảng phải có dạng schema.table: {value!r}")
    schema, table = value.split(".", 1)
    return (
        validate_identifier(schema, "schema"),
        validate_identifier(table, "table"),
    )


def quote_identifier(value: str, dialect: str = "postgresql") -> str:
    validate_identifier(value)
    if dialect == "sqlserver":
        return f"[{value}]"
    return f'"{value}"'


def quote_dataframe_column(value: str) -> str:
    """Quote a dataframe column for PostgreSQL, including spaces/non-ASCII."""
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError(f"Tên cột dataframe không hợp lệ: {value!r}")
    return '"' + value.replace('"', '""') + '"'
