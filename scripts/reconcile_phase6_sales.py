import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.config_loader import get_source_by_id
from src.connections import get_connection, get_sqlalchemy_uri
from src.minio_client import MinIOClient


SOURCE_ID = "phase6_sales"
SILVER_TABLE = "silver.fact_phase6_sales"
GOLD_TABLE = "gold.mart_phase6_daily_sales"


def decimal_sum(series: pd.Series) -> Decimal:
    values = series.dropna().tolist()

    return sum(
        (Decimal(str(value)) for value in values),
        Decimal("0"),
    )


def key_set(dataframe: pd.DataFrame) -> set[str]:
    return set(
        dataframe["sale_id"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )


def print_check(
    name: str,
    actual,
    expected,
) -> bool:
    success = actual == expected
    status = "PASS" if success else "FAIL"

    print(
        f"{status:4} | "
        f"{name:36} | "
        f"actual={actual} | "
        f"expected={expected}"
    )

    return success


def main() -> int:
    source_config = get_source_by_id(SOURCE_ID)

    source_path = PROJECT_ROOT / source_config["file_path"]

    source_df = pd.read_csv(
        source_path,
        dtype=str,
    )

    connection = get_connection("dwh_postgres")
    engine = create_engine(
        get_sqlalchemy_uri(connection)
    )

    registry_query = text("""
        SELECT
            batch_id,
            minio_path,
            row_count
        FROM meta._source_registry
        WHERE source_id = :source_id
          AND status = 'loaded'
        ORDER BY loaded_at DESC
        LIMIT 1
    """)

    with engine.connect() as conn:
        registry_row = conn.execute(
            registry_query,
            {"source_id": SOURCE_ID},
        ).mappings().one()

    batch_id = registry_row["batch_id"]
    minio_path = registry_row["minio_path"]
    registry_count = registry_row["row_count"]

    minio_client = MinIOClient()
    bronze_df = minio_client.download_dataframe(
        minio_path
    )

    staging_query = text(f"""
        SELECT *
        FROM {source_config["target_staging_table"]}
        WHERE _batch_id = :batch_id
    """)

    silver_query = text(f"""
        SELECT *
        FROM {SILVER_TABLE}
        WHERE _batch_id = :batch_id
    """)

    gold_query = text(f"""
        SELECT
            sale_date,
            sale_count,
            total_amount
        FROM {GOLD_TABLE}
    """)

    with engine.connect() as conn:
        staging_df = pd.read_sql(
            staging_query,
            conn,
            params={"batch_id": batch_id},
        )

        silver_df = pd.read_sql(
            silver_query,
            conn,
            params={"batch_id": batch_id},
        )

        gold_df = pd.read_sql(
            gold_query,
            conn,
        )

    source_total = decimal_sum(
        pd.to_numeric(
            source_df["amount"],
            errors="raise",
        )
    )

    bronze_total = decimal_sum(
        pd.to_numeric(
            bronze_df["amount"],
            errors="raise",
        )
    )

    staging_total = decimal_sum(
        pd.to_numeric(
            staging_df["amount"],
            errors="raise",
        )
    )

    silver_total = decimal_sum(
        silver_df["amount"]
    )

    gold_total = decimal_sum(
        gold_df["total_amount"]
    )

    gold_sale_count = int(
        gold_df["sale_count"].sum()
    )

    print("RECONCILIATION RESULTS")
    print("-" * 110)
    print("Batch =", batch_id)
    print("Bronze path =", minio_path)
    print("-" * 110)

    checks = [
        print_check(
            "Source rows = registry rows",
            len(source_df),
            registry_count,
        ),
        print_check(
            "Source rows = Bronze rows",
            len(source_df),
            len(bronze_df),
        ),
        print_check(
            "Bronze rows = Staging rows",
            len(bronze_df),
            len(staging_df),
        ),
        print_check(
            "Staging rows = Silver rows",
            len(staging_df),
            len(silver_df),
        ),
        print_check(
            "Silver rows = Gold sale_count",
            len(silver_df),
            gold_sale_count,
        ),
        print_check(
            "Source keys = Bronze keys",
            key_set(source_df),
            key_set(bronze_df),
        ),
        print_check(
            "Bronze keys = Staging keys",
            key_set(bronze_df),
            key_set(staging_df),
        ),
        print_check(
            "Staging keys = Silver keys",
            key_set(staging_df),
            key_set(silver_df),
        ),
        print_check(
            "Source amount = Bronze amount",
            source_total,
            bronze_total,
        ),
        print_check(
            "Bronze amount = Staging amount",
            bronze_total,
            staging_total,
        ),
        print_check(
            "Staging amount = Silver amount",
            staging_total,
            silver_total,
        ),
        print_check(
            "Silver amount = Gold amount",
            silver_total,
            gold_total,
        ),
    ]

    passed = sum(checks)
    failed = len(checks) - passed

    print("-" * 110)
    print("Total checks  =", len(checks))
    print("Passed checks =", passed)
    print("Failed checks =", failed)
    print(
        "Final status  =",
        "PASSED" if failed == 0 else "FAILED",
    )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())