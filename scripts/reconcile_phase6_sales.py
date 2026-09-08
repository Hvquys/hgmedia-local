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

    registry_query = text(f"""
        SELECT
            batch_id,
            minio_path,
            row_count
        FROM meta._source_registry
        WHERE batch_id = (
            SELECT _batch_id
            FROM {source_config["target_staging_table"]}
            ORDER BY _loaded_at DESC
            LIMIT 1
        )
          AND source_id = :source_id
          AND status = 'loaded'
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

    staging_batch_query = text(f"""
        SELECT *
        FROM {source_config["target_staging_table"]}
        WHERE _batch_id = :batch_id
    """)

    silver_batch_query = text(f"""
        SELECT *
        FROM {SILVER_TABLE}
        WHERE _batch_id = :batch_id
    """)

    staging_snapshot_query = text(f"""
        SELECT *
        FROM {source_config["target_staging_table"]}
    """)

    silver_snapshot_query = text(f"""
        SELECT *
        FROM {SILVER_TABLE}
    """)

    gold_query = text(f"""
        SELECT
            sale_date,
            sale_count,
            total_amount
        FROM {GOLD_TABLE}
    """)

    with engine.connect() as conn:
        staging_batch_df = pd.read_sql(
            staging_batch_query,
            conn,
            params={"batch_id": batch_id},
        )

        silver_batch_df = pd.read_sql(
            silver_batch_query,
            conn,
            params={"batch_id": batch_id},
        )

        staging_snapshot_df = pd.read_sql(
            staging_snapshot_query,
            conn,
        )

        silver_snapshot_df = pd.read_sql(
            silver_snapshot_query,
            conn,
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

    staging_batch_total = decimal_sum(
        pd.to_numeric(
            staging_batch_df["amount"],
            errors="raise",
        )
    )

    silver_batch_total = decimal_sum(
        silver_batch_df["amount"]
    )

    staging_snapshot_total = decimal_sum(
        pd.to_numeric(
            staging_snapshot_df["amount"],
            errors="raise",
        )
    )

    silver_snapshot_total = decimal_sum(
        silver_snapshot_df["amount"]
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
            "Registry rows = Bronze delta rows",
            registry_count,
            len(bronze_df),
        ),
        print_check(
            "Bronze delta = Staging batch rows",
            len(bronze_df),
            len(staging_batch_df),
        ),
        print_check(
            "Staging batch = Silver batch rows",
            len(staging_batch_df),
            len(silver_batch_df),
        ),
        print_check(
            "Bronze delta keys = Staging batch",
            key_set(bronze_df),
            key_set(staging_batch_df),
        ),
        print_check(
            "Staging batch keys = Silver batch",
            key_set(staging_batch_df),
            key_set(silver_batch_df),
        ),
        print_check(
            "Bronze delta amount = Staging batch",
            bronze_total,
            staging_batch_total,
        ),
        print_check(
            "Staging batch amount = Silver batch",
            staging_batch_total,
            silver_batch_total,
        ),
        print_check(
            "Source rows = Staging snapshot rows",
            len(source_df),
            len(staging_snapshot_df),
        ),
        print_check(
            "Staging snapshot = Silver rows",
            len(staging_snapshot_df),
            len(silver_snapshot_df),
        ),
        print_check(
            "Silver snapshot = Gold sale_count",
            len(silver_snapshot_df),
            gold_sale_count,
        ),
        print_check(
            "Source keys = Staging snapshot keys",
            key_set(source_df),
            key_set(staging_snapshot_df),
        ),
        print_check(
            "Staging keys = Silver snapshot keys",
            key_set(staging_snapshot_df),
            key_set(silver_snapshot_df),
        ),
        print_check(
            "Source amount = Staging snapshot",
            source_total,
            staging_snapshot_total,
        ),
        print_check(
            "Staging amount = Silver snapshot",
            staging_snapshot_total,
            silver_snapshot_total,
        ),
        print_check(
            "Silver snapshot amount = Gold amount",
            silver_snapshot_total,
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
