# Project review after Phase 12

Review date: 2026-09-08

## Scope and verified baseline

- Reviewed Docker Compose, environment handling, Python EL/DQ code, five Airflow
  DAG groups, 67 configured sources, 60 dbt SQL models, initialization SQL,
  scripts and tests.
- `python -m unittest discover -s tests -v`: 7/7 passed.
- `python -m compileall`: passed.
- `python -m pip check`: passed.
- `dbt parse --no-partial-parse`: passed.
- Phase 12 success and retry/recovery runs were already verified in Airflow.
- `docker compose up -d --force-recreate`: all services recreated successfully;
  PostgreSQL, Airflow API server, scheduler, DAG processor and triggerer reported
  healthy, while `airflow-init` exited with code 0.
- `airflow dags list-import-errors`: no data found.
- Airflow loaded all expected tasks for `dbt_transform_pipeline` and
  `phase12_sales_e2e` after the review changes.

The Phase 12 `phase6_sales` vertical slice is reproducible. This does not yet
prove that every configured business source and every dbt model is production
ready.

## Fixed in this review

1. Airflow UI, JWT and metadata database variables are now required from
   `.env`; Compose no longer silently substitutes credentials from YAML.
2. Removed credential-like defaults from Python, dbt profile and the dbt DAG.
3. Added every source connection variable to `.env.example`.
4. Bound local service ports to `127.0.0.1`.
5. Fixed duplicate YAML keys and the invalid string value `false;`.
6. Synchronized Airflow source pickers with source configuration.
7. Synchronized the dbt picker with all 60 SQL model files.
8. Mapped the 07:30 dbt sensors to the preceding EL schedules instead of
   looking for impossible runs with the same logical date.
9. Removed stale generated files and duplicate `.gitignore` rules.
10. Corrected README credentials, setup steps, Airflow password behavior and
    the MinIO API port used by `mc`.

## Open findings

### Critical: API source does not preserve extracted data

`ApiExtractor._extract_parallel_monthly()` returns an empty dataframe marked as
streamed even when `stream_to_staging` is false. The configured `sale` source
therefore can fetch rows and still load nothing. Per-month exceptions are also
logged and swallowed, which can turn partial extraction into apparent success.
Do not enable this source until the extractor returns/loads all pages and fails
the batch when any month fails.

### High: broad source and model coverage is unverified

Only `phase6_sales` has a complete Airflow EL → DQ → dbt → reconciliation test.
DQ rules currently cover only `partners` and `phase6_sales`. Most database,
Google Sheet and Elasticsearch sources require their real credentials and
schemas before they can be validated. Three business mappings remain TODO in
`dim_distributed_employee`.

### High: staging replacement is not atomic

`StagingLoader` uses pandas `to_sql(if_exists="replace")` for truncate/full
loads. This drops and recreates the table, which can remove grants, indexes and
constraints, and a failed reload can leave the target absent or partial. Replace
this with load-to-temporary-table followed by a transactional swap/truncate and
insert.

### High: SQL identifiers and watermark values are interpolated

`SQLExtractor` and `StagingLoader` compose SQL with schema, table, column and
watermark strings. Configuration is trusted today, but malformed identifiers or
quoted values can break queries and increase injection risk. Validate/quote
identifiers and bind watermark values as SQL parameters.

### Medium: one configured CSV is absent locally

`stream_distro` now points to `data/fact_view_stream.csv`, but that ignored local
file is not present in this checkout. Selecting it in Airflow will fail until the
source file is placed at that path.

### Medium: dependency builds are not fully reproducible

Several Python requirements use lower bounds and Airflow providers are not
pinned through the official Airflow constraint set. A future image rebuild can
resolve different versions. Generate and maintain a tested lock/constraints
file before deployment.

### Medium: current local credentials are legacy development values

The missing Airflow variables were added to the ignored local `.env` with the
same values already in use so existing Docker volumes keep working. Several
local service credentials are weak. Rotate Airflow UI/JWT, MinIO and database
passwords before allowing access outside the local machine. Changing the
Airflow UI value in `.env` does not update an existing FAB user; use the reset
command documented in README.

### Medium: automated test depth is small

The seven tests cover CSV incrementality and Phase 12 DQ metadata. There are no
automated tests for SQL query generation, API pagination/failure, staging
atomicity, rollback, Google Sheet parsing, Elasticsearch pagination or the full
dbt model set.

## Recommended order

1. Keep Phase 12 as the release smoke test.
2. Repair and test `ApiExtractor` before enabling `sale`.
3. Make staging full loads atomic and quote/bind SQL safely.
4. Add one real source at a time with DQ and reconciliation evidence.
5. Complete the three unresolved employee mappings.
6. Pin dependencies and rotate local credentials before any shared deployment.
