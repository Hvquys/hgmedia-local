# Project review after Phase 12

Review date: 2026-09-08

## Scope and verified baseline

- Reviewed Docker Compose, environment handling, Python EL/DQ code, five Airflow
  DAG groups, 67 configured sources, 60 dbt SQL models, initialization SQL,
  scripts and tests.
- `python -m unittest discover -s tests -v`: 21/21 passed after remediation.
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
- Post-remediation run `manual__2026-09-08T06:30:31.470067+00:00` succeeded
  with all seven tasks, DQ 9/9 and reconciliation 15/15. Registry batch
  `phase6_sales_20260908_063038_195847` finished as `loaded`.

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
11. Reworked API pagination so all pages/months return to Bronze and any monthly
    error rejects the whole batch instead of publishing partial success.
12. Replaced destructive staging table recreation with a temporary-table plus
    transactional `TRUNCATE + INSERT`; PostgreSQL integration checks prove that
    table identity, constraints, indexes and previous data survive correctly.
13. Validated and quoted configured SQL identifiers, and bound month/watermark
    values as SQLAlchemy parameters.

## Open findings

### High: broad source and model coverage is unverified

Only `phase6_sales` has a complete Airflow EL → DQ → dbt → reconciliation test.
DQ rules currently cover only `partners` and `phase6_sales`. Most database,
Google Sheet and Elasticsearch sources require their real credentials and
schemas before they can be validated. Three business mappings remain TODO in
`dim_distributed_employee`.

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

### Medium: automated test depth remains incomplete

The 21 tests now cover CSV incrementality, Phase 12 DQ metadata, API
pagination/failure, SQL query safety, staging replacement and configuration
drift. There are still no automated integration tests for rollback, Google
Sheet parsing, Elasticsearch pagination or the full dbt model set.

## Recommended order

1. Keep Phase 12 as the release smoke test.
2. Add one real source at a time with DQ and reconciliation evidence.
3. Complete the three unresolved employee mappings.
4. Pin dependencies and rotate local credentials before any shared deployment.
