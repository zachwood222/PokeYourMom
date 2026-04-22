#!/usr/bin/env bash
set -euo pipefail

echo "==> [1/4] Lint and static checks"
ruff check tests/test_billing_schema.py tests/test_parser_fixtures.py tests/parser_fixture_harness.py retailers network
python -m compileall -q app.py retailers network tests/test_billing_schema.py tests/test_parser_fixtures.py tests/parser_fixture_harness.py

echo "==> [2/4] Unit tests"
pytest -q -ra tests

echo "==> [3/4] Migration safety: fresh init_db() path"
pytest -q -ra tests/test_billing_schema.py::test_init_db_creates_billing_tables_and_columns

echo "==> [4/4] Migration safety: legacy monitors migration path"
pytest -q -ra tests/test_app.py::test_init_db_migrates_existing_monitors_table_with_msrp_column

echo "==> All CI quality gates passed."
