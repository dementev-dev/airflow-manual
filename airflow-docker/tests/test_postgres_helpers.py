from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Sequence

import pytest

# Добавляем путь к dags в sys.path для импорта модулей
dags_path = str(Path(__file__).parent.parent / "dags")
if dags_path not in sys.path:
    sys.path.insert(0, dags_path)

import helpers.postgres as postgres_helpers
from conftest import patch_postgres_hook


@dataclass
class FakeCursor:
    fetchone_value: Any = None
    fetchall_value: Sequence[Any] | None = None
    rowcount: int | None = None

    def __post_init__(self) -> None:
        self.queries: List[Any] = []

    def execute(self, query: str, params: Any | None = None) -> None:
        self.queries.append((query, params))

    def fetchone(self) -> Any:
        return self.fetchone_value

    def fetchall(self) -> Sequence[Any] | None:
        return self.fetchall_value

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class FakeConn:
    def __init__(self, cursors: Sequence[FakeCursor]) -> None:
        self._cursors = list(cursors)
        self._index = 0
        self.commits = 0

    def cursor(self) -> FakeCursor:
        cursor = self._cursors[self._index]
        self._index += 1
        return cursor

    def commit(self) -> None:
        self.commits += 1


def test_get_postgres_conn_uses_airflow_hook(monkeypatch) -> None:
    class FakeHook:
        def __init__(self, postgres_conn_id: str) -> None:
            self.postgres_conn_id = postgres_conn_id

        def get_conn(self) -> str:
            return "hook_connection"

    patch_postgres_hook(monkeypatch, FakeHook)
    monkeypatch.setattr(postgres_helpers, "POSTGRES_CONN_ID", "demo_conn", raising=False)
    monkeypatch.setattr(postgres_helpers, "POSTGRES_USE_AIRFLOW_CONN", True, raising=False)

    conn = postgres_helpers.get_postgres_conn()

    assert conn == "hook_connection"


def test_get_postgres_conn_fallback_to_psycopg(monkeypatch) -> None:
    class BrokenHook:
        def __init__(self, postgres_conn_id: str) -> None:
            self.postgres_conn_id = postgres_conn_id

        def get_conn(self):
            raise RuntimeError("boom")

    patch_postgres_hook(monkeypatch, BrokenHook)
    monkeypatch.setattr(postgres_helpers, "POSTGRES_USE_AIRFLOW_CONN", True, raising=False)
    monkeypatch.setattr(postgres_helpers, "POSTGRES_CONN_ID", "demo_conn", raising=False)
    monkeypatch.setenv("POSTGRES_DB", "demo_db")
    monkeypatch.setenv("POSTGRES_USER", "demo_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_HOST", "postgres-host")
    monkeypatch.setenv("POSTGRES_PORT", "5434")

    captured_kwargs = {}

    def fake_connect(**kwargs):
        captured_kwargs.update(kwargs)
        return "psycopg_connection"

    monkeypatch.setattr(postgres_helpers.psycopg2, "connect", fake_connect)

    conn = postgres_helpers.get_postgres_conn()

    assert conn == "psycopg_connection"
    assert captured_kwargs == {
        "dbname": "demo_db",
        "user": "demo_user",
        "password": "secret",
        "host": "postgres-host",
        "port": 5434,
    }


def test_get_postgres_conn_without_airflow(monkeypatch) -> None:
    monkeypatch.setattr(postgres_helpers, "POSTGRES_USE_AIRFLOW_CONN", False, raising=False)
    monkeypatch.setenv("POSTGRES_DB", "demo_db")
    monkeypatch.setenv("POSTGRES_USER", "demo_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_HOST", "postgres-host")
    monkeypatch.setenv("POSTGRES_PORT", "5435")

    captured_kwargs = {}

    def fake_connect(**kwargs):
        captured_kwargs.update(kwargs)
        return "direct_psycopg"

    monkeypatch.setattr(postgres_helpers.psycopg2, "connect", fake_connect)

    conn = postgres_helpers.get_postgres_conn()

    assert conn == "direct_psycopg"
    assert captured_kwargs["port"] == 5435


def test_assert_orders_table_exists_ok() -> None:
    conn = FakeConn([FakeCursor(fetchone_value=(1,))])

    postgres_helpers.assert_orders_table_exists(conn)


def test_assert_orders_table_exists_missing() -> None:
    conn = FakeConn([FakeCursor(fetchone_value=None)])

    with pytest.raises(ValueError):
        postgres_helpers.assert_orders_table_exists(conn)


def test_assert_orders_schema_ok() -> None:
    expected = list(postgres_helpers.EXPECTED_ORDERS_SCHEMA)
    conn = FakeConn([FakeCursor(fetchall_value=expected)])

    postgres_helpers.assert_orders_schema(conn)


def test_assert_orders_schema_mismatch() -> None:
    conn = FakeConn([FakeCursor(fetchall_value=[("order_id", "bigint")])])

    with pytest.raises(ValueError):
        postgres_helpers.assert_orders_schema(conn)


def test_assert_orders_have_rows_ok() -> None:
    conn = FakeConn([FakeCursor(fetchone_value=(5,))])

    postgres_helpers.assert_orders_have_rows(conn)


def test_assert_orders_have_rows_empty() -> None:
    conn = FakeConn([FakeCursor(fetchone_value=(0,))])

    with pytest.raises(ValueError):
        postgres_helpers.assert_orders_have_rows(conn)


def test_assert_orders_no_duplicates_ok() -> None:
    conn = FakeConn([FakeCursor(fetchone_value=(0,))])

    postgres_helpers.assert_orders_no_duplicates(conn)


def test_assert_orders_no_duplicates_detected() -> None:
    conn = FakeConn([FakeCursor(fetchone_value=(3,))])

    with pytest.raises(ValueError):
        postgres_helpers.assert_orders_no_duplicates(conn)