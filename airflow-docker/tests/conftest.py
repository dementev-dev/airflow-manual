from __future__ import annotations

import pytest


def patch_postgres_hook(monkeypatch, fake_hook_class) -> None:
    """
    Патчит PostgresHook для тестирования.

    Args:
        monkeypatch: pytest monkeypatch fixture
        fake_hook_class: Класс-имитация PostgresHook
    """
    # Патчим PostgresHook на уровне airflow.providers.postgres.hooks.postgres
    # Это нужно, так как get_postgres_conn() импортирует его оттуда
    monkeypatch.setattr(
        "airflow.providers.postgres.hooks.postgres.PostgresHook",
        fake_hook_class,
        raising=False,
    )
