"""
DAG для проверки качества данных (Data Quality) после загрузки в PostgreSQL
Уровень: Средний

Этот DAG запускается после csv_to_postgres и проверяет таблицу public.orders:
1. Существование таблицы
2. Соответствие схемы (колонки и типы данных)
3. Наличие данных (таблица не пуста)
4. Отсутствие дубликатов по order_id
5. Итоговая сводка по результатам проверок

Результат: логи с результатами каждой проверки. При ошибке — ValueError с описанием проблемы.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow import DAG

POSTGRES_CONN_ID = "postgres_training"

EXPECTED_ORDERS_SCHEMA = [
    ("order_id", "bigint"),
    ("order_ts", "timestamp without time zone"),
    ("customer_id", "bigint"),
    ("amount", "numeric"),
]


def _get_conn():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def _check_table_exists():
    """Проверяет наличие таблицы public.orders."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM pg_catalog.pg_tables
                WHERE schemaname = 'public' AND tablename = 'orders'
            """)
            if cur.fetchone() is None:
                raise ValueError("Таблица public.orders не найдена")
        logging.info("Таблица public.orders существует")
    finally:
        conn.close()


def _check_schema():
    """Проверяет соответствие схемы таблицы public.orders ожидаемой."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'orders'
                ORDER BY ordinal_position
            """)
            actual = cur.fetchall()
        if actual != EXPECTED_ORDERS_SCHEMA:
            raise ValueError(
                f"Схема не совпадает. Ожидалось: {EXPECTED_ORDERS_SCHEMA}, "
                f"получено: {actual}"
            )
        logging.info("Схема таблицы public.orders соответствует ожидаемой")
    finally:
        conn.close()


def _check_has_rows():
    """Проверяет, что в таблице public.orders есть данные."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.orders")
            count = cur.fetchone()[0]
        if count == 0:
            raise ValueError("Таблица public.orders пуста")
        logging.info("Таблица public.orders содержит %s строк", count)
    finally:
        conn.close()


def _check_no_duplicates():
    """Проверяет отсутствие дубликатов order_id в public.orders."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT order_id, COUNT(*) AS cnt
                FROM public.orders
                GROUP BY order_id
                HAVING COUNT(*) > 1
            """)
            duplicates = cur.fetchall()
        if duplicates:
            raise ValueError(f"Обнаружены дубликаты order_id: {duplicates}")
        logging.info("Дубликатов order_id не обнаружено")
    finally:
        conn.close()


def _log_dq_summary():
    """Логирует итоговую сводку по качеству данных."""
    logging.info("Все проверки качества данных пройдены успешно!")
    logging.info("Качество данных в таблице orders соответствует требованиям.")


default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="csv_to_postgres_dq",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "postgres", "quality", "csv", "dq"],
    description="Проверки качества данных после CSV -> public.orders в Postgres",
) as dag:
    check_exists = PythonOperator(
        task_id="check_orders_table_exists",
        python_callable=_check_table_exists,
    )

    check_schema = PythonOperator(
        task_id="check_orders_schema",
        python_callable=_check_schema,
    )

    check_has_rows = PythonOperator(
        task_id="check_orders_has_rows",
        python_callable=_check_has_rows,
    )

    check_no_duplicates = PythonOperator(
        task_id="check_order_duplicates",
        python_callable=_check_no_duplicates,
    )

    dq_summary = PythonOperator(
        task_id="data_quality_summary",
        python_callable=_log_dq_summary,
    )

    check_exists >> check_schema >> check_has_rows >> check_no_duplicates >> dq_summary
