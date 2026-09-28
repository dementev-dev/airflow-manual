"""Пулы и XCom на двух задачах с наблюдаемой длительностью.

До запуска создайте пул training_pool с двумя слотами.
Чтение источников имитируется; подключений и входных файлов не требуется.
"""
import logging
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def read_source(source, rows):
    """Имитирует чтение источника в течение 20 секунд."""
    logging.info("Начало чтения %s", source)
    time.sleep(20)
    logging.info("Чтение %s завершено: %s строк (имитация)", source, rows)
    return rows


def calculate_metrics(ti):
    """Получает небольшие результаты двух задач через XCom."""
    customers = ti.xcom_pull(task_ids="read_customers")
    orders = ti.xcom_pull(task_ids="read_orders")
    return {"customers": customers, "orders": orders}


def log_metrics(ti):
    """Печатает словарь, возвращенный предыдущей задачей."""
    logging.info("Метрики: %s", ti.xcom_pull(task_ids="calculate_metrics"))


with DAG(
    "resource_management_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "student", "retries": 0},
    tags=["educational", "pools", "xcom"],
) as dag:
    customers_task = PythonOperator(
        task_id="read_customers",
        python_callable=read_source,
        op_kwargs={"source": "customers", "rows": 20},
        pool="training_pool",
        pool_slots=2,
    )
    orders_task = PythonOperator(
        task_id="read_orders",
        python_callable=read_source,
        op_kwargs={"source": "orders", "rows": 30},
        pool="training_pool",
        pool_slots=1,
    )
    metrics_task = PythonOperator(
        task_id="calculate_metrics", python_callable=calculate_metrics,
    )
    log_task = PythonOperator(task_id="log_metrics", python_callable=log_metrics)

    [customers_task, orders_task] >> metrics_task >> log_task
