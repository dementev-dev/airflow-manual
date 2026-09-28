"""Повторы и обработчики результата. Режим задается Variable error_mode.

retry_once: вторая задача падает при первой попытке, затем выполняется.
success: обе задачи выполняются сразу.
fail_first / fail_second / fail_both: выбранные задачи падают при каждой попытке.
Меняйте режим после завершения предыдущего запуска.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def get_error_mode():
    """Читает режим во время выполнения задачи."""
    mode = Variable.get("error_mode", default_var="retry_once")
    if mode not in {"success", "retry_once", "fail_first", "fail_second", "fail_both"}:
        raise ValueError(f"Неизвестный error_mode: {mode}")
    return mode


def run_first_task():
    """Имитирует первую рабочую задачу."""
    if get_error_mode() in {"fail_first", "fail_both"}:
        raise ValueError("Учебная ошибка первой задачи")
    logging.info("Первая задача выполнена")


def run_retry_task(ti):
    """Имитирует временную или постоянную ошибку второй задачи."""
    mode = get_error_mode()
    logging.info("Режим: %s; попытка: %s", mode, ti.try_number)
    if mode in {"fail_second", "fail_both"}:
        raise ValueError("Постоянная учебная ошибка второй задачи")
    if mode == "retry_once" and ti.try_number == 1:
        raise ValueError("Временная учебная ошибка: следующая попытка выполнится")
    logging.info("Вторая задача выполнена")


def handle_success():
    """Сообщает об успехе обеих рабочих задач."""
    logging.info("Обе рабочие задачи выполнены")


def handle_failure():
    """Сообщает об окончательном отказе хотя бы одной задачи."""
    logging.error("Рабочая задача исчерпала повторы. Откройте ее лог")


with DAG(
    "error_handling_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "student",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["educational", "errors"],
) as dag:
    start_task = EmptyOperator(task_id="start_task")
    unreliable_task = PythonOperator(
        task_id="unreliable_task", python_callable=run_first_task,
    )
    retry_task = PythonOperator(
        task_id="retry_task", python_callable=run_retry_task,
    )
    success_handler_task = PythonOperator(
        task_id="success_handler",
        python_callable=handle_success,
        trigger_rule="all_success",
        retries=0,
    )
    failure_handler_task = PythonOperator(
        task_id="failure_handler",
        python_callable=handle_failure,
        trigger_rule="one_failed",
        retries=0,
    )

    start_task >> [unreliable_task, retry_task]
    [unreliable_task, retry_task] >> success_handler_task
    [unreliable_task, retry_task] >> failure_handler_task
    # Оба обработчика завершающие. При отказе success_handler получает
    # upstream_failed, поэтому успешное уведомление не скрывает ошибку DAG Run.
