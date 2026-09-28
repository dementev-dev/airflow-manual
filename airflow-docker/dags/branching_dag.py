"""Выбор ветки через Variable branch_format: csv или json.

Обработка файлов имитируется. В основной практике изучаются граф и статусы.
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


def choose_format():
    """Возвращает task_id выбранной ветки."""
    file_format = Variable.get("branch_format", default_var="csv")
    branches = {"csv": "process_csv_branch", "json": "process_json_branch"}
    if file_format not in branches:
        raise ValueError(
            f"branch_format должен быть csv или json, получено: {file_format}"
        )
    logging.info("Выбран формат: %s", file_format)
    return branches[file_format]


def process_csv_data():
    """Имитирует обработку CSV."""
    logging.info("Учебная CSV-ветка выполнена; файлы не читались")
    return "csv"


def process_json_data():
    """Имитирует обработку JSON."""
    logging.info("Учебная JSON-ветка выполнена; файлы не читались")
    return "json"


def merge_results(ti):
    """Читает результат выполненной ветки текущего запуска."""
    results = ti.xcom_pull(task_ids=["process_csv_branch", "process_json_branch"])
    selected = [value for value in results if value is not None]
    logging.info("Результат выбранной ветки: %s", selected)
    return selected


with DAG(
    "branching_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "student", "retries": 0},
    tags=["educational", "branching"],
) as dag:
    start_task = EmptyOperator(task_id="start_task")
    choose_task = BranchPythonOperator(
        task_id="choose_format", python_callable=choose_format,
    )
    process_csv_task = PythonOperator(
        task_id="process_csv_branch", python_callable=process_csv_data,
    )
    process_json_task = PythonOperator(
        task_id="process_json_branch", python_callable=process_json_data,
    )
    merge_task = PythonOperator(
        task_id="merge_results",
        python_callable=merge_results,
        trigger_rule="none_failed_min_one_success",
    )
    end_task = EmptyOperator(task_id="end_task")

    start_task >> choose_task >> [process_csv_task, process_json_task]
    [process_csv_task, process_json_task] >> merge_task >> end_task
