"""
DAG для демонстрации условного выполнения задач в Airflow
Уровень: Продвинутый
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import random

# Определение DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'branching_dag',
    default_args=default_args,
    description='DAG для изучения условного выполнения задач в Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['educational', 'branching', 'advanced']
)

def check_data_quality():
    """Проверка качества данных - случайным образом определяет, какие данные использовать"""
    # В реальном сценарии здесь будет проверка качества данных
    # Для учебных целей просто случайное решение
    quality_score = random.random()  # случайное число от 0 до 1
    
    if quality_score > 0.5:
        print(f"Качество данных хорошее (оценка: {quality_score:.2f}), используем CSV")
        return 'process_csv_branch'
    else:
        print(f"Качество данных требует внимания (оценка: {quality_score:.2f}), используем JSON")
        return 'process_json_branch'

def process_csv_data():
    """Обработка CSV данных"""
    print("Обработка CSV файла...")
    # Здесь будет логика обработки CSV файла
    return "CSV данные обработаны"

def process_json_data():
    """Обработка JSON данных"""
    print("Обработка JSON файла...")
    # Здесь будет логика обработки JSON файла
    return "JSON данные обработаны"

def merge_results():
    """Объединение результатов из разных веток"""
    print("Объединение результатов из разных веток...")
    return "Результаты объединены"

# Определение задач
start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

check_quality_task = BranchPythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

process_csv_task = PythonOperator(
    task_id='process_csv_branch',
    python_callable=process_csv_data,
    dag=dag
)

process_json_task = PythonOperator(
    task_id='process_json_branch',
    python_callable=process_json_data,
    dag=dag
)

merge_task = PythonOperator(
    task_id='merge_results',
    python_callable=merge_results,
    trigger_rule='none_failed_or_skipped',  # Выполняется, когда одна из веток завершена
    dag=dag
)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag
)

# Установка зависимостей
start_task >> check_quality_task
check_quality_task >> [process_csv_task, process_json_task]
[process_csv_task, process_json_task] >> merge_task >> end_task