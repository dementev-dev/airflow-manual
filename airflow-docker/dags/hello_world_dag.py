"""
Простой DAG для демонстрации основных концепций Airflow
Уровень: Начальный
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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
    'hello_world_dag',
    default_args=default_args,
    description='Простой DAG для изучения основ Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['educational', 'beginner']
)

# Функции для задач
def print_hello():
    print("Hello World from Airflow!")
    return 'Hello World!'

def print_date():
    print(f"Current date: {datetime.now()}")
    return f"Date: {datetime.now()}"

def print_goodbye():
    print("Goodbye from Airflow!")
    return 'Goodbye!'

# Определение задач
start_task = PythonOperator(
    task_id='start_task',
    python_callable=print_hello,
    dag=dag
)

date_task = PythonOperator(
    task_id='date_task',
    python_callable=print_date,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=print_goodbye,
    dag=dag
)

# Установка зависимостей
start_task >> date_task >> end_task