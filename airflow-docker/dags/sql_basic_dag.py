"""
DAG для демонстрации работы с SQL в Airflow
Уровень: Начальный-Средний
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    'sql_basic_dag',
    default_args=default_args,
    description='DAG для изучения SQL операций в Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['educational', 'sql', 'beginner']
)

# SQL команды
create_table_sql = """
CREATE TABLE IF NOT EXISTS students_sample (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

insert_data_sql = """
INSERT INTO students_sample (name, age) VALUES 
('Alice', 22),
('Bob', 24),
('Charlie', 21)
ON CONFLICT DO NOTHING;
"""

query_data_sql = """
SELECT * FROM students_sample;
"""

truncate_table_sql = """
TRUNCATE TABLE students_sample;
"""

# Определение задач
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_training',  # Соединение создается автоматически в airflow-init
    sql=create_table_sql,
    dag=dag
)

insert_data_task = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_training',
    sql=insert_data_sql,
    dag=dag
)

query_data_task = PostgresOperator(
    task_id='query_data',
    postgres_conn_id='postgres_training',
    sql=query_data_sql,
    dag=dag
)

truncate_table_task = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_training',
    sql=truncate_table_sql,
    dag=dag
)

# Установка зависимостей
create_table_task >> insert_data_task >> query_data_task >> truncate_table_task
