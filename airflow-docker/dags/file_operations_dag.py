"""
DAG для демонстрации работы с файлами в Airflow
Уровень: Средний
"""
import pandas as pd
import os
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
    'file_operations_dag',
    default_args=default_args,
    description='DAG для изучения работы с файлами в Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['educational', 'files', 'intermediate']
)

def generate_sample_data():
    """Создание CSV файла с примерными данными"""
    import pandas as pd
    import random
    
    data = {
        'id': range(1, 101),
        'name': [f'User_{i}' for i in range(1, 101)],
        'age': [random.randint(18, 65) for _ in range(100)],
        'salary': [random.randint(30000, 100000) for _ in range(100)]
    }
    
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/data/input/sample_data.csv', index=False)
    print(f"Создан файл с {len(df)} записями")
    return "sample_data.csv created"

def read_and_validate_data():
    """Чтение и валидация CSV файла"""
    df = pd.read_csv('/opt/airflow/data/input/sample_data.csv')
    print(f"Прочитан файл: {len(df)} строк, {len(df.columns)} столбцов")
    
    # Простая валидация
    assert len(df) > 0, "Файл пустой"
    assert 'name' in df.columns, "Отсутствует столбец name"
    
    return f"Файл валидирован: {len(df)} записей"

def transform_data():
    """Преобразование данных"""
    df = pd.read_csv('/opt/airflow/data/input/sample_data.csv')
    
    # Простое преобразование - добавим столбец с категорией зарплаты
    df['salary_category'] = df['salary'].apply(
        lambda x: 'High' if x >= 70000 else 'Medium' if x >= 50000 else 'Low'
    )
    
    # Сохраняем обработанные данные
    df.to_csv('/opt/airflow/data/output/processed_data.csv', index=False)
    print(f"Обработаны данные: {len(df)} записей")
    
    return f"Данные обработаны: {len(df)} записей"

def write_summary():
    """Создание сводки по обработанным данным"""
    df = pd.read_csv('/opt/airflow/data/output/processed_data.csv')
    
    summary = {
        'total_records': len(df),
        'avg_salary': df['salary'].mean(),
        'high_salary_count': len(df[df['salary_category'] == 'High']),
        'medium_salary_count': len(df[df['salary_category'] == 'Medium']),
        'low_salary_count': len(df[df['salary_category'] == 'Low'])
    }
    
    # Сохраняем сводку в текстовый файл
    with open('/opt/airflow/data/output/summary.txt', 'w') as f:
        f.write("Сводка по обработанным данным:\n")
        f.write(f"Всего записей: {summary['total_records']}\n")
        f.write(f"Средняя зарплата: {summary['avg_salary']:.2f}\n")
        f.write(f"Высокая зарплата: {summary['high_salary_count']}\n")
        f.write(f"Средняя зарплата: {summary['medium_salary_count']}\n")
        f.write(f"Низкая зарплата: {summary['low_salary_count']}\n")
    
    print("Создана сводка по данным")
    return "Сводка создана"

# Определение задач
generate_task = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data,
    dag=dag
)

read_task = PythonOperator(
    task_id='read_and_validate_data',
    python_callable=read_and_validate_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

summary_task = PythonOperator(
    task_id='write_summary',
    python_callable=write_summary,
    dag=dag
)

# Установка зависимостей
generate_task >> read_task >> transform_task >> summary_task