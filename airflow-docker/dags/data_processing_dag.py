"""
DAG для демонстрации ETL процессов в Airflow
Уровень: Средний-Продвинутый

Этот DAG реализует классический ETL-пайплайн:
1. Создание тестовых данных (клиенты + заказы) в CSV
2. Параллельное извлечение (Extract) клиентов и заказов
3. Трансформация (Transform) — объединение таблиц, вычисляемые поля
4. Загрузка (Load) — подготовка SQL для вставки в БД
5. Генерация текстового отчёта со статистикой

Результат: файлы в /opt/airflow/data/output/ (extracted_*, transformed_data.csv, report.txt).
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
    'data_processing_dag',
    default_args=default_args,
    description='DAG для изучения ETL процессов в Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['educational', 'etl', 'intermediate']
)

def create_sample_data():
    """Создание примерных данных для ETL процесса"""
    import pandas as pd
    # Создаем файлы с данными клиентов и заказов
    customers_data = {
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Wilson'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com', 'eve@example.com'],
        'join_date': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12']
    }
    
    orders_data = {
        'order_id': [101, 102, 103, 104, 105, 106],
        'customer_id': [1, 2, 1, 3, 4, 5],
        'product': ['Laptop', 'Monitor', 'Keyboard', 'Mouse', 'Tablet', 'Headphones'],
        'amount': [1200, 300, 80, 25, 400, 100],
        'order_date': ['2023-10-01', '2023-10-02', '2023-10-03', '2023-10-04', '2023-10-05', '2023-10-06']
    }
    
    # Сохраняем в CSV
    pd.DataFrame(customers_data).to_csv('/opt/airflow/data/input/customers.csv', index=False)
    pd.DataFrame(orders_data).to_csv('/opt/airflow/data/input/orders.csv', index=False)
    
    print("Созданы файлы с данными клиентов и заказов")
    return "Sample data created"

def extract_customers():
    """Извлечение данных клиентов"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/input/customers.csv')
    print(f"Извлечено {len(df)} записей клиентов")
    
    # Сохраняем извлеченные данные
    df.to_csv('/opt/airflow/data/output/extracted_customers.csv', index=False)
    return f"Извлечено {len(df)} клиентов"

def extract_orders():
    """Извлечение данных заказов"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/input/orders.csv')
    print(f"Извлечено {len(df)} записей заказов")
    
    # Сохраняем извлеченные данные
    df.to_csv('/opt/airflow/data/output/extracted_orders.csv', index=False)
    return f"Извлечено {len(df)} заказов"

def transform_data():
    """Преобразование данных - объединение клиентов и заказов"""
    import pandas as pd
    customers_df = pd.read_csv('/opt/airflow/data/input/customers.csv')
    orders_df = pd.read_csv('/opt/airflow/data/input/orders.csv')
    
    # Объединяем данные
    merged_df = pd.merge(orders_df, customers_df, on='customer_id', how='left')
    
    # Добавляем вычисляемые поля
    merged_df['total_spent'] = merged_df['amount']
    merged_df['order_month'] = pd.to_datetime(merged_df['order_date']).dt.month
    
    # Сохраняем преобразованные данные
    merged_df.to_csv('/opt/airflow/data/output/transformed_data.csv', index=False)
    print(f"Преобразованы данные: {len(merged_df)} записей")
    
    return f"Преобразованы {len(merged_df)} записей"

def load_to_database():
    """Загрузка данных в базу данных (симуляция)"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/output/transformed_data.csv')
    
    # В реальном сценарии здесь был бы код для загрузки в базу данных
    # Для учебных целей просто логируем
    print(f"Загружено в базу данных: {len(df)} записей")
    
    # Создаем SQL для создания таблицы (в реальном сценарии)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customer_orders (
        order_id INTEGER,
        customer_id INTEGER,
        product VARCHAR(100),
        amount DECIMAL(10,2),
        order_date DATE,
        name VARCHAR(100),
        email VARCHAR(100),
        join_date DATE,
        total_spent DECIMAL(10,2),
        order_month INTEGER
    );
    """
    
    print("SQL для создания таблицы:")
    print(create_table_sql)
    
    return f"Подготовлено к загрузке в базу: {len(df)} записей"

def generate_report():
    """Генерация отчета"""
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/output/transformed_data.csv')
    
    # Создаем простой отчет
    report = {
        'total_orders': len(df),
        'total_revenue': df['amount'].sum(),
        'avg_order_value': df['amount'].mean(),
        'unique_customers': df['customer_id'].nunique(),
        'top_customer': df.groupby('name')['amount'].sum().idxmax(),
        'top_customer_spending': df.groupby('name')['amount'].sum().max()
    }
    
    # Сохраняем отчет в файл
    with open('/opt/airflow/data/output/report.txt', 'w') as f:
        f.write("Отчет по заказам клиентов\n")
        f.write("=" * 30 + "\n")
        f.write(f"Всего заказов: {report['total_orders']}\n")
        f.write(f"Общая выручка: ${report['total_revenue']}\n")
        f.write(f"Средний чек: ${report['avg_order_value']:.2f}\n")
        f.write(f"Уникальных клиентов: {report['unique_customers']}\n")
        f.write(f"Лучший клиент: {report['top_customer']} (${report['top_customer_spending']})\n")
    
    print("Создан отчет по заказам")
    return "Отчет создан"

# Определение задач
create_data_task = PythonOperator(
    task_id='create_sample_data',
    python_callable=create_sample_data,
    dag=dag
)

extract_customers_task = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers,
    dag=dag
)

extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

# Установка зависимостей
create_data_task >> [extract_customers_task, extract_orders_task] >> transform_task >> load_task >> report_task