# Основы построения DAG-файлов в Airflow

В предыдущем занятии вы познакомились с пользовательским интерфейсом Airflow и, вероятно, задались вопросами: как создать свой собственный DAG? Где писать код? Какие именно инструкции использовать? В этом уроке вы получите ответы на все эти вопросы, создадите свой первый DAG, изучите базовую структуру кода и сможете загрузить его в систему для запуска и наблюдения за результатами.

# Как устроен код DAG-файла

DAG представляет собой граф вычислений, состоящий из отдельных задач (tasks). На уровне кода DAG‑файл — это обычный Python‑скрипт, который Airflow регулярно импортирует, чтобы «увидеть» ваши пайплайны.

Удобно мысленно разбивать любой DAG‑файл на четыре блока:
1. Импорт необходимых модулей и операторов
2. Настройка параметров и инициализация объекта `DAG`
3. Создание отдельных задач с помощью операторов
4. Определение порядка выполнения задач (задание зависимостей)

Порядок этих блоков важен: как и в любом Python‑коде, нельзя обращаться к объектам, которые ещё не созданы.

Ниже приведён простой пример DAG‑файла. В следующих подразделах мы разберём каждый блок по отдельности, опираясь на этот пример.

```python
# Секция импортов
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Настройки параметров по умолчанию (default_args)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Объявление DAG
dag = DAG(
    dag_id='tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# Объявление задач (operators)
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)

# Задание графа последовательности
t1 >> t2
```

## Импорт необходимых модулей

Все начинается с подключения нужных библиотек и классов. Обязательно импортируйте класс DAG для создания графа вычислений. Затем подключайте операторы, которые понадобятся для описания ваших задач. Также могут потребоваться дополнительные функции или объекты для работы с данными.

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
```

## Настройка параметров DAG

Следующим шагом идет объявление DAG с необходимыми параметрами:

В примере ниже создается DAG с идентификатором "sample_dag", который будет использовать общие параметры, определенные в словаре `default_args`. В данном случае, владелец DAG - это 'data_team', а дата начала выполнения - 1 января 2021 года.

```python
default_args = {
    'start_date': datetime(2021, 1, 1),
    'owner': 'data_team'
}

dag = DAG(
    "sample_dag",
    default_args=default_args,
    schedule_interval=None,
)
```

Обратите внимание на словарь `default_args`. Это стандартная практика для хранения общих параметров, которые будут применяться ко всем задачам в DAG. Такой подход помогает избежать дублирования кода и делает его более читаемым. В примере мы указываем владельца пайплайна и дату начала его работы.

Ключевыми обязательными параметрами являются `owner` и `start_date`. Без них DAG не сможет быть корректно инициализирован и не появится в пользовательском интерфейсе Airflow. Остальные параметры являются опциональными и добавляются по мере необходимости.

Для изучения всех доступных параметров рекомендуем обращаться к официальной документации Airflow.

## Создание задач с помощью операторов

После настройки DAG следует этап создания задач. Для каждого шага обработки данных создается отдельная переменная с соответствующим оператором.

В Airflow существует множество готовых операторов для различных задач:
- `BashOperator` — для выполнения bash-команд
- `PythonOperator` — для запуска Python-функций
- Специализированные операторы для работы с популярными системами обработки данных (например, Apache Spark)

Каждая задача должна иметь уникальный идентификатор (`task_id`), который используется Airflow для отображения задачи в интерфейсе.

Пример создания простых задач:

В этом примере мы создаем две задачи с использованием BashOperator. Первая задача с идентификатором 'show_time' выводит текущее время, а вторая задача с идентификатором 'process_data' имитирует обработку данных с задержкой 3 секунды и возможностью повторного запуска при ошибках (до 2 раз).

```python
# Задача для вывода текущего времени
t1 = BashOperator(
    task_id='show_time',
    bash_command='date',
    dag=dag
)

# Задача для имитации обработки с возможностью повторных попыток
t2 = BashOperator(
    task_id='process_data',
    bash_command='sleep 3',
    retries=2,
    dag=dag
)
```

## Определение последовательности выполнения

Завершающий этап — указание порядка выполнения задач. В Airflow для этого используются стрелочные операторы `>>` и `<<`.

В простейшем случае мы просто строим цепочку:

```python
t1 >> t2 >> t3  # t2 после t1, t3 после t2
```

Параллельные ветки:

```python
t1 >> [t2, t3]   # t2 и t3 стартуют после t1
[t2, t3] >> t4   # t4 стартует после завершения и t2, и t3
```

Можно комбинировать:

```python
t1 >> [t2, t3] >> t4 >> [t5, t6, t7] >> t8
```

Важно: стрелочный синтаксис хорошо работает для случаев
*«одна задача → список задач»* и *«список задач → одна задача»*.

Но он **не умеет** напрямую связывать два списка между собой:

```python
[t2, t3] >> [t5, t6, t7]  # так делать нельзя — будет ошибка
```

Если вам нужно, чтобы **каждая** из задач `t2` и `t3` была предком для **каждой** из задач `t5`, `t6`, `t7`, используйте встроенную функцию `cross_downstream`:

```python
from airflow.models.baseoperator import cross_downstream

cross_downstream(
    from_tasks=[t2, t3],
    to_tasks=[t5, t6, t7],
)
```

Такой код создаст зависимости:

* `t2` → `t5`, `t6`, `t7`
* `t3` → `t5`, `t6`, `t7`

Под капотом `cross_downstream` как раз делает вложенный цикл,
но в коде явно видно, что мы хотим «полный крест» между двумя наборами задач, и не приходится писать ручные `for`-ы — это рекомендованный в документации Apache Airflow подход.

> 💡 В более больших DAG-ах, где таких блоков много, удобнее не оперировать списками, а **группировать задачи в `TaskGroup`** (Task Group). Тогда зависимости задаются уже между группами, а не между отдельными списками задач. Об этом отдельно поговорим в разделе про TaskGroup.

## Полный пример DAG-файла

В этом полном примере мы создаем DAG с более подробной настройкой параметров. Обратите внимание на дополнительные параметры, такие как количество повторных попыток ('retries'), задержка между попытками ('retry_delay'), а также настройки уведомлений по электронной почте.

Вот как выглядит готовый DAG-файл в итоге:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['data@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    "sample_dag",
    default_args=default_args,
    schedule_interval=None,
)

t1 = BashOperator(
    task_id='show_time',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='process_data',
    bash_command='sleep 3',
    retries=2,
    dag=dag
)

t1 >> t2
```

**Важное замечание**: код DAG-файла выполняется Airflow при каждом сканировании директории DAG. Поэтому в нем не следует размещать тяжелые вычисления, чтение больших файлов или запросы к базам данных. DAG-файл должен быть максимально легковесным. Сложные операции следует выносить в отдельные функции и вызывать их через соответствующие операторы.

💡 **Правило**: DAG-файл не должен содержать ресурсоемких операций, загрузки файлов или сложных вычислений.

# Создаем свой первый рабочий пайплайн

Теперь давайте создадим полноценный пайплайн для работы с реальными данными. В качестве примера мы будем использовать датасет с информацией о клиентах, который часто применяется в задачах анализа данных.

Наш пайплайн будет состоять из трех этапов:
1. Загрузка исходного датасета из интернета
2. Создание агрегированной таблицы по регионам и категориям
3. Сохранение результата в базу данных PostgreSQL

Вот полный код нашего DAG, адаптированный под учебный стенд из папки `airflow-docker`:

```python
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from sqlalchemy import create_engine


# Подключение к учебной базе PostgreSQL (postgres-training)
DB_URL = "postgresql://student:student@postgres-training:5432/training"


# Базовые параметры DAG
args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 12, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


def download_titanic_dataset():
    """Загрузка датасета Titanic и сохранение в базу"""
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)

    engine = create_engine(DB_URL)
    df.to_sql('titanic', engine, index=False, if_exists='replace', schema='public')


def pivot_dataset():
    """Построение сводной таблицы и сохранение результата"""
    engine = create_engine(DB_URL)
    titanic_df = pd.read_sql('select * from public.titanic', con=engine)
    
    df = titanic_df.pivot_table(
        index=['Sex'],
        columns=['Pclass'],
        values='Name',
        aggfunc='count'
    ).reset_index()

    df.to_sql('titanic_pivot', engine, index=False, if_exists='replace', schema='public')


dag = DAG(
    dag_id='titanic_pivot',
    schedule_interval=None,
    default_args=args,
)

# Начальная задача для логирования
start = BashOperator(
    task_id='start',
    bash_command='echo "Начинаем выполнение пайплайна!"',
    dag=dag,
)

# Загрузка исходного датасета
create_titanic_dataset = PythonOperator(
    task_id='download_titanic_dataset',
    python_callable=download_titanic_dataset,
    dag=dag,
)

# Преобразование и сохранение сводной таблицы
pivot_titanic_dataset = PythonOperator(
    task_id='pivot_dataset',
    python_callable=pivot_dataset,
    dag=dag,
)

# Последовательность выполнения
start >> create_titanic_dataset >> pivot_titanic_dataset
```

Этот код использует `PythonOperator` для выполнения функций работы с данными: `download_titanic_dataset` загружает исходный датасет и сохраняет его в учебную базу PostgreSQL, а `pivot_dataset` строит сводную таблицу и записывает результат в отдельную таблицу `titanic_pivot`.

## Загрузка DAG в учебную среду

Для тестирования нашего DAG в учебной среде выполните следующие шаги:

1. Сохраните код в файл с расширением `.py` в папке стенда, например `airflow-docker/dags/titanic_pivot_dag.py`.
2. Убедитесь, что стенд запущен:
```bash
cd airflow-docker
docker-compose up -d
```
3. Подождите 30–60 секунд — Airflow автоматически обнаружит новый файл в папке `dags`.
4. Откройте веб-интерфейс Airflow (http://localhost:8080), найдите DAG `titanic_pivot` по идентификатору и запустите его.

После запуска вы можете посмотреть статус задач и логи в интерфейсе Airflow
(подробнее про это — в разделе про пользовательский интерфейс).

В этом уроке вы изучили основную структуру DAG-файлов, создали свой первый рабочий пайплайн,
научились загружать его в учебную среду и запускать для получения результатов.
