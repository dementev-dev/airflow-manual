# Шаблоны, переменные и подключения в Airflow

В этом материале вы познакомитесь с мощными инструментами Airflow для создания гибких и переиспользуемых пайплайнов: динамическими шаблонами, безопасными переменными и централизованными подключениями к внешним системам.

# Динамические шаблоны Airflow (на основе Jinja)

В процессах обработки данных часто возникает необходимость использовать переменные значения, такие как дата выполнения задачи, для фильтрации исходных данных. В сложных сценариях может потребоваться информация о предыдущих успешных запусках или метаданные текущего выполнения. Airflow предоставляет встроенный механизм динамических шаблонов, основанный на языке Jinja, который значительно упрощает работу с такими переменными.

Синтаксис шаблонов интуитивно понятен — переменная заключается в двойные фигурные скобки с пробелами по краям. Для эффективного использования достаточно знать, какое значение возвращает конкретный шаблон, и разместить его в нужном месте кода.

Подробный справочник доступных шаблонов можно найти в [документации Airflow 2.9.2](https://airflow.apache.org/docs/apache-airflow/2.9.2/templates-ref.html#variables).

Каждый пример ниже - отдельный DAG. Для запуска сохраните один блок кода в файл `.py` внутри `airflow-docker/dags/`. Затем найдите DAG в UI и запустите его вручную.

**Передача даты выполнения в задачу:**

В этом примере DAG `template_example` запускается каждые 15 минут. Задача `display_date` выводит логическую дату запуска в формате YYYY-MM-DD через шаблон `{{ ds }}`.
```python
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="template_example",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

t1 = BashOperator(task_id="display_date", bash_command="echo {{ ds }}", dag=dag)
```

**Использование шаблонов для лучшей отслеживаемости задач:**

В этом примере создается DAG с идентификатором "template_tracking_example", который запускается каждые 20 минут. Вторая задача использует шаблон {{ ds }} в команде bash, чтобы явно указывать дату обработки в логах.
```python
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="template_tracking_example",
    schedule_interval="*/20 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

t1 = BashOperator(task_id="show_date", bash_command="echo {{ ds }}", dag=dag)
t2 = BashOperator(task_id="process_for_date", bash_command="echo Processing for {{ ds }}", dag=dag)

t1 >> t2
```

В первом DAG одна задача, во втором - две связанные задачи. В Details выбранной задачи откройте Rendered Templates: вместо `{{ ds }}` будет дата. Та же дата появится в логе после запуска. Привязку операторов к DAG здесь задает `dag=dag`.

# Безопасные переменные (Variables)

Переменные Airflow представляют собой пары "ключ-значение", хранящиеся в метадатабазе системы. Они идеально подходят для хранения конфигурационных параметров, таких как пути к скриптам, имена таблиц или другие настройки, которые должны быть доступны в разных DAG.

Управление переменными осуществляется через веб-интерфейс Airflow (раздел **Admin → Variables**). Через этот раздел можно:

- создавать и редактировать пары «ключ-значение» вручную;
- импортировать набор переменных из JSON-файла;
- удалять больше не нужные настройки.

### Как создать переменную через UI

Интерфейс ниже соответствует Airflow 2.9.x:

1. Откройте веб-интерфейс Airflow и авторизуйтесь под пользователем с правами **Admin**.
2. В верхнем меню выберите **Admin → Variables**.
3. В правом верхнем углу нажмите кнопку **+ Add a new record** (или иконку `+`).
4. В поле **Key** задайте имя переменной.  
   Например, создадим переменную с паролем к учебной БД отчётности PostgreSQL:

   - **Key**: `reporting_db_password`
5. В поле **Value** введите значение.  
   Например:

   - **Value**: `airflow_report_ro`
6. Поле **Description** можно использовать для короткого пояснения, зачем нужна переменная, например:  
   `Пароль read-only к учебной БД отчётности`.
7. Нажмите **Save**.

После сохранения переменная появится в таблице. Значение будет частично скрыто в UI: вместо реального пароля вы увидите `***` — Airflow маскирует секреты в интерфейсе и логах, чтобы их нельзя было случайно подсмотреть.

Теперь эту переменную можно использовать в коде DAG, например:

```python
from airflow.models import Variable

reporting_db_password = Variable.get("reporting_db_password")
```

Для защиты конфиденциальной информации Airflow автоматически маскирует значения переменных, в названии которых содержится слово `secret`, а также ряд других чувствительных паттернов.

Подробнее о переменных — в [официальной документации Airflow](https://airflow.apache.org/docs/apache-airflow/2.9.3/howto/variable.html).

**Пример использования переменной в коде DAG:**

В этом примере создается DAG с идентификатором "variable_example", который использует переменную 'data_storage_path', предварительно сохраненную в Airflow. Значение переменной извлекается с помощью Variable.get() и используется в команде bash для указания пути к данным.

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id="variable_example",
    schedule_interval=None,
    default_args=default_args
)

# Получение значения переменной
data_path = Variable.get('data_storage_path')

task = BashOperator(
    task_id='process_data',
    bash_command=f'echo "Processing data from {data_path}"',
    dag=dag
)
```

Переменные делают код DAG более читаемым и модульным, позволяя легко адаптировать один и тот же пайплайн для разных окружений или сценариев использования.

# Централизованные подключения (Connections)

Подключения в Airflow — это безопасный способ хранения учетных данных и параметров для взаимодействия с внешними системами. Каждое подключение имеет уникальный идентификатор (conn_id) и содержит необходимые параметры: хост, порт, логин, пароль и другие специфичные настройки.

Подключения поддерживают широкий спектр систем:
- Базы данных (PostgreSQL, MySQL, Oracle и др.)
- Облачные хранилища (AWS S3, Google Cloud Storage)
- Системы уведомлений (Email, Telegram)
- И многие другие через Airflow Providers

Оператор получает настройки по идентификатору подключения. Например, `PostgresOperator` использует параметр `postgres_conn_id`. PostgreSQL provider уже установлен в учебном стенде.

Подключения хранятся в базе метаданных Airflow. Пароли и другие чувствительные поля шифруются с помощью Fernet и маскируются в UI и логах.

### Как создать подключение к PostgreSQL через UI

Используйте учебную БД из [README стенда](airflow-docker/README.md#запуск). В контейнере Airflow она доступна по имени сервиса `postgres-training`; адрес `localhost:5432` предназначен для подключения с вашего компьютера.

1. Откройте **Admin > Connections** и нажмите **+ Add a new record**.
2. Заполните форму:

   - **Connection Id**: `my_postgres_conn`. Это имя будет указано в коде DAG.
   - **Connection Type**: `Postgres`
   - **Host**: `postgres-training`
   - **Database / Schema**: `training`
   - **Login**: `student`
   - **Password**: `student`
   - **Port**: `5432`

3. Нажмите **Save**. Кнопка **Test** может быть отключена в конфигурации Airflow; подключение можно проверить запуском DAG ниже.

База `airflow` на сервисе `postgres-metadata` хранит служебные данные Airflow. Для учебных таблиц используйте `training`.

Сохраните пример в `airflow-docker/dags/connection_example_dag.py` и запустите `connection_example` через UI:

```python
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="connection_example",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_user_table",
        sql="""
            CREATE TABLE IF NOT EXISTS public.connection_demo_users (
                user_id INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """,
        postgres_conn_id="my_postgres_conn",
    )
```

Задача должна завершиться успешно. В учебной БД появится пустая таблица `public.connection_demo_users`; повторный запуск сохранит ее. Проверить наличие таблицы можно из SQL-консоли стенда:

```sql
SELECT to_regclass('public.connection_demo_users');
```

Результат - `connection_demo_users`. Если задача упала, откройте ее лог и сверьте Conn Id в коде с именем подключения, затем Host и остальные поля формы. Правка настроек действует при следующем выполнении задачи.

Более подробную информацию о настройке подключений можно найти в
[документации Airflow](https://airflow.apache.org/docs/apache-airflow/2.9.3/howto/connection.html).


# Проверочный список для качественного DAG

После создания DAG задайте себе следующие вопросы для обеспечения его качества и безопасности:

- Сможет ли коллега понять и поддерживать этот DAG в моё отсутствие?
- Содержит ли код чувствительную информацию (логины, пароли, API-ключи)?
- Какие параметры можно вынести в переменные для лучшей гибкости?
- Требуется ли маскировка конфиденциальных значений?
- Используются ли в логике даты или временные метки, которые можно заменить на шаблоны?

Ответы на эти вопросы помогут вам создавать надежные, безопасные и легко поддерживаемые пайплайны в Airflow.
