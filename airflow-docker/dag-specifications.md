# Что делают исходные DAG

Это справочник примеров для Airflow 2.9.2. Порядок прохождения, изменения кода и условия завершения находятся в [практических заданиях](educational-tasks.md).

| DAG и файл в `dags/` | Подготовка | Поведение и результат |
|---|---|---|
| `hello_world_dag` (`hello_world_dag.py`) | Не требуется | Три Python-задачи печатают приветствие, дату и завершение. Расписание ежедневное, `catchup=False`. |
| `sql_basic_dag` (`sql_basic_dag.py`) | Connection `postgres_training`, создаваемый при запуске стенда | Создает `public.students_sample`, очищает перед вставкой, добавляет трех студентов и выполняет SELECT. После каждого полного запуска в таблице три строки. |
| `file_operations_dag` (`file_operations_dag.py`) | Каталоги `data/input`, `data/output` из репозитория | Генерирует 100 строк, проверяет данные, сохраняет `output/processed_data.csv` и `output/summary.txt`. Категория High начинается с 70000. |
| `csv_to_postgres` (`csv_to_postgres.py`) | Connection `postgres_training` | Создает `public.orders`, генерирует CSV на 1000 строк, передает путь через XCom и загружает файл. Уже существующие `order_id` пропускаются; новый полный запуск генерирует новые заказы. |
| `csv_to_postgres_dq` (`csv_to_postgres_dq.py`) | Успешная загрузка `csv_to_postgres`; запускать вручную после нее | Проверяет таблицу, точную схему из четырех колонок, наличие строк и отсутствие дубликатов. Сводка перечисляет состояния проверок. По умолчанию после ошибки последующие задачи не выполняются (`all_success`). |
| `data_processing_dag` (`data_processing_dag.py`) | Не требуется, свои входные CSV создаются заново | Извлекает 5 клиентов и 6 заказов в `output/extracted_*.csv`, читает их при преобразовании. `simulate_load` печатает число строк и DDL без записи в БД. Отчет: 6 заказов, выручка 2105. |
| `branching_dag` (`branching_dag.py`) | Variable `branch_format`: `csv` или `json`; по умолчанию `csv` | Выбранная ветка имитирует обработку, другая получает `skipped`. Слияние читает XCom текущего запуска с `none_failed_min_one_success`. Неизвестный формат приводит к ошибке. |
| `error_handling_dag` (`error_handling_dag.py`) | Variable `error_mode`, по умолчанию `retry_once` | Две рабочие задачи, по одному повтору с задержкой 10 секунд. Оба обработчика зависят от обеих задач. Окончательный отказ сохраняет `failed` у DAG Run, даже если обработчик ошибки успешен. |
| `resource_management_dag` (`resource_management_dag.py`) | Создать `training_pool` с 2 слотами | Две задачи по 20 секунд имитируют чтение и занимают 2 и 1 слот, поэтому работают последовательно. Через XCom передают 20 и 30; следующая задача возвращает словарь, последняя печатает его. |

Все DAG, кроме `hello_world_dag`, запускаются вручную. Связи между загрузчиком CSV и DQ-DAG нет. Файлы из `input/orders.csv` в ETL-примере и таблица `public.orders` в загрузчике относятся к разным блокам.

## Режимы примера с ошибками

| `error_mode` | Рабочие задачи | Обработчики | DAG Run |
|---|---|---|---|
| `success` | Обе успешны с первой попытки | Успех: `success`, ошибка: `skipped` | `success` |
| `retry_once` | `retry_task` падает один раз, затем успешна | Успех: `success`, ошибка: `skipped` | `success` |
| `fail_first` | `unreliable_task` окончательно падает | Успех: `upstream_failed`, ошибка: `success` | `failed` |
| `fail_second` | `retry_task` окончательно падает | Успех: `upstream_failed`, ошибка: `success` | `failed` |
| `fail_both` | Обе окончательно падают | Успех: `upstream_failed`, ошибка: `success` | `failed` |

Меняйте Variable после завершения предыдущего запуска. `retries=1` означает две попытки всего. `failure_handler` использует `one_failed`: достаточно одного окончательного отказа, ждать завершения другой рабочей задачи ему не обязательно.
