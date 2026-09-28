# Учебный стенд Apache Airflow

Стенд содержит Airflow 2.9.2, учебную PostgreSQL и девять DAG. Он нужен для знакомства с инструментом: запустить пример, изменить его и посмотреть результат. Для работы понадобятся Docker с Compose, браузер и знания Python, pandas и SQL.

## Запуск

Из корня репозитория:

```bash
cd airflow-docker
docker compose up -d --build
```

Команды подходят для Linux и PowerShell в Windows. При первом запуске Docker собирает образ, поднимает две БД, выполняет `airflow-init`, затем запускает webserver и scheduler. Инициализация создает учетную запись и подключение `postgres_training`.

Проверьте состояние:

```bash
docker compose ps -a
docker compose logs airflow-init
```

Завершение `airflow-init` с кодом 0 нормально: это разовая подготовка. Остальные четыре сервиса должны работать. Откройте http://localhost:8080 и войдите как `admin` / `admin`.

| Сервис | Адрес с компьютера | База | Пользователь / пароль |
|---|---|---|---|
| Airflow UI | `http://localhost:8080` | - | `admin` / `admin` |
| Учебная PostgreSQL | `localhost:5432` | `training` | `student` / `student` |
| Метаданные Airflow | `localhost:5434` | `airflow` | `airflow` / `airflow` |

В задачах используется адрес `postgres-training:5432`: это имя сервиса внутри Docker. Базу метаданных для упражнений не используйте. Учебные пароли заданы в Compose; для публичного сервера такая конфигурация не подходит.

## Прохождение практики

Откройте [задания](educational-tasks.md). Их восемь блоков проходят последовательно; перед каждым указаны нужные главы [учебника](../README.md), исходные файлы и настройки.

Основные задания внутри блока сохраняют изменения предыдущих шагов. Дополнительные находятся в конце блока и помечены "По желанию"; их можно пропустить. Для завершения практики достаточно основных заданий и проверки их результатов.

| Блок | DAG |
|---|---|
| [Первый DAG](educational-tasks.md#first-dag) | `hello_world_dag` |
| [SQL и подключения](educational-tasks.md#sql) | `sql_basic_dag` |
| [Работа с файлами](educational-tasks.md#files) | `file_operations_dag` |
| [Загрузка CSV и DQ](educational-tasks.md#load-dq) | `csv_to_postgres`, `csv_to_postgres_dq` |
| [Разбор ETL](educational-tasks.md#etl) | `data_processing_dag` |
| [Ветвление](educational-tasks.md#branching) | `branching_dag` |
| [Ошибки и повторы](educational-tasks.md#errors) | `error_handling_dag` |
| [Пулы, XCom и TaskGroup](educational-tasks.md#resources) | `resource_management_dag` |

Описание поведения исходных примеров находится в [справочнике DAG](dag-specifications.md). Почта нужна только для отдельного задания по желанию; SMTP в стенде не настроен.

## Файлы и данные

```text
airflow-docker/
├── docker-compose.yml       # Сервисы, порты и учебные подключения
├── Dockerfile               # Airflow 2.9.2
├── requirements.txt         # Дополнительные Python-пакеты
├── dags/                    # Девять учебных DAG
├── data/input/              # Входные CSV и данные упражнений
├── data/output/             # Результаты задач
├── init-postgres.sql        # Начальная подготовка учебной БД
└── educational-tasks.md     # Маршрут и условия заданий
```

`dags/` и `data/` смонтированы в контейнеры как `/opt/airflow/dags/` и `/opt/airflow/data/`. Правки DAG подхватываются планировщиком; перед новым запуском дождитесь обновления вкладки Code. Резервные копии Python-файлов храните вне `dags/`, чтобы не создать два определения одного `dag_id`.

Таблицы PostgreSQL сохраняются в именованных томах `pg_data` и `pgmeta`. Обычная остановка их не удаляет. Логи Airflow хранятся внутри контейнеров; важные логи опыта сохраните до удаления контейнеров.

Для SQL-запросов из заданий:

```bash
docker compose exec postgres-training psql -U student -d training
```

Выход из psql - `\q`. Готовое подключение Airflow можно проверить командой:

```bash
docker compose exec airflow-webserver airflow connections get postgres_training
```

## Управление и поиск ошибок

Все команды ниже выполняются из `airflow-docker`.

```bash
docker compose exec airflow-webserver airflow version
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-import-errors
docker compose logs airflow-scheduler
docker compose logs airflow-webserver
```

Если DAG не появился, проверьте путь файла и ошибки импорта. Если задача упала, сначала откройте ее лог в UI. При ошибке подключения проверьте `docker compose ps` и Conn Id задачи. Задачи из блока 8 ждут, пока вы создадите указанный в задании пул.

Остановка с сохранением данных:

```bash
docker compose down
```

Повторный запуск - `docker compose up -d`. Для полного сброса обеих БД используйте `docker compose down -v`: эта команда удалит учебные таблицы, историю запусков и настройки Airflow в томах. Файлы в `data/` останутся.

## Для авторов курса

[План обновления практики](../docs/specs/2026-09-28-practice-improvement-plan.md) сохраняет согласованные решения, разбор исходных заданий и результаты проверок.
