# Educational Airflow Setup for Beginners

Простой учебный стенд Apache Airflow для начинающих, изучающих SQL и Python.

## 🎯 Цель проекта

Создать максимально простую среду для изучения Apache Airflow.
Для удобства все переменные окружения захардкожены в docker скрипты. На проде так делать не надо :)

## 📋 Предварительные требования

- Docker и Docker Compose
- Базовые знания Python и SQL
- Веб-браузер для доступа к интерфейсу Airflow

## 🚀 Быстрый старт

### 1. Запуск стенда

```bash
# Перейдите в директорию проекта
cd airflow-docker

# Запустите все сервисы
docker-compose up -d
```

### 2. Доступ к интерфейсам

- **Airflow UI**: http://localhost:8080
  - Логин: `admin`
  - Пароль: `admin`

- **PostgreSQL для тренировок**: `localhost:5432`
  - База данных: `training`
  - Пользователь: `student`
  - Пароль: `student`

- **PostgreSQL для метаданных Airflow**: `localhost:5433`
  - База данных: `airflow`
  - Пользователь: `airflow`
  - Пароль: `airflow`

## 🏗️ Архитектура стенда

```mermaid
graph TB
    A[Airflow Webserver] --> B[PostgreSQL Metadata]
    C[Airflow Scheduler] --> B
    D[Educational DAGs] --> E[PostgreSQL Training]
    D --> F[Local Filesystem]
    
    A -- порт 8080 --> G[Пользователь]
    E -- порт 5432 --> H[SQL Клиент]
    
    subgraph "Контейнеры Docker"
        A
        C
        B
        E
    end
    
    subgraph "Локальная файловая система"
        F
        D
    end
```

## 📁 Структура проекта

```
airflow-docker/
├── docker-compose.yml          # Конфигурация Docker
├── .env                        # Файл не используется: переменные заданы в docker-compose.yml
├── dags/                       # DAG файлы для обучения
│   ├── hello_world_dag.py      # Базовый пример
│   ├── sql_basic_dag.py        # Работа с SQL
│   ├── file_operations_dag.py  # Обработка файлов
│   ├── data_processing_dag.py  # ETL пайплайн
│   ├── branching_dag.py        # Условная логика
│   └── error_handling_dag.py   # Обработка ошибок
├── data/                       # Данные для упражнений
│   ├── input/                  # Входные данные
│   └── output/                 # Результаты обработки
├── logs/                       # Логи Airflow
└── README.md                   # Эта инструкция
```

## 🎓 Учебные материалы

### Основы Airflow

**Цели:**
- Понимание структуры DAG
- Создание простых задач
- Настройка зависимостей между задачами

**Примеры DAG:**
- `hello_world_dag.py` - базовые операторы Python
- `sql_basic_dag.py` - работа с базами данных

### Интеграция с данными

**Цели:**
- Подключение к PostgreSQL
- Выполнение SQL запросов
- Обработка файлов CSV

**Примеры DAG:**
- `file_operations_dag.py` - работа с файлами
- `data_processing_dag.py` - ETL процессы

### Продвинутые возможности

**Цели:**
- Условное выполнение задач
- Обработка ошибок
- Параметризация workflows

**Примеры DAG:**
- `branching_dag.py` - условная логика
- `error_handling_dag.py` - обработка ошибок

## 🔧 Технические детали

### Переменные окружения

В этом проекте мы не используем `.env`: все значения заданы напрямую в `docker-compose.yml`.

Сервисы Airflow получают:
- `POSTGRES_TRAINING_HOST=postgres-training`
- `POSTGRES_TRAINING_PORT=5432`
- `POSTGRES_TRAINING_DB=training`
- `POSTGRES_TRAINING_USER=student`
- `POSTGRES_TRAINING_PASSWORD=student`
- `AIRFLOW_CONN_POSTGRES_TRAINING=postgresql://student:student@postgres-training:5432/training`

`docker-compose run --rm airflow-init` запускает `airflow connections add postgres_training`, поэтому соединение доступно сразу после инициализации. Проверить наличие можно через:

```bash
docker-compose exec airflow-webserver airflow connections get postgres_training
```


### Порты

- `8080` - Airflow Webserver
- `5432` - PostgreSQL для тренировок
- `5433` - PostgreSQL для метаданных Airflow

## 🛠️ Управление стендом

### Запуск сервисов
```bash
docker-compose up -d
```

### Остановка сервисов
```bash
docker-compose down
```

### Просмотр логов
```bash
# Логи Airflow
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Логи PostgreSQL
docker-compose logs postgres-training
docker-compose logs postgres-metadata
```

### Перезапуск конкретного сервиса
```bash
docker-compose restart airflow-webserver
```

## 🐛 Решение проблем

### DAG не появляется в интерфейсе
- Проверьте, что файл находится в папке `dags/`
- Убедитесь в правильности синтаксиса Python
- Проверьте логи планировщика: `docker-compose logs airflow-scheduler`

### Ошибки подключения к базе данных
- Убедитесь, что PostgreSQL запущен: `docker-compose ps`
- Проверьте логи PostgreSQL: `docker-compose logs postgres-training`

### Задачи завершаются с ошибкой
- Проверьте логи задачи в интерфейсе Airflow
- Убедитесь в наличии необходимых Python пакетов

## 📚 Дополнительные ресурсы

- [Официальная документация Airflow](https://airflow.apache.org/docs/)
- [Учебные материалы в родительской папке](../)

---

**Примечание**: Этот стенд предназначен исключительно для учебных целей. Для production использования требуется дополнительная настройка безопасности.
