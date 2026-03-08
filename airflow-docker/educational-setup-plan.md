# Educational Airflow Setup Plan for Beginners

## Current Issues Identified

### 1. Docker Compose Structure Problems
- Duplicate `services:` sections in [`docker-compose.yml`](airflow-docker/docker-compose.yml:1,21)
- Inconsistent container naming

### 2. Missing Directory Structure
- No `airflow/dags/` directory
- No `data/` directory for sample files
- No initialization scripts

## Proposed Simple Educational Setup

### Core Components

#### 1. Fixed Docker Compose Structure
```yaml
services:
  # PostgreSQL for Airflow metadata
  postgres-metadata:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5434:5432"
    volumes:
      - pgmeta:/var/lib/postgresql/data

  # PostgreSQL for training exercises
  postgres-training:
    image: postgres:16
    environment:
      POSTGRES_USER: student
      POSTGRES_PASSWORD: student
      POSTGRES_DB: training
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data

  # Airflow services
  airflow-webserver:
    image: apache/airflow:2.9.2
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-metadata:5432/airflow
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data

  airflow-scheduler:
    image: apache/airflow:2.9.2
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-metadata:5432/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data

  airflow-init:
    image: apache/airflow:2.9.2
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-metadata:5432/airflow
```

#### 2. Hardcoded Environment Variables
Create `.env` file with all variables hardcoded:
- Airflow admin: admin/admin
- PostgreSQL metadata: airflow/airflow
- PostgreSQL training: student/student

#### 3. Educational DAG Examples

**Level 1: Basic Concepts**
- `hello_world_dag.py` - Simple print statements
- `sql_basic_dag.py` - Basic SQL operations
- `file_operations_dag.py` - CSV file processing

**Level 2: Intermediate**
- `csv_to_postgres.py` - CSV to PostgreSQL pipeline with data quality checks
- `data_processing_dag.py` - ETL pipeline with multiple steps
- `branching_dag.py` - Conditional task execution

#### 4. Sample Data Structure
```
data/
├── input/
│   ├── customers.csv
│   ├── orders.csv
│   └── products.csv
├── output/
└── logs/
```

#### 5. Learning Progression

**Week 1: Airflow Basics**
- DAG structure and syntax
- Basic operators (PythonOperator, BashOperator)
- Task dependencies

**Week 2: SQL Integration**
- PostgreSQL connections
- SQL execution in tasks
- Data transformation

**Week 3: Real-world Scenarios**
- Error handling and retries
- Parameter passing
- Scheduling

### Implementation Steps

1. **Fix Docker Compose** - Remove duplicate sections, add missing services
2. **Create .env file** - Hardcode all environment variables
3. **Setup directories** - Create dags/, data/input/, data/output/
4. **Create sample DAGs** - From simple to complex
5. **Add sample data** - CSV files for practical exercises
6. **Create requirements.txt** - Essential Python packages
7. **Update documentation** - Clear step-by-step instructions
8. **Test setup** - Ensure everything works end-to-end

### Key Educational Principles

- **Simplicity First** - Start with minimal configuration
- **Progressive Complexity** - Build skills step by step
- **Practical Focus** - Real data processing tasks
- **Immediate Feedback** - Students see results quickly

### Sample Exercise Structure

Each DAG will include:
- Clear comments explaining each component
- Step-by-step task definitions
- Expected output descriptions
- Common pitfalls and solutions

### Technical Requirements

- Docker and Docker Compose
- Basic Python knowledge
- Basic SQL knowledge
- Web browser for Airflow UI
