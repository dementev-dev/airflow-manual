# AGENTS.md - Guide for AI Agents

## 🎯 Purpose
This file provides guidance for AI agents working on the educational Airflow setup. It contains information about code style, debugging approaches, and project structure.

## 📁 Project Structure Overview

```
airflow-docker/
├── docker-compose.yml          # Docker configuration and environment
├── dags/                       # DAG files for learning
│   ├── hello_world_dag.py      # Basic Python operators
│   ├── sql_basic_dag.py       # SQL operations
│   ├── file_operations_dag.py   # File processing
│   ├── data_processing_dag.py    # ETL pipeline
│   ├── branching_dag.py         # Conditional logic
│   └── error_handling_dag.py    # Error handling
├── data/                        # Data for exercises
│   ├── input/                   # Input data
│   └── output/                  # Processing results
├── logs/                        # Airflow logs
└── README.md                    # Main documentation
```

## 🎓 Learning Progression for Students

### Basic Concepts
- **DAG Structure**: Understanding basic DAG components
- **Python Operators**: PythonOperator, BashOperator basics
- **Task Dependencies**: Setting up task execution order

### Data Integration
- **PostgreSQL Connections**: Database connectivity
- **SQL Operations**: CRUD operations in tasks
- **File Processing**: CSV operations and data transformation

### Advanced Features
- **Conditional Logic**: BranchPythonOperator usage
- **Error Handling**: Task retries and failure management

## 🔧 Code Style Guidelines

### Python Code Style
- Use 4-space indentation
- Follow PEP 8 conventions
- Include comprehensive docstrings in Russian
- Use descriptive variable names in English

### DAG File Structure
```python
"""
Описание DAG на русском языке
Уровень: Начальный/Средний/Продвинутый
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='Описание функциональности DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['educational', 'beginner']
)
```

## 🐛 Debugging Approaches

### Common Issues and Solutions

#### 1. DAG Not Appearing in UI
- Check DAG file location (`dags/` directory)
- Verify Python syntax and imports
- Check scheduler logs: `docker-compose logs airflow-scheduler`
- Ensure DAG has valid start_date

#### 2. Database Connection Errors
- Verify PostgreSQL services are running
- Check connection strings in environment variables
- Confirm database health checks

#### 3. Task Failures
- Check task logs in Airflow UI
- Verify required Python packages are installed
- Check database permissions and credentials

#### 4. Import Errors
- Ensure all required imports are available
- Check `requirements.txt` for missing dependencies

### Log Analysis
```bash
# Airflow scheduler logs
docker-compose logs airflow-scheduler

# Airflow webserver logs
docker-compose logs airflow-webserver

# PostgreSQL logs
docker-compose logs postgres-training
docker-compose logs postgres-metadata
```

## 📚 Key Files to Examine

### Configuration Files
- [`docker-compose.yml`](docker-compose.yml) - Main Docker configuration and environment variables
- [`requirements.txt`](requirements.txt) - Python dependencies

### Sample Data Files
- [`customers.csv`](data/input/customers.csv) - Customer data for exercises
- [`orders.csv`](data/input/orders.csv) - Order data for practical work

## 🛠️ Development Workflow

### Adding New DAGs
1. Create Python file in `dags/` directory
2. Ensure proper DAG structure and imports
3. File will be automatically discovered by scheduler

### Testing Changes
1. Restart services after major changes
2. Monitor scheduler logs for DAG processing
3. Use Airflow UI for monitoring and debugging

## 💡 Best Practices for AI Agents

### When Making Changes
- Always read the file first using `read_file`
- Use `apply_diff` for surgical edits
- Test DAG execution in Airflow UI

## 🎓 Educational Focus Areas

### For Beginners
- Focus on clear, commented code
- Include expected output descriptions
- Provide common pitfalls and solutions

### Code Quality Checks
- Validate DAG structure before deployment
- Test with sample data first
- Provide clear error messages and handling

### Progressive Complexity
- Start with simple print statements
- Progress to database operations
- Advance to error handling and conditional logic

## 🔍 Troubleshooting Checklist

### Before Reporting Issues
- [ ] Services are running: `docker-compose ps`
- [ ] DAG files are in correct location
- [ ] Environment variables are properly set
- [ ] Database connections are established
- [ ] Task dependencies are correctly set
- [ ] Required Python packages are installed
- [ ] DAG syntax is correct
- [ ] No import errors in DAG files

### Performance Monitoring
- Check task execution times in Airflow UI
- Monitor resource usage in Docker
- Review logs for warnings or errors

## 📝 Documentation Standards

### For New DAGs
- Include comprehensive docstring in Russian
- Describe learning objectives clearly
- Provide step-by-step task explanations
