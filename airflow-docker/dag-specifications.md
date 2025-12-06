# Educational DAG Specifications for Airflow Learning

## Learning Progression Structure

### Level 1: Basic Concepts (Week 1)

#### 1.1 hello_world_dag.py
**Learning Objectives:**
- Understand basic DAG structure
- Learn about PythonOperator
- Understand task dependencies

**DAG Structure:**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello World from Airflow!")

def print_date():
    print(f"Current date: {datetime.now()}")

def print_goodbye():
    print("Goodbye from Airflow!")

# DAG definition with simple tasks
```

**Tasks:**
- `start_task`: Print welcome message
- `date_task`: Print current date/time
- `end_task`: Print goodbye message

**Dependencies:**
start_task → date_task → end_task

#### 1.2 sql_basic_dag.py
**Learning Objectives:**
- Connect to PostgreSQL database
- Execute SQL queries
- Use PostgresOperator

**Connection Hint:** `docker-compose run --rm airflow-init` automatically provisions the `postgres_training` connection via `airflow connections add`, so no manual setup is required. You can verify it with `docker-compose exec airflow-webserver airflow connections get postgres_training`.

**Tasks:**
- `create_table`: Create simple table (users, products)
- `insert_data`: Insert sample records
- `query_data`: Select and display data
- `drop_table`: Clean up (optional)

**SQL Operations:**
```sql
-- Create table
CREATE TABLE IF NOT EXISTS students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    age INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert data
INSERT INTO students (name, age) VALUES 
('Alice', 22),
('Bob', 24),
('Charlie', 21);
```

### Level 2: Intermediate Concepts (Week 2)

#### 2.1 file_operations_dag.py
**Learning Objectives:**
- File system operations
- CSV data processing
- Data transformation

**Tasks:**
- `generate_sample_data`: Create CSV file with random data
- `read_csv_file`: Read and validate data
- `transform_data`: Simple data transformations
- `write_output`: Save processed data

**Sample Data Structure:**
```csv
id,name,department,salary
1,Alice,Engineering,50000
2,Bob,Marketing,45000
3,Charlie,Sales,48000
```

#### 2.2 data_processing_dag.py
**Learning Objectives:**
- ETL pipeline concepts
- Multiple data sources
- Error handling basics

**Tasks:**
- `extract_customers`: Read customer data
- `extract_orders`: Read order data
- `transform_data`: Join and process data
- `load_to_database`: Save results
- `generate_report`: Create summary

### Level 3: Advanced Concepts (Week 3)

#### 3.1 branching_dag.py
**Learning Objectives:**
- Conditional task execution
- BranchPythonOperator
- Decision making in workflows

**Scenario:**
Process data based on file type or data quality

**Tasks:**
- `check_file_type`: Determine processing path
- `process_csv_branch`: For CSV files
- `process_json_branch`: For JSON files
- `merge_results`: Combine outputs

#### 3.2 error_handling_dag.py
**Learning Objectives:**
- Task retries
- Error notifications
- Failure handling

**Tasks:**
- `unreliable_task`: Simulate failures
- `retry_task`: Demonstrate retry mechanism
- `success_handler`: On success callback
- `failure_handler`: On failure callback

### Level 4: Orchestration & Collaboration (Week 4)

#### 4.1 advanced_features_dag.py
**Learning Objectives:**
- Control concurrency with pools and `pool_slots`
- Exchange data between tasks using XCom
- Group related tasks using `TaskGroup`
- Configure email-based alerting on failures

**Scenario:**
Enhanced daily analytics pipeline that reads data from the training database, performs transformations, and writes summaries, while limiting heavy backup tasks via a dedicated pool and sending notifications about pipeline status.

**Tasks:**
- `extract_group`: Use `TaskGroup` to wrap extract tasks (e.g., customers and orders)
- `transform_group`: Aggregate metrics and prepare summary tables
- `load_group`: Simulate loading results back into the training database or files
- `backup_task`: Heavy backup task running in a dedicated pool (e.g., `backup_pool`) with custom `pool_slots`
- `calculate_metrics`: Python task that returns aggregated metrics (pushed to XCom)
- `log_metrics`: Task that reads metrics via `xcom_pull` and logs them or uses them in a template
- `send_notification`: Final notification task (email or log) triggered with `ALL_DONE` semantics

## Sample Data Files

### customers.csv
```csv
customer_id,name,email,join_date
1,Alice Johnson,alice@example.com,2023-01-15
2,Bob Smith,bob@example.com,2023-02-20
3,Charlie Brown,charlie@example.com,2023-03-10
```

### orders.csv
```csv
order_id,customer_id,product,amount,order_date
101,1,Laptop,1200,2023-10-01
102,2,Monitor,300,2023-10-02
103,1,Keyboard,80,2023-10-03
104,3,Mouse,25,2023-10-04
```

### products.csv
```csv
product_id,name,category,price
1,Laptop,Electronics,1200
2,Monitor,Electronics,300
3,Keyboard,Electronics,80
4,Mouse,Electronics,25
```

## Database Schema for Training

### Students Table
```sql
CREATE TABLE students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    enrollment_date DATE,
    grade INTEGER
);
```

### Courses Table
```sql
CREATE TABLE courses (
    course_id SERIAL PRIMARY KEY,
    course_name VARCHAR(100),
    instructor VARCHAR(100),
    credits INTEGER
);
```

### Enrollments Table
```sql
CREATE TABLE enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES students(student_id),
    course_id INTEGER REFERENCES courses(course_id),
    enrollment_date DATE,
    grade CHAR(1)
);
```

## Learning Outcomes by Week

### Week 1: Foundation
- ✅ Understand DAG structure and components
- ✅ Create basic Python tasks
- ✅ Set up task dependencies
- ✅ Run first successful workflow

### Week 2: Integration
- ✅ Connect to databases
- ✅ Execute SQL operations
- ✅ Process file data
- ✅ Build simple ETL pipelines

### Week 3: Advanced Features
- ✅ Implement conditional logic
- ✅ Handle errors and retries
- ✅ Use parameters and templates
- ✅ Monitor and debug workflows

### Week 4: Orchestration & Operations
- ✅ Use pools to control resource usage
- ✅ Share data between tasks via XCom
- ✅ Group tasks using `TaskGroup`
- ✅ Configure alerting and notifications for failures

## Common Pitfalls and Solutions

### Problem: DAG not appearing in UI
**Solution:** Check DAG file location and syntax

### Problem: Database connection errors
**Solution:** Verify connection strings and database availability

### Problem: Task failures
**Solution:** Check logs, implement proper error handling

### Problem: Scheduling issues
**Solution:** Understand cron expressions and execution dates

## Assessment Criteria

### Basic Competency
- Can create simple DAG with 3+ tasks
- Understands task dependencies
- Can run and monitor workflows

### Intermediate Competency
- Can integrate with databases
- Can process file data
- Implements basic error handling

### Advanced Competency
- Uses conditional branching
- Implements proper error handling
- Creates reusable components
- Optimizes workflow performance
