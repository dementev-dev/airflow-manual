# Repository Guidelines

This repository contains an educational manual for Apache Airflow with runnable examples. The root holds the written guides; `airflow-docker/` provides a self-contained Docker setup to run and explore DAGs.

## Project Structure & Module Organization
- Root `01-09 *.md`: step-by-step articles (RU).
- `_attachments/`: images and GIFs used by the docs.
- `airflow-docker/`: Dockerized Airflow environment.
  - `docker-compose.yml`, `Dockerfile`, `requirements.txt`.
  - `dags/`: Python DAG examples (e.g., `hello_world_dag.py`).
  - `data/`: sample input data for exercises.
- Note: `airflow-docker/AGENTS.md` adds extra, folder-specific guidance and takes precedence there.

## Build, Test, and Development Commands
Prerequisite: Docker + Docker Compose.
- Build images: `cd airflow-docker && docker-compose build`
- Initialize Airflow DB and admin: `docker-compose run --rm airflow-init`
- Start services: `docker-compose up -d airflow-webserver airflow-scheduler`
- Web UI: http://localhost:8080 (admin/admin)
- List DAGs: `docker-compose exec airflow-webserver airflow dags list`
- Follow scheduler logs: `docker-compose logs -f airflow-scheduler`
- Stop: `docker-compose down` (add `-v` to drop volumes; this deletes data).

## Coding Style & Naming Conventions
- Language: Python 3; follow PEP 8; 4-space indentation.
- DAG files: name as `*_dag.py` (e.g., `data_processing_dag.py`).
- Names: `snake_case` for functions/variables/tasks, `UPPER_CASE` for constants.
- Structure: keep imports grouped (stdlib, third-party, local). Prefer docstrings and clear task IDs.

## Testing Guidelines
- Quick checks via Airflow CLI inside containers:
  - Task dry-run: `docker-compose exec airflow-webserver airflow tasks test <dag_id> <task_id> 2024-01-01`
  - Validate DAGs load: `docker-compose exec airflow-webserver airflow dags list`
- For docs, ensure referenced files exist under `_attachments/` and paths render correctly.

## Commit & Pull Request Guidelines
- Commits: short, imperative summary (≤72 chars), optional body explaining why/how. Examples: “Добавить DAG для ветвления”, “Документация: обновить раздел Gantt”.
- Reference issues with `#<id>` when relevant.
- PRs: clear description, steps to validate locally, affected DAGs/docs, and screenshots for doc/UI changes. Keep PRs focused on one logical change.

## Security & Configuration Tips
- Do not commit secrets. Use environment variables and local `.env` files if needed.
- Use `airflow-docker/data/` for sample data; avoid real PII in the repo.

## Hints
- При работе под Windows для работы с командной строкой используй PowerShell
