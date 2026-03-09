# Contributing to E-commerce Sales Performance Pipeline

Thank you for your interest in contributing! Here's how to get started.

## Development Setup

```bash
# 1. Fork and clone the repo
git clone https://github.com/<your-username>/ecommerce_pipeline.git
cd ecommerce_pipeline

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy environment file
cp .env.example .env
```

## Running Tests

```bash
# Unit + integration tests (no Docker required)
pytest tests/ -v --ignore=tests/test_dag.py

# With coverage
pytest tests/ -v --cov=etl --cov-report=term-missing --ignore=tests/test_dag.py

# Full suite (requires Airflow installed)
pytest tests/ -v
```

Tests must pass (49/49) before any PR is merged.

## Code Style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting:

```bash
pip install ruff
ruff check etl/ dags/ tests/ dashboard/
```

## Branch Strategy

| Branch | Purpose |
|--------|---------|
| `main` | Production-ready, protected |
| `develop` | Integration branch |
| `feature/<name>` | New features |
| `fix/<name>` | Bug fixes |

## Pull Request Checklist

- [ ] All tests pass (`pytest tests/ -v --ignore=tests/test_dag.py`)
- [ ] Ruff linting passes (`ruff check etl/ dags/ tests/`)
- [ ] New code has corresponding tests
- [ ] `README.md` updated if behaviour changed
- [ ] No secrets or `.env` files committed

## Project Structure

```
ecommerce_pipeline/
├── .github/workflows/   # CI/CD (GitHub Actions)
├── sql/                 # Schema DDL + seed scripts
├── etl/                 # Python ETL scripts + utilities
├── dags/                # Airflow DAG definitions
├── dashboard/           # Streamlit showcase app
├── tests/               # Pytest test suite (49 tests)
├── data/raw/            # Generated CSVs (git-ignored)
├── docker-compose.yml   # Full Airflow + PostgreSQL stack
└── README.md
```

## Reporting Issues

Please open a GitHub Issue with:
- A clear title and description
- Steps to reproduce
- Expected vs. actual behaviour
- Python version and OS
