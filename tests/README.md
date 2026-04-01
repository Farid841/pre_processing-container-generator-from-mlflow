# Tests

Unit and contract tests for the preprocessing pipeline.

## Setup

```bash
pdm install -dG test
```

## Running tests

```bash
# Unit tests only (excludes integration)
pdm run test

# All tests including integration
pdm run test-all

# Specific file
pdm run pytest tests/test_preprocessing_contract.py

# With coverage (HTML report in htmlcov/)
pdm run pytest tests/ --cov=. --cov-report=html
```

## Test structure

- `test_preprocessing_contract.py` — contract tests for `training/preprocessing.py` (run in CI before Docker build)
- `test_runner.py` — tests for the runner module
- `test_build_image.py` — tests for the build_image module
- `conftest.py` — shared fixtures

Coverage reports: `htmlcov/index.html` (HTML), `coverage.xml` (XML/CI).
