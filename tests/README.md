# Tests

This directory contains unit tests for the  project.

**Note:** If you only want to run tests without coverage, you can install just pytest:

```bash
pip install pytest pytest-mock
```

For coverage reports, install pytest-cov:

```bash
pip install pytest-cov
```

## Running Tests

Run all tests:

```bash
pytest
```

Run with coverage:

```bash
pytest --cov=runner --cov=build_scripts --cov-report=html
```

Run specific test file:

```bash
pytest tests/test_runner.py
```

Run specific test:

```bash
pytest tests/test_runner.py::TestLoadPreprocessing::test_load_preprocessing_function
```

Run tests in verbose mode:

```bash
pytest -v
```

## Test Structure

- `conftest.py`: Pytest fixtures and configuration
- `test_runner.py`: Tests for the runner module
- `test_build_image.py`: Tests for the build_image module

## Coverage

Coverage reports are generated in:
- Terminal: `--cov-report=term-missing`
- HTML: `htmlcov/index.html`
- XML: `coverage.xml`
