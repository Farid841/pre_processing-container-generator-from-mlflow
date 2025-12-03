#!/usr/bin/env python3
# Copyright 2025 AstroLab Software
# Author: Farid MAMAN and improved by IA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pytest configuration and fixtures."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_preprocessing_code():
    """Sample preprocessing code for testing."""
    return '''
def pre_processing(data):
    """Simple preprocessing function."""
    if isinstance(data, dict):
        result = {k: v for k, v in data.items() if v is not None}
        result["processed"] = True
        return result
    return data
'''


@pytest.fixture
def sample_preprocessing_class():
    """Sample preprocessing class for testing."""
    return '''
class Preprocessor:
    def pre_processing(self, data):
        """Preprocessing method."""
        if isinstance(data, dict):
            result = {k: v for k, v in data.items() if v is not None}
            result["processed"] = True
            return result
        return data
'''


@pytest.fixture
def sample_jsonl_data():
    """Sample JSONL data for testing."""
    return '{"key1": "value1", "key2": "value2"}\n{"key3": "value3", "key4": null}\n'


@pytest.fixture
def sample_json_data():
    """Sample JSON data for testing."""
    return '{"key1": "value1", "key2": "value2"}'


@pytest.fixture
def sample_dict_data():
    """Sample dictionary data for testing."""
    return {"key1": "value1", "key2": "value2", "key3": None}
