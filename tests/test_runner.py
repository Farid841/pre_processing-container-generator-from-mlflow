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

"""Tests for runner module."""

import sys
from builtins import __import__ as original_import
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

# Import runner functions
sys.path.insert(0, str(Path(__file__).parent.parent))
from runner import runner  # noqa: E402


class TestLoadPreprocessing:
    """Tests for load_preprocessing function."""

    def test_load_preprocessing_function(self, temp_dir, sample_preprocessing_code):
        """Test loading a preprocessing function."""
        preprocessing_file = temp_dir / "preprocessing.py"
        preprocessing_file.write_text(sample_preprocessing_code)

        with patch("runner.runner.PREPROCESSING_PATH", preprocessing_file):
            func = runner.load_preprocessing()
            assert callable(func)
            assert func.__name__ == "pre_processing"

            # Test the function
            result = func({"key1": "value1", "key2": None})
            assert result["processed"] is True
            assert "key1" in result
            assert "key2" not in result

    def test_load_preprocessing_class(self, temp_dir, sample_preprocessing_class):
        """Test loading a preprocessing class."""
        preprocessing_file = temp_dir / "preprocessing.py"
        preprocessing_file.write_text(sample_preprocessing_class)

        with patch("runner.runner.PREPROCESSING_PATH", preprocessing_file):
            func = runner.load_preprocessing()
            assert callable(func)

            # Test the method
            result = func({"key1": "value1", "key2": None})
            assert result["processed"] is True

    def test_load_preprocessing_not_found(self):
        """Test error when preprocessing file doesn't exist."""
        fake_path = Path("/nonexistent/preprocessing.py")
        with patch("runner.runner.PREPROCESSING_PATH", fake_path):
            with pytest.raises(FileNotFoundError):
                runner.load_preprocessing()

    def test_load_preprocessing_no_function(self, temp_dir):
        """Test error when preprocessing function doesn't exist."""
        preprocessing_file = temp_dir / "preprocessing.py"
        preprocessing_file.write_text("def other_function(): pass")

        with patch("runner.runner.PREPROCESSING_PATH", preprocessing_file):
            with pytest.raises(ValueError, match="No pre_processing"):
                runner.load_preprocessing()


class TestIsAvroFile:
    """Tests for is_avro_file function."""

    def test_is_avro_file_by_extension(self, temp_dir):
        """Test detection by file extension."""
        avro_file = temp_dir / "test.avro"
        avro_file.write_bytes(b"dummy content")
        assert runner.is_avro_file(str(avro_file)) is True

    def test_is_avro_file_by_magic_bytes(self, temp_dir):
        """Test detection by magic bytes."""
        avro_file = temp_dir / "test.bin"
        avro_file.write_bytes(b"Obj\x01dummy")
        assert runner.is_avro_file(str(avro_file)) is True

    def test_is_not_avro_file(self, temp_dir):
        """Test that non-Avro files are not detected."""
        json_file = temp_dir / "test.json"
        json_file.write_bytes(b'{"key": "value"}')
        assert runner.is_avro_file(str(json_file)) is False

    def test_is_avro_file_stdin(self):
        """Test stdin detection (should return False by default)."""
        assert runner.is_avro_file(None) is False


class TestReadAvroFile:
    """Tests for read_avro_file function."""

    def test_read_avro_file_not_installed(self, temp_dir):
        """Test error when fastavro is not installed."""
        # Create a fake avro file
        avro_file = temp_dir / "test.avro"
        avro_file.write_bytes(b"dummy")

        # Mock the import to raise ImportError when importing fastavro
        def mock_import(name, *args, **kwargs):
            if name == "fastavro":
                raise ImportError("No module named fastavro")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="fastavro not installed"):
                list(runner.read_avro_file(str(avro_file)))


class TestReadJson:
    """Tests for JSON reading functions."""

    def test_read_json_from_file_jsonl(self, temp_dir):
        """Test reading JSONL from file."""
        jsonl_file = temp_dir / "test.jsonl"
        jsonl_file.write_text('{"key1": "value1"}\n{"key2": "value2"}\n')

        results = list(runner._read_json_from_file(str(jsonl_file)))
        assert len(results) == 2
        assert results[0] == {"key1": "value1"}
        assert results[1] == {"key2": "value2"}

    def test_read_json_from_file_complete_json(self, temp_dir):
        """Test reading complete JSON from file."""
        json_file = temp_dir / "test.json"
        json_file.write_text('{"key": "value"}')

        results = list(runner._read_json_from_file(str(json_file)))
        assert len(results) == 1
        assert results[0] == {"key": "value"}

    def test_read_json_from_file_json_array(self, temp_dir):
        """Test reading JSON array from file."""
        json_file = temp_dir / "test.json"
        json_file.write_text('[{"key1": "value1"}, {"key2": "value2"}]')

        results = list(runner._read_json_from_file(str(json_file)))
        assert len(results) == 2
        assert results[0] == {"key1": "value1"}
        assert results[1] == {"key2": "value2"}

    @patch("sys.stdin")
    def test_read_json_from_stdin_jsonl(self, mock_stdin):
        """Test reading JSONL from stdin."""
        mock_stdin.read.return_value = '{"key1": "value1"}\n{"key2": "value2"}\n'

        results = list(runner._read_json_from_stdin())
        assert len(results) == 2
        assert results[0] == {"key1": "value1"}
        assert results[1] == {"key2": "value2"}

    @patch("sys.stdin")
    def test_read_json_from_stdin_complete_json(self, mock_stdin):
        """Test reading complete JSON from stdin."""
        mock_stdin.read.return_value = '{"key": "value"}'

        results = list(runner._read_json_from_stdin())
        assert len(results) == 1
        assert results[0] == {"key": "value"}


class TestMain:
    """Tests for main function."""

    @patch("runner.runner.load_preprocessing")
    @patch("runner.runner.is_avro_file")
    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}\n')
    @patch("sys.stdout")
    def test_main_with_jsonl_file(self, mock_stdout, mock_file, mock_is_avro, mock_load):
        """Test main function with JSONL file."""
        mock_is_avro.return_value = False
        mock_func = MagicMock(return_value={"processed": True})
        mock_load.return_value = mock_func

        with patch("sys.argv", ["runner.py", "test.jsonl"]):
            runner.main()

        mock_load.assert_called_once()
        mock_func.assert_called_once()

    @patch("runner.runner.load_preprocessing")
    @patch("sys.stdin")
    @patch("sys.stdout")
    def test_main_with_stdin(self, mock_stdout, mock_stdin, mock_load):
        """Test main function with stdin input."""
        mock_stdin.read.return_value = '{"key": "value"}\n'
        mock_func = MagicMock(return_value={"processed": True})
        mock_load.return_value = mock_func

        with patch("sys.argv", ["runner.py"]):
            with patch("runner.runner.is_avro_file", return_value=False):
                runner.main()

        mock_load.assert_called_once()
