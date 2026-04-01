#!/usr/bin/env python3
# Copyright 2025 AstroLab Software
# Author: Farid MAMAN
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Unit tests for build_model_image module."""

import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# build_scripts is at the repo root, not installed as a package
sys.path.insert(0, str(Path(__file__).parent.parent))

from build_scripts import build_model_image  # noqa: E402
from build_scripts.utils import sanitize_docker_name  # noqa: E402


class TestSanitize:
    """Tests for sanitize_docker_name helper."""

    def test_lowercase(self):
        """Uppercase letters are lowercased."""
        assert sanitize_docker_name("MyModel") == "mymodel"

    def test_replaces_special_chars(self):
        """Underscores and dots are replaced with hyphens."""
        assert sanitize_docker_name("my_model.v1") == "my-model-v1"

    def test_strips_leading_trailing_hyphens(self):
        """Leading and trailing hyphens are stripped."""
        assert sanitize_docker_name("_model_") == "model"

    def test_leaves_valid_hyphens_intact(self):
        """Existing hyphens are preserved."""
        assert sanitize_docker_name("my-model") == "my-model"


class TestBuildMlflowBaseImage:
    """Tests for build_mlflow_base_image."""

    @patch("build_scripts.build_model_image._stream_run")
    def test_calls_mlflow_build_docker(self, mock_stream):
        """Correct mlflow CLI command is assembled."""
        result = build_model_image.build_mlflow_base_image(
            model_uri="models:/my-model/3",
            base_image_name="model-my-model-3-base",
        )

        mock_stream.assert_called_once_with(
            [
                "mlflow",
                "models",
                "build-docker",
                "-m",
                "models:/my-model/3",
                "-n",
                "model-my-model-3-base",
                "--install-mlflow",
            ]
        )
        assert result == "model-my-model-3-base:latest"

    @patch("build_scripts.build_model_image._stream_run")
    def test_raises_on_failure(self, mock_stream):
        """Propagate CalledProcessError when mlflow fails."""
        mock_stream.side_effect = subprocess.CalledProcessError(1, ["mlflow"])

        with pytest.raises(subprocess.CalledProcessError):
            build_model_image.build_mlflow_base_image(
                model_uri="models:/bad/1",
                base_image_name="bad-base",
            )


class TestBuildWrapperImage:
    """Tests for build_wrapper_image."""

    @patch("build_scripts.build_model_image._stream_run")
    def test_calls_docker_build_with_build_arg(self, mock_stream, tmp_path):
        """Call docker build with --build-arg BASE_IMAGE."""
        dockerfile = tmp_path / "Dockerfile.model"
        dockerfile.write_text("ARG BASE_IMAGE\nFROM ${BASE_IMAGE}\n")

        result = build_model_image.build_wrapper_image(
            base_image="model-foo-3-base:latest",
            final_image_name="model-foo",
            image_tags=["latest"],
            dockerfile_path=str(dockerfile),
            build_context=".",
        )

        mock_stream.assert_called_once_with(
            [
                "docker",
                "build",
                "-t",
                "model-foo:latest",
                "-f",
                str(dockerfile),
                "--build-arg",
                "BASE_IMAGE=model-foo-3-base:latest",
                ".",
            ]
        )
        assert result == ["model-foo:latest"]

    @patch("build_scripts.build_model_image._stream_run")
    def test_respects_custom_tag(self, mock_stream, tmp_path):
        """Custom image tag is passed through to docker build."""
        dockerfile = tmp_path / "Dockerfile.model"
        dockerfile.write_text("FROM scratch")

        build_model_image.build_wrapper_image(
            base_image="base:latest",
            final_image_name="model-foo",
            image_tags=["v3.0"],
            dockerfile_path=str(dockerfile),
        )

        args = mock_stream.call_args[0][0]
        assert "model-foo:v3.0" in args

    def test_raises_when_dockerfile_missing(self):
        """Raise an error when Dockerfile.model does not exist."""
        with pytest.raises(FileNotFoundError, match="Dockerfile not found"):
            build_model_image.build_wrapper_image(
                base_image="base:latest",
                final_image_name="model-foo",
                dockerfile_path="/nonexistent/Dockerfile.model",
            )

    @patch("build_scripts.build_model_image._stream_run")
    def test_raises_on_docker_failure(self, mock_stream, tmp_path):
        """Propagate CalledProcessError when docker build fails."""
        dockerfile = tmp_path / "Dockerfile.model"
        dockerfile.write_text("FROM scratch")
        mock_stream.side_effect = subprocess.CalledProcessError(1, ["docker"])

        with pytest.raises(subprocess.CalledProcessError):
            build_model_image.build_wrapper_image(
                base_image="base:latest",
                final_image_name="model-foo",
                dockerfile_path=str(dockerfile),
            )


class TestStreamRun:
    """Tests for the stream_run helper (imported as _stream_run)."""

    @patch("subprocess.Popen")
    def test_streams_output(self, mock_popen):
        """Popen is called and output lines are iterated."""
        mock_proc = mock_popen.return_value
        mock_proc.stdout.__iter__ = lambda self: iter(["line1\n", "line2\n"])
        mock_proc.wait.return_value = None
        mock_proc.returncode = 0

        build_model_image._stream_run(["echo", "hello"])

        mock_popen.assert_called_once()

    @patch("subprocess.Popen")
    def test_raises_on_non_zero_exit(self, mock_popen):
        """Non-zero returncode raises CalledProcessError."""
        mock_proc = mock_popen.return_value
        mock_proc.stdout.__iter__ = lambda self: iter([])
        mock_proc.wait.return_value = None
        mock_proc.returncode = 1

        with pytest.raises(subprocess.CalledProcessError):
            build_model_image._stream_run(["false"])


class TestMain:
    """Integration-style tests for the main() entry point."""

    @patch("build_scripts.build_model_image.build_wrapper_image")
    @patch("build_scripts.build_model_image.build_mlflow_base_image")
    def test_full_build_two_steps(self, mock_base, mock_wrapper):
        """main() runs base build then wrapper build with correct args."""
        mock_base.return_value = "model-mymodel-3-base:latest"
        mock_wrapper.return_value = ["model-mymodel:latest", "model-mymodel:v3"]

        with patch(
            "sys.argv",
            [
                "build_model_image.py",
                "models:/my-model/3",
                "mymodel",
                "3",
            ],
        ):
            build_model_image.main()

        mock_base.assert_called_once_with(
            "models:/my-model/3",
            "model-mymodel-3-base",
        )
        mock_wrapper.assert_called_once_with(
            base_image="model-mymodel-3-base:latest",
            final_image_name="model-mymodel",
            image_tags=["latest", "v3"],
            dockerfile_path="docker/Dockerfile.model",
        )

    @patch("build_scripts.build_model_image.build_wrapper_image")
    @patch("build_scripts.build_model_image.build_mlflow_base_image")
    def test_skip_base_build(self, mock_base, mock_wrapper):
        """--skip-base-build skips mlflow build-docker entirely."""
        mock_wrapper.return_value = ["model-mymodel:latest", "model-mymodel:v3"]

        with patch(
            "sys.argv",
            [
                "build_model_image.py",
                "models:/my-model/3",
                "mymodel",
                "3",
                "--skip-base-build",
            ],
        ):
            build_model_image.main()

        mock_base.assert_not_called()
        mock_wrapper.assert_called_once()

    @patch("build_scripts.build_model_image.build_mlflow_base_image")
    def test_exits_on_build_failure(self, mock_base):
        """Exit with code 1 when the build step fails."""
        mock_base.side_effect = subprocess.CalledProcessError(1, ["mlflow"])

        with patch(
            "sys.argv",
            [
                "build_model_image.py",
                "models:/my-model/3",
                "mymodel",
                "3",
            ],
        ):
            with pytest.raises(SystemExit) as exc_info:
                build_model_image.main()
            assert exc_info.value.code == 1
