"""
Shared utilities for build scripts.

Covers:
  - Name sanitization (Docker image names, Kafka topic names)
  - Subprocess streaming (stream_run)
"""

import re
import subprocess


def stream_run(cmd: list[str]) -> None:
    """Run a subprocess, stream its stdout/stderr to the terminal, raise on failure.

    The :class:`subprocess.CalledProcessError` raised on non-zero exit carries
    the full captured output in its ``output`` attribute, which callers can
    inspect for error diagnostics.

    Args:
        cmd: Command and arguments to execute.

    Raises:
        subprocess.CalledProcessError: If the process exits with a non-zero code.
    """
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    lines: list[str] = []
    for line in process.stdout:
        print(line, end="", flush=True)
        lines.append(line)
    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd, output="".join(lines))


def sanitize_docker_name(name: str) -> str:
    """Sanitize a string for use as a Docker image name component.

    Converts to lowercase and replaces any character that is not
    alphanumeric or a hyphen with a hyphen.  Leading/trailing hyphens
    are stripped.

    Args:
        name: Raw string (model name, version, type…)

    Returns:
        Sanitized string safe for Docker image names.

    Examples:
        >>> sanitize_docker_name("My_Model.v1")
        'my-model-v1'
        >>> sanitize_docker_name("--foo--")
        'foo'
    """
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


def sanitize_kafka_topic(name: str) -> str:
    """Sanitize a string for use as a Kafka topic name component.

    Kafka topics allow alphanumeric characters, hyphens, underscores,
    and dots.  Everything else is replaced with a hyphen.

    Args:
        name: Raw string (model name, version, suffix…)

    Returns:
        Sanitized string safe for Kafka topic names.

    Examples:
        >>> sanitize_kafka_topic("My Model@v1.0")
        'my-model-v1.0'
    """
    return re.sub(r"[^a-zA-Z0-9-_.]", "-", name.lower())
