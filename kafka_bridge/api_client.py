"""
REST API client for calling preprocessing/model endpoints.

Handles retries, timeouts, and error handling.
"""

import time
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from kafka_bridge.config import BridgeConfig
from kafka_bridge.logger import BridgeLogger


class APIClient:
    """HTTP client for calling REST APIs with retry support."""

    def __init__(self, config: BridgeConfig, logger: BridgeLogger):
        """Initialize the API client.

        Args:
            config: Bridge configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        self.session = self._create_session()
        self.base_url = config.api_url.rstrip("/")
        self.endpoint = config.api_endpoint

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.api_retry_count,
            backoff_factor=self.config.api_retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def health_check(self) -> bool:
        """Check if the API is healthy.

        Returns:
            True if API is responsive, False otherwise
        """
        try:
            url = f"{self.base_url}/health"
            response = self.session.get(url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            self.logger.warning(
                "Health check failed",
                error=str(e),
                url=f"{self.base_url}/health",
            )
            return False

    def wait_for_api(self, timeout: int = 60, interval: int = 5) -> bool:
        """Wait for the API to become available.

        Args:
            timeout: Maximum time to wait in seconds
            interval: Time between checks in seconds

        Returns:
            True if API became available, False if timeout
        """
        start_time = time.time()

        self.logger.info(
            "Waiting for API to become available",
            url=self.base_url,
            timeout=timeout,
        )

        while time.time() - start_time < timeout:
            if self.health_check():
                self.logger.info("API is available")
                return True
            time.sleep(interval)

        self.logger.error(
            "API did not become available within timeout",
            timeout=timeout,
        )
        return False

    def call_single(self, data: dict) -> Optional[dict]:
        """Call the API with a single record.

        Args:
            data: Single record data

        Returns:
            API response data, or None if failed
        """
        url = f"{self.base_url}{self.endpoint.replace('/batch', '')}"

        try:
            response = self.session.post(
                url,
                json={"data": data},
                timeout=self.config.api_timeout,
            )
            response.raise_for_status()

            self.logger.record_api_call(success=True)
            return response.json()

        except requests.exceptions.RequestException as e:
            self.logger.record_api_call(success=False)
            self.logger.error(
                "API call failed",
                error=str(e),
                url=url,
            )
            return None

    def call_batch(self, data: List[dict]) -> Optional[List[dict]]:
        """Call the API with a batch of records.

        Args:
            data: List of record data

        Returns:
            List of API response data, or None if failed
        """
        url = f"{self.base_url}{self.endpoint}"

        try:
            start_time = time.time()

            response = self.session.post(
                url,
                json={"data": data},
                timeout=self.config.api_timeout,
            )
            response.raise_for_status()

            elapsed = time.time() - start_time

            self.logger.record_api_call(success=True)
            self.logger.debug(
                "Batch API call succeeded",
                url=url,
                batch_size=len(data),
                elapsed_ms=int(elapsed * 1000),
            )

            result = response.json()

            # Handle different response formats
            if isinstance(result, dict):
                # Format: {"result": [...], "processed_count": N}
                return result.get("result", [result])
            elif isinstance(result, list):
                return result
            else:
                return [result]

        except requests.exceptions.Timeout:
            self.logger.record_api_call(success=False)
            self.logger.error(
                "API call timed out",
                url=url,
                timeout=self.config.api_timeout,
                batch_size=len(data),
            )
            return None

        except requests.exceptions.RequestException as e:
            self.logger.record_api_call(success=False)
            self.logger.error(
                "API batch call failed",
                error=str(e),
                url=url,
                batch_size=len(data),
            )
            return None

    def call_mlflow_invocations(self, data: List[dict]) -> Optional[List[dict]]:
        """Call MLflow model serving /invocations endpoint.

        MLflow expects a specific format for invocations.

        Args:
            data: List of input records

        Returns:
            List of predictions, or None if failed
        """
        url = f"{self.base_url}/invocations"

        try:
            start_time = time.time()

            # MLflow expects {"inputs": [...]} or {"instances": [...]}
            # or {"dataframe_split": {"columns": [...], "data": [...]}}
            payload = {"inputs": data}

            response = self.session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.config.api_timeout,
            )
            response.raise_for_status()

            elapsed = time.time() - start_time

            self.logger.record_api_call(success=True)
            self.logger.debug(
                "MLflow invocations call succeeded",
                url=url,
                batch_size=len(data),
                elapsed_ms=int(elapsed * 1000),
            )

            result = response.json()

            # MLflow returns {"predictions": [...]} or just [...]
            if isinstance(result, dict) and "predictions" in result:
                return result["predictions"]
            elif isinstance(result, list):
                return result
            else:
                return [result]

        except requests.exceptions.RequestException as e:
            self.logger.record_api_call(success=False)
            self.logger.error(
                "MLflow invocations call failed",
                error=str(e),
                url=url,
                batch_size=len(data),
            )
            return None

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()
