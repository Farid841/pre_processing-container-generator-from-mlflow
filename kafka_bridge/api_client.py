"""
REST API client for calling preprocessing/model endpoints.

Handles retries, timeouts, and error handling.
"""

import math
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
            health_endpoint = self.config.api_health_endpoint
            url = f"{self.base_url}{health_endpoint}"
            response = self.session.get(url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            self.logger.warning(
                "Health check failed",
                error=str(e),
                url=f"{self.base_url}{self.config.api_health_endpoint}",
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

    def _normalize_record(self, record, index: int) -> Optional[list]:
        """Normalize a single record into a feature vector list.

        Handles scalars, lists, and dicts from various preprocessing outputs.

        Args:
            record: A single input record in any supported format.
            index:  Position in the batch (for log context).

        Returns:
            A list of features, or None if the record should be skipped.
        """
        if record is None:
            self.logger.warning("Skipping None record", index=index)
            return None
        if isinstance(record, (int, float)):
            return [record]
        if isinstance(record, list):
            filtered = [x for x in record if x is not None]
            if not filtered:
                self.logger.warning("Empty list after filtering None, skipping", index=index)
                return None
            return filtered
        if isinstance(record, dict):
            return self._normalize_dict_record(record, index)
        self.logger.warning("Invalid record format, skipping", record_type=type(record).__name__)
        return None

    def _normalize_dict_record(self, record: dict, index: int) -> Optional[list]:
        """Extract a feature vector from a dict record.

        Supports three dict shapes:
        - ``{"model_input": [...]}`` — legacy format
        - ``{"result": [...]}`` — preprocessing output format
        - arbitrary dict  — values sorted by key are used as features

        Args:
            record: Dict record to normalize.
            index:  Position in the batch (for log context).

        Returns:
            A list of features, or None if extraction fails.
        """
        if "model_input" in record:
            return record["model_input"]
        if "features" in record:
            features = record["features"]
            if isinstance(features, list):
                return features
            self.logger.warning(
                "Invalid features format in record, skipping",
                features_type=type(features).__name__,
            )
            return None
        if "result" in record:
            result = record["result"]
            if isinstance(result, list):
                return result
            self.logger.warning(
                "Invalid result format in record, skipping",
                result_type=type(result).__name__,
            )
            return None
        # Fall back: use scalar dict values sorted by key as feature vector
        try:
            feature_list = [
                v
                for k, v in sorted(record.items())
                if not isinstance(v, (dict, list)) or k == "features"
            ]
            if feature_list:
                return feature_list
            self.logger.warning(
                "Could not extract features from dict, skipping",
                keys=list(record.keys())[:5],
            )
        except Exception as e:
            self.logger.warning(
                "Error extracting features from dict, skipping",
                error=str(e),
            )
        return None

    def _clean_feature_vector(self, inp: list, index: int) -> Optional[list]:
        """Replace None/NaN/Inf values in a feature vector with 0.0.

        Args:
            inp:   Raw feature vector (may contain None, NaN, or Inf).
            index: Position in the batch (for log context).

        Returns:
            Cleaned list, or None if the result would be empty.
        """
        if not isinstance(inp, list):
            inp = [inp]
        cleaned = []
        for val in inp:
            if val is None:
                cleaned.append(0.0)
            elif isinstance(val, (int, float)) and (math.isnan(val) or math.isinf(val)):
                self.logger.debug("Replacing invalid value (NaN or Inf) with 0.0", index=index)
                cleaned.append(0.0)
            else:
                cleaned.append(val)
        if not cleaned:
            self.logger.warning("Empty input after cleaning, skipping", index=index)
            return None
        return cleaned

    def call_mlflow_invocations(self, data: List) -> Optional[List]:
        """Call MLflow model serving ``/invocations`` endpoint.

        Normalizes each record to a feature vector, cleans invalid numeric
        values, then POSTs ``{"inputs": [[…], …]}`` to MLflow.

        Args:
            data: List of records in any format supported by :meth:`_normalize_record`.

        Returns:
            List of predictions, or None if the call failed.
        """
        url = f"{self.base_url}/invocations"

        self.logger.debug(
            "Processing records for MLflow invocations",
            total_records=len(data),
            first_record_type=type(data[0]).__name__ if data else "empty",
        )

        # Step 1: normalize each record to a raw feature vector
        raw_inputs = []
        for i, record in enumerate(data):
            vec = self._normalize_record(record, i)
            if vec is not None:
                raw_inputs.append(vec)

        if not raw_inputs:
            self.logger.error("No valid inputs to send to model")
            return None

        # Step 2: clean each vector (replace None/NaN/Inf with 0.0)
        validated_inputs = []
        for i, inp in enumerate(raw_inputs):
            try:
                cleaned = self._clean_feature_vector(inp, i)
                if cleaned is not None:
                    validated_inputs.append(cleaned)
            except Exception as e:
                self.logger.warning("Error validating input, skipping", index=i, error=str(e))

        if not validated_inputs:
            self.logger.error("No valid inputs after validation")
            return None

        payload = {"inputs": validated_inputs}
        self.logger.debug(
            "Sending to MLflow invocations",
            url=url,
            batch_size=len(validated_inputs),
            sample_input_length=len(validated_inputs[0]) if validated_inputs else 0,
            sample_input_preview=(
                str(validated_inputs[0][:5])
                if validated_inputs and validated_inputs[0]
                else "empty"
            ),
        )

        # Step 3: POST to MLflow and parse the response
        try:
            start_time = time.time()
            response = self.session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.config.api_timeout,
            )

            if not response.ok:
                try:
                    self.logger.error(
                        "MLflow invocations call failed",
                        status_code=response.status_code,
                        error_detail=response.text[:500],
                        payload_preview=str(payload)[:200],
                    )
                except Exception:
                    pass
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
            if isinstance(result, dict) and "predictions" in result:
                return result["predictions"]
            elif isinstance(result, list):
                return result
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
