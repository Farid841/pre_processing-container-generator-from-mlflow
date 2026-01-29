#!/usr/bin/env python3
"""
REST API server for preprocessing.

Exposes the preprocessing function via HTTP API instead of stdin/stdout.

Usage:
    The container will start this API server automatically.
    Access at: http://localhost:8000/docs
"""

import logging
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, List, Optional, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Import preprocessing loader
# In container: /app is in PYTHONPATH, so 'runner.runner' works
# As script: need to add parent to path
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))

from runner.runner import load_preprocessing

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class AppState:
    """Thread-safe application state container."""

    pre_processing_func: Optional[Callable[[Any], Any]] = field(default=None)

    def is_ready(self) -> bool:
        """Check if preprocessing function is loaded."""
        return self.pre_processing_func is not None


# Application state (thread-safe dataclass instead of global variable)
app_state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle.

    Loads preprocessing function on startup, cleans up on shutdown.
    This replaces the deprecated @app.on_event("startup") decorator.
    """
    # Startup
    try:
        logger.info("Loading preprocessing function...")
        app_state.pre_processing_func = load_preprocessing()
        logger.info("✅ Preprocessing loaded successfully")
    except Exception as e:
        logger.error(f"❌ Failed to load preprocessing: {e}", exc_info=True)
        raise

    yield  # Application runs here

    # Shutdown (cleanup if needed)
    logger.info("Shutting down preprocessing API...")
    app_state.pre_processing_func = None


app = FastAPI(
    title="Preprocessing API",
    description="REST API for preprocessing data",
    version="1.0.0",
    lifespan=lifespan,
)


class PreprocessRequest(BaseModel):
    """Request body for preprocessing a single record."""

    data: dict = Field(..., description="Input data to preprocess")


class PreprocessBatchRequest(BaseModel):
    """Request body for preprocessing multiple records."""

    data: List[dict] = Field(..., description="List of input data to preprocess")


class PreprocessResponse(BaseModel):
    """Response from preprocessing."""

    result: Union[dict, List[dict], Any] = Field(..., description="Preprocessed data")
    processed_count: int = Field(..., description="Number of records processed")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    preprocessing_loaded: bool
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    component_type: str = "preprocessing"


@app.get("/", tags=["Info"])
async def root() -> dict:
    """Show available API endpoints."""
    return {
        "name": "Preprocessing API",
        "version": "1.0.0",
        "status": "ready" if app_state.is_ready() else "not_loaded",
        "endpoints": {
            "POST /preprocess": "Preprocess a single record",
            "POST /preprocess/batch": "Preprocess multiple records",
            "GET /health": "Health check",
            "GET /docs": "API documentation (Swagger UI)",
        },
    }


@app.get("/health", response_model=HealthResponse, tags=["Info"])
async def health() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(
        status="healthy" if app_state.is_ready() else "error",
        preprocessing_loaded=app_state.is_ready(),
        model_name=os.getenv("MODEL_NAME"),
        model_version=os.getenv("MODEL_VERSION"),
        component_type=os.getenv("COMPONENT_TYPE", "preprocessing"),
    )


@app.post("/preprocess", response_model=PreprocessResponse, tags=["Preprocessing"])
async def preprocess(request: PreprocessRequest) -> PreprocessResponse:
    """
    Preprocess a single data record.

    Example:
    ```json
    {
      "data": {"field1": "value1", "field2": 123}
    }
    ```
    """
    if not app_state.is_ready():
        raise HTTPException(status_code=503, detail="Preprocessing not loaded")

    try:
        result = app_state.pre_processing_func(request.data)
        return PreprocessResponse(result=result, processed_count=1)
    except Exception as e:
        logger.error(f"Preprocessing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Preprocessing error: {str(e)}")


@app.post("/preprocess/batch", response_model=PreprocessResponse, tags=["Preprocessing"])
async def preprocess_batch(request: PreprocessBatchRequest) -> PreprocessResponse:
    """
    Preprocess multiple data records in batch.

    Example:
    ```json
    {
      "data": [
        {"field1": "value1", "field2": 123},
        {"field1": "value2", "field2": 456}
      ]
    }
    ```
    """
    if not app_state.is_ready():
        raise HTTPException(status_code=503, detail="Preprocessing not loaded")

    if not request.data:
        raise HTTPException(status_code=400, detail="Empty data list")

    results = []
    errors = []

    for idx, record in enumerate(request.data):
        try:
            result = app_state.pre_processing_func(record)
            results.append(result)
        except Exception as e:
            logger.error(f"Preprocessing failed for record {idx}: {e}", exc_info=True)
            errors.append({"index": idx, "error": str(e)})

    if errors:
        # Return partial results with errors
        return PreprocessResponse(
            result={"results": results, "errors": errors},
            processed_count=len(results),
        )

    return PreprocessResponse(result=results, processed_count=len(results))


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("API_PORT", "8000"))
    host = os.getenv("API_HOST", "0.0.0.0")

    logger.info(f"Starting preprocessing API on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
