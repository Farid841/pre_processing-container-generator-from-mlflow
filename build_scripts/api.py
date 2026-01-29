#!/usr/bin/env python3
"""
REST API for building Docker images from MLflow preprocessing.

Run with:
    uvicorn build_scripts.api:app --reload --port 8000

Or:
    python -m build_scripts.api
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

from build_scripts.build_image import (
    build_docker_image,
    build_image_name,
    get_model_info_from_mlflow,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MLflow Preprocessing Builder API",
    description="API to build Docker images from MLflow preprocessing artifacts",
    version="1.0.0",
)

# In-memory storage for build jobs (use Redis/DB for production)
builds: dict = {}


class BuildStatus(str, Enum):
    """Build job status enumeration."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class BuildRequest(BaseModel):
    """Request to build a Docker image."""

    run_id: str = Field(
        ..., description="MLflow run ID", example="e6c1131f4673449aa688ed1ffc3abbbe"
    )
    image_name: Optional[str] = Field(
        None, description="Docker image name (auto-generated if not provided)"
    )
    tag: str = Field("latest", description="Docker image tag")
    preprocessing_path: Optional[str] = Field(
        None,
        description="Artifact path in MLflow (e.g., 'code/preprocessing.py')",
        example="code/preprocessing.py",
    )
    python_version: Optional[str] = Field(None, description="Python version (e.g., '3.11', '3.12')")

    class Config:
        """Pydantic model configuration."""

        json_schema_extra = {
            "example": {
                "run_id": "e6c1131f4673449aa688ed1ffc3abbbe",
                "preprocessing_path": "code/preprocessing.py",
                "tag": "latest",
            }
        }


class BuildResponse(BaseModel):
    """Response after starting a build."""

    build_id: str
    status: BuildStatus
    message: str
    image_name: Optional[str] = None


class BuildInfo(BaseModel):
    """Detailed build information."""

    build_id: str
    status: BuildStatus
    run_id: str
    image_name: Optional[str] = None
    full_image_name: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    logs: list[str] = []


def run_build(build_id: str, request: BuildRequest):
    """Execute the build in background."""
    builds[build_id]["status"] = BuildStatus.RUNNING
    builds[build_id]["logs"].append(f"Starting build for run_id: {request.run_id}")

    try:
        # Get model info from MLflow
        model_name, version, type_name = get_model_info_from_mlflow(request.run_id)
        builds[build_id]["logs"].append(
            f"Retrieved from MLflow: {model_name}, version={version}, type={type_name}"
        )

        # Build image name if not provided
        if request.image_name:
            image_name = request.image_name
        else:
            image_name = build_image_name(model_name, version, type_name)
        builds[build_id]["image_name"] = image_name
        builds[build_id]["logs"].append(f"Image name: {image_name}")

        # Build the Docker image
        full_image_name = build_docker_image(
            run_id=request.run_id,
            image_name=image_name,
            image_tag=request.tag,
            preprocessing_path=request.preprocessing_path,
            python_version=request.python_version,
            model_name=model_name,
            model_version=version,
            component_type=type_name,
        )

        builds[build_id]["full_image_name"] = full_image_name
        builds[build_id]["status"] = BuildStatus.SUCCESS
        builds[build_id]["finished_at"] = datetime.now()
        builds[build_id]["logs"].append(f"✅ Build successful: {full_image_name}")

    except Exception as e:
        builds[build_id]["status"] = BuildStatus.FAILED
        builds[build_id]["error"] = str(e)
        builds[build_id]["finished_at"] = datetime.now()
        builds[build_id]["logs"].append(f"❌ Build failed: {e}")
        logger.error(f"Build {build_id} failed: {e}")


@app.get("/", tags=["Info"])
async def root():
    """Show available API endpoints."""
    return {
        "name": "MLflow Preprocessing Builder API",
        "version": "1.0.0",
        "endpoints": {
            "POST /build": "Start a new build",
            "GET /builds": "List all builds",
            "GET /builds/{build_id}": "Get build status",
            "GET /health": "Health check",
        },
    }


@app.get("/health", tags=["Info"])
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "mlflow_uri": os.getenv("MLFLOW_TRACKING_URI", "not set"),
    }


@app.post("/build", response_model=BuildResponse, tags=["Build"])
async def start_build(request: BuildRequest, background_tasks: BackgroundTasks):
    """
    Start a new Docker image build.

    The build runs in the background. Use GET /builds/{build_id} to check status.
    """
    build_id = str(uuid.uuid4())[:8]

    # Initialize build record
    builds[build_id] = {
        "build_id": build_id,
        "status": BuildStatus.PENDING,
        "run_id": request.run_id,
        "image_name": None,
        "full_image_name": None,
        "started_at": datetime.now(),
        "finished_at": None,
        "error": None,
        "logs": [],
    }

    # Run build in background
    background_tasks.add_task(run_build, build_id, request)

    return BuildResponse(
        build_id=build_id,
        status=BuildStatus.PENDING,
        message=f"Build started. Check status at GET /builds/{build_id}",
    )


@app.get("/builds", response_model=list[BuildInfo], tags=["Build"])
async def list_builds(limit: int = 20, status: Optional[BuildStatus] = None):
    """List all builds, optionally filtered by status."""
    all_builds = list(builds.values())

    # Filter by status if provided
    if status:
        all_builds = [b for b in all_builds if b["status"] == status]

    # Sort by started_at (newest first) and limit
    all_builds.sort(key=lambda x: x["started_at"], reverse=True)
    return [BuildInfo(**b) for b in all_builds[:limit]]


@app.get("/builds/{build_id}", response_model=BuildInfo, tags=["Build"])
async def get_build(build_id: str):
    """Get detailed information about a specific build."""
    if build_id not in builds:
        raise HTTPException(status_code=404, detail=f"Build {build_id} not found")
    return BuildInfo(**builds[build_id])


@app.delete("/builds/{build_id}", tags=["Build"])
async def delete_build(build_id: str):
    """Delete a build record (does not delete the Docker image)."""
    if build_id not in builds:
        raise HTTPException(status_code=404, detail=f"Build {build_id} not found")
    del builds[build_id]
    return {"message": f"Build {build_id} deleted"}


# Synchronous endpoint for simple use cases
@app.post("/build/sync", tags=["Build"])
async def build_sync(request: BuildRequest):
    """
    Build and wait for completion (synchronous).

    ⚠️ Warning: This endpoint blocks until build is complete.
    For long builds, use POST /build instead.
    """
    build_id = str(uuid.uuid4())[:8]

    builds[build_id] = {
        "build_id": build_id,
        "status": BuildStatus.RUNNING,
        "run_id": request.run_id,
        "image_name": None,
        "full_image_name": None,
        "started_at": datetime.now(),
        "finished_at": None,
        "error": None,
        "logs": [],
    }

    # Run build synchronously
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_build, build_id, request)

    return BuildInfo(**builds[build_id])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
