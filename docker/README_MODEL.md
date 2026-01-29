# Dockerfile for Model Serving

## Note on MLflow Model Serving

MLflow's `mlflow models build-docker` command automatically generates a Dockerfile optimized for model serving. This Dockerfile includes:

- Base Python image
- MLflow installation (if `--install-mlflow` is used)
- Model dependencies
- MLflow model server configuration
- Health check endpoints

## Custom Dockerfile (Optional)

If you need to customize the Dockerfile for model serving, you can:

1. **Use MLflow's generated Dockerfile as base**: Build the image first with `mlflow models build-docker`, then modify it
2. **Create a custom Dockerfile**: Create `docker/Dockerfile.model` and use it with custom build process

## Example Custom Dockerfile

If you need a custom Dockerfile, here's a template:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Install MLflow and model dependencies
RUN pip install --no-cache-dir mlflow

# Copy model (will be downloaded from MLflow during build)
# The model should be downloaded and copied here
COPY model/ /app/model/

# Expose MLflow model server port
EXPOSE 8080

# Run MLflow model server
CMD ["mlflow", "models", "serve", "-m", "/app/model", "--host", "0.0.0.0", "--port", "8080"]
```

However, **using `mlflow models build-docker` is recommended** as it handles all the complexity automatically.
