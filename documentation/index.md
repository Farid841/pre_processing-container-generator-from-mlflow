# MLflow Preprocessing Runner

Starting from an MLflow run, this service automatically builds **the two containers** a model needs to run in the Fink / ZTF alert pipeline:

| Container | Role | Content |
|---|---|---|
| **Preprocessing** (data processing) | Turns a raw alert into a feature vector | `preprocessing.py` from the MLflow run + REST API + Kafka consumer/producer |
| **AI model** | Computes the prediction from the features | `mlflow models serve` + Kafka bridge |

## Why two separate containers?

- **No recomputation on failure.** The features produced by the preprocessing are stored in an intermediate Kafka topic. If the model fails, the features are replayed from that topic: the preprocessing is never run again.
- **The model is the slowest and most resource-hungry step** (CPU / RAM). Kept separate, it can be sized and scaled independently of the preprocessing, which is light and fast.

## Overview

When a model version is promoted in MLflow, a webhook triggers the build and test of both images:

![Pipeline triggered by the MLflow webhook](diagrams/webhook-pipeline.drawio.svg)

Once deployed, the two containers talk to each other through Kafka:

![Kafka pipeline](diagrams/kafka-pipeline.drawio.svg)

## Where to start

- [Write a preprocessing](preprocessing.md): the `pre_processing(data)` contract
- [Build the images](build.md): `build.sh` on your machine
- [CI/CD and webhook](ci-cd.md): automatic builds from MLflow
- [Kafka pipeline](kafka.md): the steps and configuration of both containers
