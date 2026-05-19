"""
Promote a model version to @champion and trigger the CI build.

This script does two things atomically:
  1. Sets the @champion alias on the given model version in MLflow
  2. Fires a repository_dispatch event on GitHub to trigger the build pipeline

Usage:
    export MLFLOW_TRACKING_URI="https://mlflow-dev.fink-broker.org"
    export MLFLOW_TRACKING_USERNAME="your_username"
    export MLFLOW_TRACKING_PASSWORD="your_password"
    export GITHUB_TOKEN="ghp_..."        # needs 'repo' or 'workflow' scope
    export GITHUB_REPO="org/repo"        # e.g. astrolabsoftware/fink-ml-models

    python training/promote.py --model ztf-real-bogus --version 3

Rollback to a previous version:
    python training/promote.py --model ztf-real-bogus --version 2
"""

import argparse
import os
import sys

import mlflow
import requests
from mlflow.tracking import MlflowClient


def _setup_mlflow() -> MlflowClient:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
    username = os.environ.get("MLFLOW_TRACKING_USERNAME")
    password = os.environ.get("MLFLOW_TRACKING_PASSWORD")

    if username and password:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(tracking_uri)
        netloc = f"{username}:{password}@{parsed.netloc}"
        tracking_uri = urlunparse(parsed._replace(netloc=netloc))

    mlflow.set_tracking_uri(tracking_uri)
    return MlflowClient()


def promote(model_name: str, version: str, dry_run: bool = False) -> None:
    client = _setup_mlflow()

    # 1. Fetch the model version to get its run_id
    try:
        mv = client.get_model_version(model_name, version)
    except Exception as e:
        print(f"ERROR: could not fetch {model_name} v{version}: {e}")
        sys.exit(1)

    run_id = mv.run_id
    print(f"Model   : {model_name}")
    print(f"Version : {version}")
    print(f"Run ID  : {run_id}")

    if dry_run:
        print("\n[dry-run] Would set @champion alias and trigger CI — exiting.")
        return

    # 2. Set @champion alias
    try:
        client.set_registered_model_alias(model_name, "champion", version)
        print(f"\n✅ @champion → {model_name} v{version}")
    except Exception as e:
        print(f"ERROR: failed to set @champion alias: {e}")
        sys.exit(1)

    # 3. Trigger GitHub CI via repository_dispatch
    github_token = os.environ.get("GITHUB_TOKEN")
    github_repo  = os.environ.get("GITHUB_REPO")

    if not github_token:
        print("\nWARNING: GITHUB_TOKEN not set — @champion alias was set but CI was NOT triggered.")
        print("         Set GITHUB_TOKEN and re-run, or trigger the workflow manually from GitHub UI.")
        return

    if not github_repo:
        print("\nWARNING: GITHUB_REPO not set — @champion alias was set but CI was NOT triggered.")
        print("         Set GITHUB_REPO=org/repo and re-run.")
        return

    payload = {
        "event_type": "mlflow-model-version",
        "client_payload": {
            "data": {
                "run_id":  run_id,
                "source":  f"models:/{model_name}@champion",
            }
        },
    }

    try:
        resp = requests.post(
            f"https://api.github.com/repos/{github_repo}/dispatches",
            headers={
                "Authorization": f"Bearer {github_token}",
                "Accept":        "application/vnd.github.v3+json",
            },
            json=payload,
            timeout=10,
        )
    except requests.RequestException as e:
        print(f"ERROR: could not reach GitHub API: {e}")
        sys.exit(1)

    if resp.status_code == 204:
        print(f"✅ GitHub CI triggered on {github_repo}")
        print(f"   Workflow : build-mlflow-images.yml")
        print(f"   Payload  : run_id={run_id}, source=models:/{model_name}@champion")
    else:
        print(f"ERROR: GitHub dispatch failed ({resp.status_code}): {resp.text}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Promote a model version to @champion and trigger the CI build.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--model",   required=True, help="Registered model name (e.g. ztf-real-bogus)")
    parser.add_argument("--version", required=True, help="Model version to promote (e.g. 3)")
    parser.add_argument("--dry-run", action="store_true", help="Check without making any changes")
    args = parser.parse_args()

    promote(args.model, args.version, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
