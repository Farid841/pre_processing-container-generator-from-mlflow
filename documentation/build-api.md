# Build API

A REST API can start preprocessing builds without going through the command line.

```bash
uvicorn build_scripts.api:app --port 8000
```

| Method | Endpoint | Description |
|---|---|---|
| POST | `/build` | Starts a build in the background, returns a `build_id` |
| POST | `/build/sync` | Blocking build |
| GET | `/builds` | List of builds (filter with `?status=success`) |
| GET | `/builds/{id}` | Status of a build |
| DELETE | `/builds/{id}` | Removes a build from the history |

```bash
curl -X POST http://localhost:8000/build \
  -H "Content-Type: application/json" \
  -d '{"run_id": "<run_id>", "preprocessing_path": "code/preprocessing.py"}'
```

!!! note
    The build history is kept in memory: it is lost when the API restarts.
