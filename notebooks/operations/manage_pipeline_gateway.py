# Databricks notebook source
import json
import time
import urllib.error
import urllib.request


dbutils.widgets.text("action", "start")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("timeout_seconds", "900")
dbutils.widgets.text("poll_seconds", "15")


action = dbutils.widgets.get("action").strip().lower()
pipeline_id = dbutils.widgets.get("pipeline_id").strip()
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))
poll_seconds = int(dbutils.widgets.get("poll_seconds"))

if action not in {"start", "stop"}:
    raise ValueError("action must be either 'start' or 'stop'")

if not pipeline_id:
    raise ValueError("pipeline_id is required")


context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = context.apiUrl().get().rstrip("/")
api_token = context.apiToken().get()


def call_api(method, path, payload=None):
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        f"{workspace_url}{path}",
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            response_body = response.read().decode("utf-8")
            return json.loads(response_body) if response_body else {}
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8")
        raise RuntimeError(f"Databricks API error {error.code}: {error_body}") from error


def pipeline_state():
    pipeline = call_api("GET", f"/api/2.0/pipelines/{pipeline_id}")
    return pipeline.get("state")


def wait_for_state(expected_states):
    deadline = time.time() + timeout_seconds
    expected = set(expected_states)

    while time.time() < deadline:
        state = pipeline_state()
        print(f"Pipeline {pipeline_id} state: {state}")

        if state in expected:
            return state

        if state in {"FAILED", "DELETED"}:
            raise RuntimeError(f"Pipeline {pipeline_id} reached terminal state {state}")

        time.sleep(poll_seconds)

    raise TimeoutError(
        f"Pipeline {pipeline_id} did not reach {sorted(expected)} within {timeout_seconds} seconds"
    )


current_state = pipeline_state()
print(f"Current pipeline state: {current_state}")

if action == "start":
    if current_state == "RUNNING":
        dbutils.notebook.exit("Gateway already running")

    call_api("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", {})
    final_state = wait_for_state({"RUNNING"})
    dbutils.notebook.exit(f"Gateway started: {final_state}")

if current_state == "IDLE":
    dbutils.notebook.exit("Gateway already stopped")

call_api("POST", f"/api/2.0/pipelines/{pipeline_id}/stop", {})
final_state = wait_for_state({"IDLE"})
dbutils.notebook.exit(f"Gateway stopped: {final_state}")
