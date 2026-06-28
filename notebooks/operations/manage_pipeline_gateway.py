# Databricks notebook source
import json
import time
import urllib.error
import urllib.parse
import urllib.request


dbutils.widgets.text("action", "start")
dbutils.widgets.text("pipeline_id", "")
dbutils.widgets.text("timeout_seconds", "900")
dbutils.widgets.text("poll_seconds", "15")
dbutils.widgets.text("ready_grace_seconds", "0")
dbutils.widgets.text("expected_datasets", "")


action = dbutils.widgets.get("action").strip().lower()
pipeline_id = dbutils.widgets.get("pipeline_id").strip()
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))
poll_seconds = int(dbutils.widgets.get("poll_seconds"))
ready_grace_seconds = int(dbutils.widgets.get("ready_grace_seconds"))
expected_datasets = {
    dataset.strip()
    for dataset in dbutils.widgets.get("expected_datasets").split(",")
    if dataset.strip()
}

if action not in {"start", "stop"}:
    raise ValueError("action must be either 'start' or 'stop'")

if not pipeline_id:
    raise ValueError("pipeline_id is required")


context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = context.apiUrl().get().rstrip("/")
api_token = context.apiToken().get()

READINESS_EVENT_TYPES = {"REALTIME_STARTED", "SNAPSHOT_COMPLETED"}
TERMINAL_UPDATE_STATES = {"CANCELED", "FAILED"}


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


def get_pipeline():
    return call_api("GET", f"/api/2.0/pipelines/{pipeline_id}")


def pipeline_state():
    return get_pipeline().get("state")


def latest_active_update_id(pipeline):
    for update in pipeline.get("latest_updates", []):
        update_state = update.get("state")
        if update_state not in {"CANCELED", "COMPLETED", "FAILED"}:
            return update.get("update_id")
    return None


def wait_for_active_update_id():
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        update_id = latest_active_update_id(get_pipeline())
        if update_id:
            return update_id

        print("Waiting for gateway active update_id to appear")
        time.sleep(poll_seconds)

    raise TimeoutError(f"Gateway active update_id did not appear within {timeout_seconds} seconds")


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


def list_recent_events(max_events=500):
    events = []
    page_token = None

    while len(events) < max_events:
        params = {"max_results": min(100, max_events - len(events))}
        if page_token:
            params["page_token"] = page_token

        response = call_api(
            "GET",
            f"/api/2.0/pipelines/{pipeline_id}/events?{urllib.parse.urlencode(params)}",
        )
        events.extend(response.get("events", []))

        page_token = response.get("next_page_token")
        if not page_token:
            break

    return events


def message_event_type(event):
    message = event.get("message") or ""
    try:
        parsed_message = json.loads(message)
        return parsed_message.get("eventType")
    except json.JSONDecodeError:
        for event_type in READINESS_EVENT_TYPES:
            if event_type in message:
                return event_type
    return None


def update_progress_state(event):
    details = event.get("details", {})
    progress = details.get("update_progress", {})
    if progress.get("state"):
        return progress["state"]

    message = event.get("message") or ""
    marker = " is "
    if marker in message:
        return message.rsplit(marker, 1)[-1].strip(".")

    return None


def readiness_status(update_id):
    pipeline = get_pipeline()
    update_running = (
        pipeline.get("state") == "RUNNING"
        and latest_active_update_id(pipeline) == update_id
    )
    ready_datasets = set()
    terminal_state = None

    for event in list_recent_events():
        origin = event.get("origin", {})
        event_update_id = origin.get("update_id") or origin.get("request_id")
        if event_update_id != update_id:
            continue

        if event.get("event_type") == "update_progress":
            state = update_progress_state(event)
            if state == "RUNNING":
                update_running = True
            elif state in TERMINAL_UPDATE_STATES:
                terminal_state = state

        if event.get("event_type") == "flow_progress":
            event_type = message_event_type(event)
            dataset_name = origin.get("dataset_name")
            if event_type in READINESS_EVENT_TYPES and dataset_name:
                ready_datasets.add(dataset_name)

    if expected_datasets:
        missing_datasets = sorted(expected_datasets - ready_datasets)
        has_ready_flows = not missing_datasets
    else:
        missing_datasets = []
        has_ready_flows = bool(ready_datasets)

    return {
        "ready": update_running and has_ready_flows,
        "update_running": update_running,
        "ready_datasets": sorted(ready_datasets),
        "missing_datasets": missing_datasets,
        "terminal_state": terminal_state,
    }


def wait_for_gateway_readiness(update_id):
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        status = readiness_status(update_id)
        print(
            "Gateway readiness: "
            f"update_id={update_id}, "
            f"update_running={status['update_running']}, "
            f"ready_datasets={status['ready_datasets']}, "
            f"missing_datasets={status['missing_datasets']}"
        )

        if status["terminal_state"]:
            raise RuntimeError(
                f"Gateway update {update_id} reached terminal state {status['terminal_state']}"
            )

        if status["ready"]:
            if ready_grace_seconds > 0:
                print(f"Gateway is ready. Waiting {ready_grace_seconds} seconds before continuing.")
                time.sleep(ready_grace_seconds)
            return status

        time.sleep(poll_seconds)

    raise TimeoutError(
        f"Gateway update {update_id} did not become ready within {timeout_seconds} seconds"
    )


current_pipeline = get_pipeline()
current_state = current_pipeline.get("state")
print(f"Current pipeline state: {current_state}")

if action == "start":
    if current_state == "RUNNING":
        update_id = latest_active_update_id(current_pipeline)
        if not update_id:
            raise RuntimeError("Gateway is RUNNING, but no active update_id was found")
    else:
        update_response = call_api("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", {})
        update_id = update_response.get("update_id")
        if not update_id:
            update_id = wait_for_active_update_id()

    if not update_id:
        raise RuntimeError("Could not determine gateway update_id")

    print(f"Waiting for gateway update {update_id} to become ready")
    final_status = wait_for_gateway_readiness(update_id)
    dbutils.notebook.exit(f"Gateway ready: {json.dumps(final_status)}")

if current_state == "IDLE":
    dbutils.notebook.exit("Gateway already stopped")

call_api("POST", f"/api/2.0/pipelines/{pipeline_id}/stop", {})
final_state = wait_for_state({"IDLE"})
dbutils.notebook.exit(f"Gateway stopped: {final_state}")
