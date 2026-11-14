import json
import logging
import os
from datetime import UTC, datetime
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


def _send_webhook_alert(message: str) -> None:
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    request = Request(
        webhook_url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10):
            logger.info("Webhook alert sent")
    except Exception:
        logger.exception("Unable to send webhook alert")


def notify_pipeline_failure(context: dict) -> None:
    exception = context.get("exception")
    if not exception:
        return

    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    dag_id = getattr(dag_run, "dag_id", "unknown")
    run_id = getattr(dag_run, "run_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    message = (
        f"Airflow pipeline failure: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run: {run_id}\n"
        f"Error: {exception or 'unknown error'}"
    )
    _send_webhook_alert(message)
    logger.info("Pipeline failure alert sent for %s/%s", dag_id, task_id)


def notify_stale_data(data_loaded_at: str | None, threshold_hours: int | str | None = None) -> None:
    if not data_loaded_at:
        return

    threshold = int(
        threshold_hours
        if threshold_hours is not None
        else os.getenv("FRESHNESS_THRESHOLD_HOURS", "24")
    )
    try:
        loaded_at = datetime.fromisoformat(data_loaded_at.replace("Z", "+00:00"))
    except ValueError:
        logger.warning(
            "Ignoring stale-data alert for invalid data_loaded_at value: %s", data_loaded_at
        )
        return

    if loaded_at.tzinfo is None:
        loaded_at = loaded_at.replace(tzinfo=UTC)

    age_hours = (datetime.now(UTC) - loaded_at).total_seconds() / 3600
    if age_hours <= threshold:
        return

    message = (
        "Data freshness alert: real_estate_analytics dataset is stale.\n"
        f"Last loaded: {data_loaded_at}\n"
        f"Threshold: {threshold} hours\n"
        f"Current age: greater than {threshold} hours"
    )
    _send_webhook_alert(message)
    logger.info("Data freshness alert sent for real_estate_analytics")
