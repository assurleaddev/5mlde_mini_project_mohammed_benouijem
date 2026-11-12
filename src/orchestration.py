"""Liens Prefect ↔ MLflow : identifiants croisés et finalisation du run MLflow par les hooks du flow."""
from __future__ import annotations

import logging

from src import tracking

logger = logging.getLogger(__name__)

# flow_run_id -> (config, mlflow_run_id) : renseigné par load_data, consommé par les hooks d'état.
_OPEN_RUNS: dict[str, tuple[dict, str]] = {}


def flow_run_id() -> str | None:
    try:
        from prefect.runtime import flow_run
        return flow_run.id
    except Exception:
        return None


def task_logger():
    try:
        from prefect import get_run_logger
        return get_run_logger()
    except Exception:  # hors contexte Prefect (tests unitaires, scripts)
        return logger


def register_mlflow_run(config: dict, context: dict) -> None:
    """Appelé dès que le run MLflow existe : tag croisé, tags du flow run et clôture différée aux hooks."""
    run_id, fr_id = context.get("mlflow_run_id"), flow_run_id()
    if not run_id or not fr_id:
        return
    context["defer_run_close"] = True
    context["prefect_flow_run_id"] = fr_id
    _OPEN_RUNS[fr_id] = (config, run_id)
    tracking.set_tags(config, run_id, {"prefect.flow_run_id": fr_id})
    try:
        from prefect.client.orchestration import get_client
        with get_client(sync_client=True) as client:
            current = client.read_flow_run(fr_id)
            client.update_flow_run(fr_id, tags=sorted(set(current.tags) | {
                f"mlflow_run:{run_id}", f"run:{context['run_name']}", f"trigger:{context.get('trigger', 'manual')}"}))
    except Exception as exc:  # la traçabilité Prefect ne doit pas casser le pipeline
        logger.warning("Impossible de taguer le flow run : %s", exc)


def _close(flow_run, status: str) -> None:
    config, run_id = _OPEN_RUNS.pop(str(flow_run.id), (None, None))
    if run_id:
        tracking.close_run_if_open(config, run_id, status)


def on_flow_failure(flow, flow_run, state) -> None:
    _close(flow_run, "FAILED")


def on_flow_completion(flow, flow_run, state) -> None:
    _close(flow_run, "FINISHED")


def is_transient(task, task_run, state) -> bool:
    """retry_condition_fn : on ne retente que les pannes temporaires de la source, jamais une donnée invalide."""
    from src.acquisition import TransientSourceError
    try:
        state.result()
    except TransientSourceError:
        return True
    except Exception:
        return False
    return False
