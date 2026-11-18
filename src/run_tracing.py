"""Trace de chaque exécution du pipeline dans PostgreSQL (tracing.pipeline_runs) pour le dashboard Grafana.

Best effort : une base indisponible ne doit jamais faire échouer ni masquer l'état réel du pipeline.
Connexion : variables API_DB_* (mêmes valeurs par défaut que l'API) ; désactivable par config["tracing"]["enabled"].
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _conninfo() -> str:
    env = os.environ.get
    return (f"host={env('API_DB_HOST', '127.0.0.1')} port={env('API_DB_PORT', '5432')} "
            f"dbname={env('API_DB_NAME', 'bike_api')} user={env('API_DB_USER', 'api_writer')} "
            f"password={env('API_DB_PASSWORD', '5mlde-local-api')} connect_timeout=3")


def _parse(value: str | None) -> datetime:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def record(config: dict, context: dict, status: str, result: dict | None = None, error: str | None = None) -> None:
    if not config.get("tracing", {}).get("enabled", True):
        return
    started = _parse(context.get("started_at"))
    ended = datetime.now(timezone.utc)
    result = result or {}
    metrics = result.get("metrics", {})
    row = {
        "run_name": context["run_name"], "flow_run_id": context.get("prefect_flow_run_id"),
        "mlflow_run_id": context.get("mlflow_run_id"), "trigger": context.get("trigger", "manual"),
        "status": status, "failed_step": context.get("failed_step"), "error": (error or "")[:2000] or None,
        "started_at": started, "ended_at": ended, "duration_s": max(0.0, (ended - started).total_seconds()),
        "snapshot_rows": context.get("snapshot_rows"), "batches": context.get("batches"),
        "test_mae": metrics.get("model", {}).get("mae"), "baseline_mae": metrics.get("baseline", {}).get("mae"),
        "model_name": result.get("model_name"), "model_version": result.get("model_version"),
        "qualification": result.get("qualification"),
        "promoted": result.get("promotion", {}).get("promoted") if result.get("promotion") else None,
    }
    try:
        import psycopg
        with psycopg.connect(_conninfo()) as conn:
            conn.execute(
                f"INSERT INTO tracing.pipeline_runs ({', '.join(row)}) VALUES ({', '.join('%(' + k + ')s' for k in row)}) "
                "ON CONFLICT (run_name) DO NOTHING", row)
    except Exception as exc:
        logger.warning("Trace du run %s non écrite dans PostgreSQL : %s", context["run_name"], exc)
