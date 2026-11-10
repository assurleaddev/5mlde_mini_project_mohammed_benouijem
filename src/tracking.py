"""Opérations MLflow regroupées dans un module technique : le run_id circule explicitement entre étapes."""
from __future__ import annotations

import json
import os
import platform
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import mlflow
from mlflow.entities import RunStatus
from mlflow.tracking import MlflowClient

ROOT = Path(__file__).resolve().parents[1]


def tracking_uri(config: dict) -> str:
    return os.environ.get("MLFLOW_TRACKING_URI") or config.get("mlflow", {}).get("tracking_uri", "http://localhost:7281")


def client(config: dict) -> MlflowClient:
    uri = tracking_uri(config)
    mlflow.set_tracking_uri(uri)
    mlflow.set_registry_uri(os.environ.get("MLFLOW_REGISTRY_URI", uri))
    return MlflowClient()


def git_info() -> dict:
    def run(*args):
        try:
            return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, timeout=5).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return ""
    return {"git.commit": run("rev-parse", "HEAD") or "unknown",
            "git.dirty": str(bool(run("status", "--porcelain", "--untracked-files=no")))}


def start_run(config: dict, context: dict) -> str:
    """Un run MLflow par tentative de pipeline, ouvert avant l'acquisition."""
    c = client(config)
    experiment_name = config.get("mlflow", {}).get("experiment", "bike-demand")
    experiment = c.get_experiment_by_name(experiment_name)
    experiment_id = experiment.experiment_id if experiment else c.create_experiment(experiment_name)
    tags = {
        "mlflow.runName": context["run_name"],
        "pipeline.trigger": os.environ.get("PIPELINE_TRIGGER", context.get("trigger", "manual")),
        "python.version": platform.python_version(),
        "config.path": config["_config_path"],
        **git_info(),
    }
    tags.update({k: str(v) for k, v in context.get("extra_tags", {}).items()})
    return c.create_run(experiment_id, tags=tags).info.run_id


def log_params(config: dict, run_id: str, params: dict) -> None:
    c = client(config)
    for key, value in params.items():
        c.log_param(run_id, key, str(value)[:6000])


def log_metrics(config: dict, run_id: str, metrics: dict) -> None:
    c = client(config)
    timestamp = int(time.time() * 1000)
    for key, value in metrics.items():
        if value is not None:
            c.log_metric(run_id, key, float(value), timestamp=timestamp)


def set_tags(config: dict, run_id: str, tags: dict) -> None:
    c = client(config)
    for key, value in tags.items():
        c.set_tag(run_id, key, str(value)[:5000])


def log_json(config: dict, run_id: str, payload, artifact_file: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / Path(artifact_file).name
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        client(config).log_artifact(run_id, str(path), str(Path(artifact_file).parent) if "/" in artifact_file else None)


def log_file(config: dict, run_id: str, path, artifact_path: str | None = None) -> None:
    client(config).log_artifact(run_id, str(path), artifact_path)


def log_dataset(config: dict, run_id: str, frame, name: str, context: str, source: str, target: str = "cnt") -> None:
    """Métadonnées de dataset (digest MLflow) : ne remplace pas l'archivage du snapshot."""
    client(config)
    dataset = mlflow.data.from_pandas(frame, source=source, name=name, targets=target if target in frame else None)
    with mlflow.start_run(run_id=run_id, nested=False):
        mlflow.log_input(dataset, context=context)


def end_run(config: dict, run_id: str, status: str = "FINISHED") -> None:
    c = client(config)
    if c.get_run(run_id).info.status == "RUNNING":
        c.set_terminated(run_id, status=status)


def close_run_if_open(config: dict, run_id: str, status: str) -> None:
    """Finalisation tolérante : utilisée par les hooks d'état du flow."""
    try:
        end_run(config, run_id, status)
    except Exception:  # serveur indisponible : diagnostic local, réconciliation après reprise
        pass


@contextmanager
def step(data: dict, name: str):
    """Mesurer la durée d'une étape et tracer l'étape en échec ; l'exception est toujours propagée."""
    config, run_id = data["config"], data["context"].get("mlflow_run_id")
    started = time.perf_counter()
    try:
        yield
    except Exception as exc:
        if run_id:
            try:
                set_tags(config, run_id, {"pipeline.failed_step": name, "pipeline.error": f"{type(exc).__name__}: {exc}"})
                if not data["context"].get("defer_run_close"):
                    end_run(config, run_id, RunStatus.to_string(RunStatus.FAILED))
            except Exception:
                pass
        raise
    else:
        if run_id:
            log_metrics(config, run_id, {f"duration.{name}_seconds": time.perf_counter() - started})
