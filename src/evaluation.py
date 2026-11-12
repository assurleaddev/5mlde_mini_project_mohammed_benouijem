"""Étape 4 : évaluer, publier le run, versionner le modèle dans MLflow et appliquer la qualification."""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import joblib
import mlflow
import numpy as np
from mlflow.models import infer_signature
from prefect import task
from prefect.cache_policies import NO_CACHE

from src import orchestration, qualification, registry, tracking
from src.metrics import regression_metrics, segment_mae
from src.training import predict_baseline
from src.utils import publish_dir, sha256_file, utc_now, write_json

logger = logging.getLogger(__name__)


def compute_metrics(data: dict) -> tuple[dict, np.ndarray]:
    y_test = data["y_test"]
    predictions = data["pipeline"].predict(data["X_test"])
    baseline_predictions = predict_baseline(data["baseline"], data["X_test"])
    metrics = {
        "model": regression_metrics(y_test, predictions),
        "baseline": regression_metrics(y_test, baseline_predictions),
        "segments": {
            **segment_mae(data["test_frame"], y_test, predictions, "weathersit"),
            **segment_mae(data["test_frame"], y_test, predictions, "workingday"),
        },
        "rows": {"train": data["preprocessing"]["train_rows"], "test": data["preprocessing"]["test_rows"]},
        "training_seconds": data["training_seconds"],
    }
    return metrics, predictions


def publish_local_run(data: dict, metrics: dict) -> Path:
    """Artefacts locaux écrits dans un dossier temporaire, publiés seulement si tout a réussi."""
    run_dir = Path(data["context"]["run_dir"])
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix=".tmp-", dir=run_dir.parent))
    joblib.dump(data["pipeline"], tmp_dir / "model.joblib")
    write_json(tmp_dir / "metrics.json", metrics)
    write_json(tmp_dir / "config.json", {k: v for k, v in data["config"].items() if not k.startswith("_")})
    data["split"].to_csv(tmp_dir / "split.csv", index=False)
    write_json(tmp_dir / "manifest.json", {
        "run_name": data["context"]["run_name"], "created_at": utc_now(),
        "mlflow_run_id": data["context"].get("mlflow_run_id"),
        "snapshot_sha256": data["context"].get("snapshot_sha256"),
        "sources": data["sources"], "preprocessing": data["preprocessing"],
        "files": {p.name: sha256_file(p) for p in sorted(tmp_dir.iterdir()) if p.is_file()},
    })
    publish_dir(tmp_dir, run_dir)
    return run_dir


def _flat_metrics(metrics: dict, champion_mae: float | None) -> dict:
    flat = {f"test.{k}": v for k, v in metrics["model"].items()}
    flat.update({f"baseline.test.{k}": v for k, v in metrics["baseline"].items()})
    flat.update({f"segment.{k.replace('=', '_')}.mae": v["mae"] for k, v in metrics["segments"].items()})
    flat["test.mae_ratio_vs_baseline"] = metrics["model"]["mae"] / metrics["baseline"]["mae"]
    if champion_mae is not None:
        flat["champion.test.mae_same_split"] = champion_mae
    return flat


def _publish_mlflow(data: dict, metrics: dict, predictions: np.ndarray, run_dir: Path) -> dict:
    config, context = data["config"], data["context"]
    run_id = context["mlflow_run_id"]
    name = config["mlflow"]["model_name"]
    policy = qualification.load_policy()
    alias = policy["promotion"]["alias"]

    # 1. Modèle complet (encodage + régresseur) avec signature et exemple d'entrée sans cible.
    example = data["X_test"].head(5)
    version = registry.find_published_version(config, name, context["run_name"])
    tracking.client(config)
    if version is None:
        with mlflow.start_run(run_id=run_id):
            info = mlflow.sklearn.log_model(
                data["pipeline"], name="model", input_example=example,
                signature=infer_signature(example, predictions[:5]),
                serialization_format="cloudpickle", pip_requirements=["scikit-learn==1.6.1", "pandas", "numpy"])
        model_uri = info.model_uri
        reloaded = mlflow.sklearn.load_model(model_uri)
        reload_ok = bool(np.allclose(reloaded.predict(data["X_test"]), predictions, atol=1e-8))
        registered = mlflow.register_model(model_uri, name, tags={"publication_id": context["run_name"]})
        version_number = registered.version
    else:  # reprise : la publication existe déjà, on ne crée pas de doublon
        logger.warning("Version %s déjà publiée pour %s : reprise sans nouvelle version", version.version,
                       context["run_name"])
        model_uri, version_number, reload_ok = version.source, version.version, True

    # 2. Comparaison équitable : le champion actuel est réévalué sur le même split de test.
    champion = registry.current_alias_version(config, name, alias)
    champion_mae = None
    if champion is not None and str(champion.version) != str(version_number):
        champion_model = mlflow.sklearn.load_model(f"models:/{name}/{champion.version}")
        champion_mae = regression_metrics(data["y_test"], champion_model.predict(data["X_test"]))["mae"]

    tracking.log_metrics(config, run_id, _flat_metrics(metrics, champion_mae))
    tracking.log_json(config, run_id, metrics, "evaluation/metrics.json")
    tracking.log_file(config, run_id, run_dir / "split.csv", "evaluation")

    # 3. Qualification automatique puis décision de mise en service (deux opérations distinctes).
    evidence = {**metrics, "test_rows": metrics["rows"]["test"], "reload_ok": reload_ok,
                "data_origin": context.get("data_origin")}
    decision = qualification.decide(evidence, policy)
    decision.update(evaluated_at=utc_now(), model=name, version=str(version_number), mlflow_run_id=run_id,
                    champion_version=champion.version if champion else None, champion_mae_same_split=champion_mae)
    registry.tag_version(config, name, version_number, {
        "qualification": decision["decision"], "qualification.reason": decision["reason"],
        "qualification.policy": f"{decision['policy_id']}@{decision['policy_version']}",
        "qualification.date": decision["evaluated_at"], "mlflow_run_id": run_id,
        "snapshot_sha256": context.get("snapshot_sha256"), "test.mae": round(metrics["model"]["mae"], 4),
        "data_origin": context.get("data_origin"),
    })
    promotion = {"promoted": False, "reason": "Qualification non satisfaite"}
    if decision["decision"] == qualification.PRODUCTION_READY and config["mlflow"].get("auto_promote", False):
        replace, why = qualification.should_replace_champion(metrics["model"]["mae"], champion_mae, policy)
        promotion = {"promoted": replace, "reason": why}
        if replace:
            promotion["selection"] = registry.set_alias(config, name, version_number, alias, "pipeline", why, decision)
    decision["promotion"] = promotion
    tracking.log_json(config, run_id, decision, f"qualification/decision-{decision['evaluated_at'].replace(':', '')}.json")
    tracking.set_tags(config, run_id, {"model.name": name, "model.version": version_number,
                                       "qualification": decision["decision"],
                                       "promotion": "promoted" if promotion["promoted"] else "not-promoted"})
    return {"model_name": name, "model_version": str(version_number), "model_uri": model_uri,
            "qualification": decision["decision"], "qualification_reason": decision["reason"],
            "promotion": promotion}


@task(name="evaluation", retries=0, timeout_seconds=600, cache_policy=NO_CACHE, persist_result=False)
def evaluate_models(trained_models):
    """Prédire sur test, calculer les métriques, publier le run puis le modèle versionné."""
    data = trained_models
    with tracking.step(data, "evaluation"):
        metrics, predictions = compute_metrics(data)
        run_dir = publish_local_run(data, metrics)
        result = {"run_name": data["context"]["run_name"], "run_dir": str(run_dir), "metrics": metrics}
        run_id = data["context"].get("mlflow_run_id")
        if run_id:
            result.update(mlflow_run_id=run_id, **_publish_mlflow(data, metrics, predictions, run_dir))
            if not data["context"].get("defer_run_close"):
                tracking.end_run(data["config"], run_id, "FINISHED")
    orchestration.task_logger().info("Run %s : MAE=%.2f (baseline %.2f), qualification=%s", result["run_name"],
                metrics["model"]["mae"], metrics["baseline"]["mae"], result.get("qualification", "n/a"))
    return result
