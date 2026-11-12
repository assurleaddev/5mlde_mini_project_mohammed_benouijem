"""Étape 1 : acquisition (historique + lots reçus), snapshot versionné et barrière de validation GX."""
from __future__ import annotations

import json
import logging
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE

from src import data_validation, orchestration, tracking
from src.acquisition import ensure_dataset
from src.schema import DATE_COLUMN, ID_COLUMN, REQUIRED_COLUMNS, DataValidationError
from src.utils import resolve, sha256_file, utc_now, write_json

logger = logging.getLogger(__name__)


def read_config(config_path) -> dict:
    config_path = Path(config_path).resolve()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["_config_path"] = str(config_path)
    config["_base_dir"] = str(config_path.parent)
    return config


def run_context(config: dict) -> dict:
    base_dir = Path(config["_base_dir"])
    run_name = config.get("run_name") or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])
    run_dir = resolve(base_dir, config["output"]["runs_dir"]) / run_name
    if run_dir.exists():
        raise FileExistsError(f"Dossier de run déjà existant, refus de l'écraser : {run_dir}")
    diagnostics_dir = resolve(base_dir, config["output"].get("diagnostics_dir", "../artifacts/diagnostics")) / run_name
    return {"run_name": run_name, "run_dir": str(run_dir), "diagnostics_dir": str(diagnostics_dir),
            "started_at": utc_now()}


@task(name="acquire-history", retries=2, retry_delay_seconds=[5, 20], retry_condition_fn=orchestration.is_transient,
      timeout_seconds=300, cache_policy=NO_CACHE, persist_result=False)
def acquire_history(config: dict) -> tuple[pd.DataFrame, dict]:
    """Corpus historique : CSV de référence vérifié par empreinte, téléchargé si absent."""
    base_dir = Path(config["_base_dir"])
    raw_path = resolve(base_dir, config["data"]["raw_path"])
    manifest_path = resolve(base_dir, config["data"].get("manifest_path", "../data/manifest.json"))
    if config["data"].get("auto_download", True) and manifest_path.exists():
        provenance = ensure_dataset(json.loads(manifest_path.read_text(encoding="utf-8")), raw_path,
                                    **config["data"].get("retry", {}))
    elif raw_path.exists():
        provenance = {"source": "local", "path": str(raw_path), "sha256": sha256_file(raw_path)}
    else:
        raise FileNotFoundError(f"Source absente : {raw_path} (lancer scripts/download_data.py)")
    history = pd.read_csv(raw_path)
    missing = [c for c in REQUIRED_COLUMNS if c not in history.columns]
    if missing:
        raise DataValidationError(f"Colonnes indispensables absentes : {missing}")
    until = config["data"].get("history_until")
    if until:
        history = history[history[DATE_COLUMN] <= until]
    provenance.update(kind="history", rows=int(len(history)), history_until=until)
    return history, provenance


def list_batches(config: dict) -> list[Path]:
    landing = config["data"].get("landing_dir")
    if not landing:
        return []
    landing_dir = resolve(Path(config["_base_dir"]), landing)
    return sorted(landing_dir.glob("batch_*.csv")) if landing_dir.exists() else []


def quarantine(path: Path, config: dict) -> Path:
    """Isoler un lot rejeté : il ne sera pas rejoué en boucle tant qu'il n'est pas corrigé et redéposé."""
    target_dir = resolve(Path(config["_base_dir"]), config["data"].get("quarantine_dir", "../data/quarantine"))
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{path.stem}.{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.csv"
    shutil.move(str(path), target)
    return target


def upsert(history: pd.DataFrame, batch: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Une seule observation par (dteday, hr) : le lot le plus récent remplace l'historique."""
    keys = set(zip(history[DATE_COLUMN], history["hr"]))
    updated = sum((d, h) in keys for d, h in zip(batch[DATE_COLUMN], batch["hr"]))
    merged = pd.concat([history, batch]).drop_duplicates([DATE_COLUMN, "hr"], keep="last")
    return merged.sort_values([DATE_COLUMN, "hr"]).reset_index(drop=True), len(batch) - updated, updated


def mlflow_enabled(config: dict) -> bool:
    return config.get("mlflow", {}).get("enabled", True)


@task(name="ingestion", retries=0, timeout_seconds=600, cache_policy=NO_CACHE, persist_result=False)
def load_data(config_path):
    """Acquérir, fusionner les lots reçus, figer un snapshot et le valider avant tout prétraitement."""
    config = read_config(config_path)
    context = run_context(config)
    context["trigger"] = config.get("trigger", "manual")
    if mlflow_enabled(config):
        context["mlflow_run_id"] = tracking.start_run(config, context)
        orchestration.register_mlflow_run(config, context)
    data = {"config": config, "context": context}
    log = orchestration.task_logger()
    log.info("Run %s démarré (MLflow %s)", context["run_name"], context.get("mlflow_run_id"))
    with tracking.step(data, "ingestion"):
        result = _load(config, context)
    if mlflow_enabled(config):
        _trace_ingestion(result)
    log.info("Snapshot %s : %s lignes, %s lot(s) fusionné(s)", context["snapshot_sha256"][:12], len(result["snapshot"]),
             len(result["sources"]) - 1)
    return result


def _load(config: dict, context: dict) -> dict:
    diagnostics = Path(context["diagnostics_dir"])
    history, provenance = acquire_history(config)
    sources, reports = [provenance], []
    snapshot = history
    for batch_path in list_batches(config):
        batch = pd.read_csv(batch_path)
        try:
            reports.append(data_validation.validate(batch, "batch", batch_path.name, sha256_file(batch_path),
                                                    diagnostics))
        except data_validation.DataValidationError:
            moved = quarantine(batch_path, config)
            logger.error("Lot %s rejeté et mis en quarantaine : %s", batch_path.name, moved)
            raise
        snapshot, added, updated = upsert(snapshot, batch)
        sources.append({"kind": "batch", "path": str(batch_path), "rows": int(len(batch)),
                        "sha256": sha256_file(batch_path), "added": added, "updated": updated})

    snapshot = snapshot[[*REQUIRED_COLUMNS, *[c for c in ("casual", "registered") if c in snapshot.columns]]]
    snapshot_dir = resolve(Path(config["_base_dir"]), config["output"].get("snapshots_dir", "../artifacts/snapshots"))
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    tmp = snapshot_dir / f".{context['run_name']}.csv"
    snapshot.to_csv(tmp, index=False)
    digest = sha256_file(tmp)
    snapshot_path = snapshot_dir / f"snapshot-{digest[:16]}.csv"
    tmp.replace(snapshot_path)

    reports.append(data_validation.validate(snapshot, "snapshot", snapshot_path.name, digest, diagnostics,
                                            min_rows=int(config["data"].get("min_snapshot_rows", 24 * 60))))
    write_json(diagnostics / "sources.json", sources)
    # On transmet exactement le DataFrame validé, pas une relecture d'un fichier mutable.
    context.update(snapshot_path=str(snapshot_path), snapshot_sha256=digest, data_origin="uci-replay")
    return {"config": config, "context": context, "snapshot": snapshot, "sources": sources,
            "validation_reports": reports, "id_column": ID_COLUMN}


def _trace_ingestion(data: dict) -> None:
    config, context, run_id = data["config"], data["context"], data["context"]["mlflow_run_id"]
    batches = [s for s in data["sources"] if s["kind"] == "batch"]
    tracking.log_params(config, run_id, {
        "data.history_until": config["data"].get("history_until"),
        "data.origin": context["data_origin"],
        "data.snapshot_sha256": context["snapshot_sha256"],
        "data.batches": ",".join(Path(b["path"]).name for b in batches) or "none",
        "data.cache_policy": "sha256-verified local copy, download if absent",
    })
    tracking.log_metrics(config, run_id, {
        "data.rows_snapshot": len(data["snapshot"]),
        "data.rows_history": data["sources"][0]["rows"],
        "data.rows_added": sum(b["added"] for b in batches),
        "data.rows_updated": sum(b["updated"] for b in batches),
    })
    tracking.set_tags(config, run_id, {"data.snapshot_sha256": context["snapshot_sha256"],
                                       "data.period_end": str(data["snapshot"][DATE_COLUMN].max())})
    tracking.log_file(config, run_id, context["snapshot_path"], "data")
    tracking.log_json(config, run_id, data["sources"], "data/sources.json")
    for report in data["validation_reports"]:
        tracking.log_file(config, run_id, report["report_path"], "validation")
    tracking.log_dataset(config, run_id, data["snapshot"], name=Path(context["snapshot_path"]).name,
                         context="snapshot", source=context["snapshot_path"])
