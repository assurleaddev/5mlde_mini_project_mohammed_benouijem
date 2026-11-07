"""Étape 1 : lire la configuration et la source, contrôler la structure, préparer le contexte du run."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.schema import REQUIRED_COLUMNS, DataValidationError
from src.utils import resolve


def _read_config(config_path) -> dict:
    config_path = Path(config_path).resolve()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["_config_path"] = str(config_path)
    config["_base_dir"] = str(config_path.parent)
    return config


def _run_context(config: dict) -> dict:
    base_dir = Path(config["_base_dir"])
    run_name = config.get("run_name") or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    )
    run_dir = resolve(base_dir, config["output"]["runs_dir"]) / run_name
    if run_dir.exists():
        raise FileExistsError(f"Dossier de run déjà existant, refus de l'écraser : {run_dir}")
    return {"run_name": run_name, "run_dir": str(run_dir)}


def load_data(config_path):
    """Lire configuration et CSV ; vérifier la présence des colonnes du contrat."""
    config = _read_config(config_path)
    context = _run_context(config)
    raw_path = resolve(Path(config["_base_dir"]), config["data"]["raw_path"])
    if not raw_path.exists():
        raise FileNotFoundError(f"Source absente : {raw_path} (lancer scripts/download_data.py)")
    snapshot = pd.read_csv(raw_path)
    missing = [c for c in REQUIRED_COLUMNS if c not in snapshot.columns]
    if missing:
        raise DataValidationError(f"Colonnes indispensables absentes : {missing}")
    return {"config": config, "context": context, "snapshot": snapshot,
            "sources": [{"path": str(raw_path), "rows": len(snapshot)}]}
