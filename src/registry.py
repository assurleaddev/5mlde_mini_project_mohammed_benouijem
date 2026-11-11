"""Model Registry : versions, tags de qualification, alias champion, historique de sélection et retour arrière."""
from __future__ import annotations

import json
import os
from pathlib import Path

from mlflow.exceptions import MlflowException

from src import tracking
from src.utils import utc_now

HISTORY_PATH = Path(__file__).resolve().parents[1] / "artifacts" / "selection_history.jsonl"


def find_published_version(config: dict, name: str, publication_id: str):
    """Reprise : retrouver une version déjà créée pour cette publication plutôt que d'en créer une seconde."""
    c = tracking.client(config)
    try:
        versions = c.search_model_versions(f"name='{name}' and tags.publication_id='{publication_id}'")
    except MlflowException:
        return None
    return versions[0] if versions else None


def tag_version(config: dict, name: str, version: str, tags: dict) -> None:
    c = tracking.client(config)
    for key, value in tags.items():
        c.set_model_version_tag(name, version, key, str(value)[:5000])


def current_alias_version(config: dict, name: str, alias: str):
    try:
        return tracking.client(config).get_model_version_by_alias(name, alias)
    except MlflowException:
        return None


def set_alias(config: dict, name: str, version: str, alias: str, actor: str, reason: str,
              report: dict | None = None, history_path: Path = HISTORY_PATH) -> dict:
    """Déplacer l'alias et tracer l'opération (ancienne/nouvelle version, acteur, date, motif)."""
    c = tracking.client(config)
    previous = current_alias_version(config, name, alias)
    c.set_registered_model_alias(name, alias, str(version))
    entry = {"date": utc_now(), "model": name, "alias": alias, "previous_version": previous.version if previous else None,
             "new_version": str(version), "actor": actor, "reason": reason,
             "qualification": (report or {}).get("decision")}
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with open(history_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    c.set_model_version_tag(name, str(version), f"selected_as_{alias}_at", entry["date"])
    c.set_registered_model_tag(name, f"last_{alias}_change", json.dumps(entry, ensure_ascii=False)[:5000])
    return entry


def actor() -> str:
    return os.environ.get("USERNAME") or os.environ.get("USER") or "pipeline"
