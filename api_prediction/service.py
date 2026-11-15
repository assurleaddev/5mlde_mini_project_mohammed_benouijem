"""Chargement du couple manifeste/modèle une seule fois par worker et écritures transactionnelles."""
from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import mlflow
from mlflow.tracking import MlflowClient
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from api_prediction.settings import Settings

logger = logging.getLogger("api_prediction")


@dataclass
class LoadedModel:
    name: str
    version: str
    run_id: str
    model_uri: str
    tracking_uri: str
    qualification: str | None
    model: object
    model_id: uuid.UUID | None = None


def load_manifest(settings: Settings) -> dict:
    if not settings.manifest_path.exists():
        raise RuntimeError(f"Manifeste absent : {settings.manifest_path} (scripts/make_serving_manifest.py)")
    return json.loads(settings.manifest_path.read_text(encoding="utf-8"))


def load_model(settings: Settings) -> LoadedModel:
    """Charge la version précise du manifeste (pas l'alias) et vérifie sa qualification."""
    manifest = load_manifest(settings)
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", manifest["tracking_uri"])
    mlflow.set_tracking_uri(tracking_uri)
    version = MlflowClient().get_model_version(manifest["name"], str(manifest["version"]))
    qualification = version.tags.get("qualification")
    if qualification != "production-ready" and not settings.allow_unqualified:
        raise RuntimeError(f"{manifest['name']} v{manifest['version']} n'est pas qualifiée ({qualification}) ; "
                           "ALLOW_UNQUALIFIED_MODEL=1 réservé aux démonstrations")
    model = mlflow.sklearn.load_model(f"models:/{manifest['name']}/{manifest['version']}")
    return LoadedModel(manifest["name"], str(manifest["version"]), version.run_id, manifest["model_uri"],
                       tracking_uri, qualification, model)


def register_model_version(pool: ConnectionPool, loaded: LoadedModel) -> uuid.UUID:
    """Plusieurs workers peuvent découvrir la même version : l'unicité SQL tranche, on relit l'identifiant."""
    with pool.connection() as conn:
        conn.execute(
            """INSERT INTO tracing.model_versions (model_id, tracking_uri, registered_name, version, mlflow_run_id,
                                                   model_uri, qualification)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (tracking_uri, registered_name, version) DO NOTHING""",
            (uuid.uuid4(), loaded.tracking_uri, loaded.name, int(loaded.version), loaded.run_id, loaded.model_uri,
             loaded.qualification))
        row = conn.execute(
            """SELECT model_id, mlflow_run_id FROM tracing.model_versions
               WHERE tracking_uri = %s AND registered_name = %s AND version = %s""",
            (loaded.tracking_uri, loaded.name, int(loaded.version))).fetchone()
    if row[1] != loaded.run_id:
        raise RuntimeError("Métadonnées incohérentes pour cette version de modèle en base")
    return row[0]


def insert_request(conn, trace: dict) -> None:
    conn.execute(
        """INSERT INTO tracing.api_requests (request_id, started_at, endpoint, http_status, error_code, n_items,
               processing_duration_ms, inference_duration_ms, worker_pid, instance_id, api_revision, campaign_id)
           VALUES (%(request_id)s, %(started_at)s, %(endpoint)s, %(http_status)s, %(error_code)s, %(n_items)s,
                   %(processing_duration_ms)s, %(inference_duration_ms)s, %(worker_pid)s, %(instance_id)s,
                   %(api_revision)s, %(campaign_id)s)""", trace)


def insert_predictions(conn, rows: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO tracing.predictions (prediction_id, request_id, item_index, model_id, features, predicted_cnt)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            [(p, r, i, m, Jsonb(f), v) for p, r, i, m, f, v in rows])


def local_error_log(settings: Settings, payload: dict) -> None:
    """Dernier recours si PostgreSQL est indisponible : journal local structuré."""
    try:
        settings.error_log.parent.mkdir(parents=True, exist_ok=True)
        with open(settings.error_log, "a", encoding="utf-8") as handle:
            handle.write(json.dumps({"logged_at": datetime.now(timezone.utc).isoformat(), **payload},
                                    default=str) + "\n")
    except OSError:
        logger.exception("Impossible d'écrire le journal local")
