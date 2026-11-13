"""Déploiements locaux Prefect (flow.serve) : déclenchement automatique de l'entraînement.

    uv run python deploy.py                       # watcher toutes les 2 min + producteur simulé toutes les 10 min
    uv run python deploy.py --no-producer         # watcher seul (les lots arrivent via simulate_batch.py)

`serve` exécute les runs dans des sous-processus locaux : ce terminal doit avoir PREFECT_API_URL
et MLFLOW_TRACKING_URI. Ctrl+C arrête l'exécutant et met les plannings en pause.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from prefect import flow, get_run_logger, serve

from main import main as training_flow
from src import state
from src.tracking import git_info

ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = str(ROOT / "config" / "config.json")


@flow(name="bike-demand-watch-and-train", retries=0)
def watch_and_train(config_path: str = DEFAULT_CONFIG, force: bool = False):
    """Déclencheur : n'entraîne que si l'ensemble des lots reçus a changé depuis le dernier succès."""
    logger = get_run_logger()
    config_path = Path(config_path).resolve()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    landing = (config_path.parent / config["data"]["landing_dir"]).resolve()
    state_path = (config_path.parent / config["output"]["runs_dir"]).resolve().parent / "state" / "trigger.json"
    fingerprint = state.landing_fingerprint(landing)
    previous = state.read_state(state_path)
    if not force and previous.get("fingerprint") == fingerprint:
        logger.info("Aucun nouveau lot depuis le run %s : pas d'entraînement", previous.get("run_name"))
        return {"skipped": True, "reason": "no new batch"}
    logger.info("Nouveaux lots détectés (%s) : lancement de l'entraînement", len(fingerprint))
    result = training_flow(str(config_path))  # sous-flow : un échec remonte et rend ce flow Failed
    state.mark_trained(state_path, fingerprint, result["run_name"])
    return result


@flow(name="bike-demand-producer-replay", retries=0)
def producer_replay(config_path: str = DEFAULT_CONFIG):
    """Producteur de démonstration : dépose le mois suivant (rejeu du dataset historique)."""
    out = subprocess.run([sys.executable, str(ROOT / "scripts" / "simulate_batch.py"), "--config", config_path],
                         capture_output=True, text=True, cwd=ROOT)
    get_run_logger().info(out.stdout.strip() or out.stderr.strip())
    return out.returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--team", default="med", help="Suffixe identifiant le binôme / l'étudiant")
    parser.add_argument("--watch-minutes", type=int, default=2)
    parser.add_argument("--producer-minutes", type=int, default=10)
    parser.add_argument("--no-producer", action="store_true")
    args = parser.parse_args()
    version = git_info()["git.commit"][:12]
    deployments = [watch_and_train.to_deployment(
        name=f"watch-and-train-{args.team}", interval=timedelta(minutes=args.watch_minutes),
        parameters={"config_path": DEFAULT_CONFIG}, version=version, tags=["bike-demand", args.team],
        description="Réentraîne le modèle quand un nouveau lot validable est arrivé dans data/landing.")]
    if not args.no_producer:
        deployments.append(producer_replay.to_deployment(
            name=f"producer-replay-{args.team}", interval=timedelta(minutes=args.producer_minutes),
            parameters={"config_path": DEFAULT_CONFIG}, version=version, tags=["bike-demand", "demo", args.team],
            description="Simulation : dépose le mois suivant du dataset historique."))
    serve(*deployments, limit=1)
