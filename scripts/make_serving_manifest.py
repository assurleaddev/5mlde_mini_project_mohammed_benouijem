"""Figer la version servie : résout l'alias UNE fois et écrit nom + numéro exact dans le manifeste.

    uv run python -m scripts.make_serving_manifest                 # depuis l'alias champion
    uv run python -m scripts.make_serving_manifest --version 1     # version explicite (ex. retour arrière)
Redémarrer ensuite tous les workers de l'API : changer l'alias ne modifie pas un modèle déjà chargé.
"""
import argparse
import json
from pathlib import Path

from src import registry, tracking
from src.ingestion import read_config
from src.utils import utc_now, write_json

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--config", default=str(root / "config" / "config.json"))
parser.add_argument("--alias", default="champion")
parser.add_argument("--version")
parser.add_argument("--output", default=str(root / "config" / "serving_manifest.json"))
args = parser.parse_args()

config = read_config(args.config)
name = config["mlflow"]["model_name"]
client = tracking.client(config)
version = client.get_model_version(name, args.version) if args.version else \
    registry.current_alias_version(config, name, args.alias)
if version is None:
    raise SystemExit(f"Aucune version pour {name}@{args.alias}")
manifest = {"name": name, "version": str(version.version), "model_uri": f"models:/{name}/{version.version}",
            "source_model_uri": version.source, "run_id": version.run_id,
            "tracking_uri": tracking.tracking_uri(config), "resolved_from": args.version and "version" or args.alias,
            "qualification": version.tags.get("qualification"), "created_at": utc_now()}
write_json(Path(args.output), manifest)
print(json.dumps(manifest, indent=2, ensure_ascii=False))
