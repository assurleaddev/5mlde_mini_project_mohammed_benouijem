"""Sélection manuelle et retour arrière : déplace un alias seulement vers une version qualifiée.

    uv run python scripts/promote.py --version 3            # sélectionner la version 3 comme champion
    uv run python scripts/promote.py --rollback             # revenir à la version précédemment sélectionnée
    uv run python scripts/promote.py --show                 # état de l'alias et historique
"""
import argparse
import json
from pathlib import Path

from src import qualification, registry, tracking
from src.ingestion import read_config

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--config", default=str(root / "config" / "config.json"))
parser.add_argument("--version")
parser.add_argument("--rollback", action="store_true")
parser.add_argument("--show", action="store_true")
parser.add_argument("--reason", default="sélection manuelle")
args = parser.parse_args()

config = read_config(args.config)
name = config["mlflow"]["model_name"]
policy = qualification.load_policy()
alias = policy["promotion"]["alias"]
client = tracking.client(config)

if args.show:
    current = registry.current_alias_version(config, name, alias)
    print(f"{name}@{alias} -> version {current.version if current else 'aucune'}")
    if registry.HISTORY_PATH.exists():
        print(registry.HISTORY_PATH.read_text(encoding="utf-8"))
    raise SystemExit(0)

if args.rollback:
    history = [json.loads(line) for line in registry.HISTORY_PATH.read_text(encoding="utf-8").splitlines()
               if json.loads(line)["model"] == name and json.loads(line)["alias"] == alias]
    if not history or not history[-1]["previous_version"]:
        raise SystemExit("Aucune version précédente à restaurer")
    target, reason = history[-1]["previous_version"], f"retour arrière depuis v{history[-1]['new_version']}"
else:
    if not args.version:
        raise SystemExit("--version, --rollback ou --show requis")
    target, reason = args.version, args.reason

version = client.get_model_version(name, target)
tags = version.tags
# Un tag saisi à la main ne suffit pas : on revérifie la qualification avec la politique courante.
if tags.get("qualification") != qualification.PRODUCTION_READY:
    raise SystemExit(f"Refus : v{target} est '{tags.get('qualification')}' ({tags.get('qualification.reason')})")
if tags.get("qualification.policy") != f"{policy['policy_id']}@{policy['version']}":
    raise SystemExit(f"Refus : v{target} a été qualifiée avec {tags.get('qualification.policy')}, "
                     f"politique courante {policy['policy_id']}@{policy['version']}")
entry = registry.set_alias(config, name, target, alias, registry.actor(), reason)
print(json.dumps(entry, indent=2, ensure_ascii=False))
