"""Récupérer le dataset de référence (UCI, fallback miroir GitHub) et vérifier son empreinte."""
import json
import logging
from pathlib import Path

from src.acquisition import ensure_dataset

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "data" / "manifest.json").read_text(encoding="utf-8"))
print(ensure_dataset(manifest, root / "data" / "raw" / manifest["file"]))
