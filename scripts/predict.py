"""Inférence hors ligne : recharge un modèle publié et prédit un CSV sans la cible."""
import argparse
from pathlib import Path

import joblib
import pandas as pd

from src.schema import FEATURES, ID_COLUMN

parser = argparse.ArgumentParser()
parser.add_argument("--run-dir", required=True)
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

model = joblib.load(Path(args.run_dir) / "model.joblib")
frame = pd.read_csv(args.input)
missing = [c for c in FEATURES if c not in frame.columns]
if missing:
    raise SystemExit(f"Colonnes manquantes : {missing}")
out = pd.DataFrame({ID_COLUMN: frame.get(ID_COLUMN), "cnt_pred": model.predict(frame[FEATURES]).clip(min=0)})
out.to_csv(args.output, index=False)
print(f"{len(out)} prédictions écrites dans {args.output}")
