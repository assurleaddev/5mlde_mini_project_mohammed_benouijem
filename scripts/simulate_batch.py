"""Producteur simulé : dépose le lot mensuel suivant dans data/landing (rejeu de l'historique UCI).

Le dataset est historique : on le « rejoue » par mois pour simuler l'arrivée de nouvelles données.
Les incidents contrôlés permettent de démontrer la barrière de qualité.

    uv run python scripts/simulate_batch.py                 # mois suivant
    uv run python scripts/simulate_batch.py --month 2012-09
    uv run python scripts/simulate_batch.py --incident negative_cnt
"""
import argparse
import json
from pathlib import Path

import pandas as pd

INCIDENTS = ("negative_cnt", "missing_column", "out_of_range_hum", "duplicates", "text_value", "truncated")

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--config", default=str(root / "config" / "config.json"))
parser.add_argument("--month", help="AAAA-MM ; par défaut le mois qui suit le dernier lot déposé")
parser.add_argument("--incident", choices=INCIDENTS)
args = parser.parse_args()

config_path = Path(args.config).resolve()
config = json.loads(config_path.read_text(encoding="utf-8"))
raw = pd.read_csv((config_path.parent / config["data"]["raw_path"]).resolve())
landing = (config_path.parent / config["data"]["landing_dir"]).resolve()
landing.mkdir(parents=True, exist_ok=True)

if args.month:
    month = pd.Period(args.month, freq="M")
else:
    existing = sorted(p.stem.removeprefix("batch_") for p in landing.glob("batch_*.csv"))
    last = pd.Period(existing[-1], freq="M") if existing else pd.Period(config["data"]["history_until"][:7], freq="M")
    month = last + 1

batch = raw[raw["dteday"].str.startswith(str(month))].copy()
if batch.empty:
    raise SystemExit(f"Aucune donnée source pour {month} (fin du rejeu ?)")

if args.incident == "negative_cnt":
    batch.loc[batch.index[:3], "cnt"] = -5
elif args.incident == "missing_column":
    batch = batch.drop(columns=["weathersit"])
elif args.incident == "out_of_range_hum":
    batch.loc[batch.index[:10], "hum"] = 1.7
elif args.incident == "duplicates":
    batch = pd.concat([batch, batch.head(24)])
elif args.incident == "text_value":
    batch["temp"] = batch["temp"].astype(object)
    batch.loc[batch.index[0], "temp"] = "chaud"
elif args.incident == "truncated":
    batch = batch.head(48)

target = landing / f"batch_{month}.csv"
batch.to_csv(target, index=False)
print(f"Lot déposé : {target} ({len(batch)} lignes{', incident ' + args.incident if args.incident else ''})")
