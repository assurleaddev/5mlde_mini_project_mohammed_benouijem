import json

import numpy as np
import pandas as pd
import pytest


def make_frame(days: int = 70, start: str = "2012-01-01", seed: int = 0) -> pd.DataFrame:
    """Jeu synthétique conforme au contrat, construit sans le CSV réel."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range(start, periods=days, freq="D")
    rows = []
    for d in dates:
        for h in range(24):
            workingday = int(d.weekday() < 5)
            base = 20 + 200 * workingday * (h in (8, 17, 18)) + 80 * (10 <= h <= 20)
            cnt = int(max(1, base + rng.normal(0, 10)))
            casual = cnt // 4
            rows.append({
                "dteday": d.strftime("%Y-%m-%d"), "season": (d.month % 12) // 3 + 1, "yr": d.year - 2011,
                "mnth": d.month, "hr": h, "holiday": 0, "weekday": (d.weekday() + 1) % 7,
                "workingday": workingday, "weathersit": int(rng.integers(1, 4)),
                "temp": round(float(rng.uniform(0.1, 0.9)), 2), "atemp": round(float(rng.uniform(0.1, 0.9)), 4),
                "hum": round(float(rng.uniform(0.2, 0.9)), 2), "windspeed": round(float(rng.uniform(0, 0.5)), 4),
                "casual": casual, "registered": cnt - casual, "cnt": cnt,
            })
    frame = pd.DataFrame(rows)
    frame.insert(0, "instant", range(1, len(frame) + 1))
    return frame


@pytest.fixture
def frame():
    return make_frame()


@pytest.fixture
def project(tmp_path, frame):
    """Arborescence minimale : un CSV et une configuration résolue depuis son propre dossier."""
    (tmp_path / "data").mkdir()
    frame.to_csv(tmp_path / "data" / "hour.csv", index=False)
    config = {
        "project": "bike-demand-test",
        "data": {"raw_path": "data/hour.csv"},
        "output": {"runs_dir": "runs"},
        "split": {"test_days": 7},
        "seed": 42,
        "model": {"learning_rate": 0.1, "max_iter": 30, "max_leaf_nodes": 15,
                  "l2_regularization": 0.0, "min_samples_leaf": 20},
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return {"root": tmp_path, "config_path": path, "config": config}
