"""Étape 4 : évaluer sur test, comparer à la baseline et publier les artefacts seulement après succès."""
from __future__ import annotations

import tempfile
from pathlib import Path

import joblib

from src.metrics import regression_metrics, segment_mae
from src.training import predict_baseline
from src.utils import publish_dir, sha256_file, utc_now, write_json


def evaluate_models(trained_models):
    """Prédire sur test, calculer les métriques et publier le run dans un dossier neuf."""
    data = trained_models
    y_test = data["y_test"]
    predictions = data["pipeline"].predict(data["X_test"])
    baseline_predictions = predict_baseline(data["baseline"], data["X_test"])
    metrics = {
        "model": regression_metrics(y_test, predictions),
        "baseline": regression_metrics(y_test, baseline_predictions),
        "segments": {
            **segment_mae(data["test_frame"], y_test, predictions, "weathersit"),
            **segment_mae(data["test_frame"], y_test, predictions, "workingday"),
        },
        "rows": {"train": data["preprocessing"]["train_rows"], "test": data["preprocessing"]["test_rows"]},
        "training_seconds": data["training_seconds"],
    }

    run_dir = Path(data["context"]["run_dir"])
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix=".tmp-", dir=run_dir.parent))
    joblib.dump(data["pipeline"], tmp_dir / "model.joblib")
    write_json(tmp_dir / "metrics.json", metrics)
    write_json(tmp_dir / "config.json", {k: v for k, v in data["config"].items() if not k.startswith("_")})
    data["split"].to_csv(tmp_dir / "split.csv", index=False)
    write_json(tmp_dir / "manifest.json", {
        "run_name": data["context"]["run_name"], "created_at": utc_now(),
        "sources": data["sources"], "preprocessing": data["preprocessing"],
        "files": {p.name: sha256_file(p) for p in sorted(tmp_dir.iterdir()) if p.is_file()},
    })
    publish_dir(tmp_dir, run_dir)
    return {"run_dir": str(run_dir), "metrics": metrics}
