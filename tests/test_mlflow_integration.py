"""Intégration MLflow sur un store SQLite temporaire : run tracé, version, alias, rechargement, reprise."""
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

mlflow = pytest.importorskip("mlflow")

from main import main  # noqa: E402
from src import registry  # noqa: E402
from src.qualification import load_policy  # noqa: E402


@pytest.fixture
def tracked_project(project, tmp_path, monkeypatch):
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    monkeypatch.setenv("MLFLOW_TRACKING_URI", uri)
    monkeypatch.setenv("MLFLOW_REGISTRY_URI", uri)
    monkeypatch.setattr(registry, "HISTORY_PATH", tmp_path / "selection_history.jsonl")
    # Politique de test : seuils adaptés au petit jeu synthétique, origine autorisée.
    policy = load_policy()
    policy["thresholds"].update(max_mae=1e6, min_r2=-1e6, max_mae_ratio_vs_baseline=10, max_segment_mae_ratio=100,
                                min_test_rows=24)
    monkeypatch.setattr("src.qualification.load_policy", lambda path=None: policy)
    config = dict(project["config"], mlflow={"enabled": True, "experiment": "test", "model_name": "bike-test",
                                             "auto_promote": True})
    project["config_path"].write_text(json.dumps(config))
    monkeypatch.chdir(tmp_path)
    return project, uri


def test_run_is_traced_versioned_promoted_and_reloadable(tracked_project):
    project, uri = tracked_project
    result = main(project["config_path"])
    client = mlflow.tracking.MlflowClient(uri)
    run = client.get_run(result["mlflow_run_id"])
    assert run.info.status == "FINISHED"
    assert run.data.params["data.snapshot_sha256"] and "test.mae" in run.data.metrics
    version = client.get_model_version("bike-test", result["model_version"])
    assert version.tags["qualification"] == "production-ready"
    assert str(client.get_model_version_by_alias("bike-test", "champion").version) == result["model_version"]

    # Rechargement par alias dans un nouveau processus : prédictions identiques au modèle publié localement.
    sample = pd.read_csv(project["root"] / "data" / "hour.csv").tail(5)
    sample_path = project["root"] / "sample.csv"
    sample.to_csv(sample_path, index=False)
    code = ("import mlflow, pandas as pd, joblib, numpy as np, sys;"
            "from src.schema import FEATURES;"
            f"mlflow.set_tracking_uri('{uri}');"
            "m = mlflow.sklearn.load_model('models:/bike-test@champion');"
            f"x = pd.read_csv(r'{sample_path}')[FEATURES];"
            f"local = joblib.load(r'{Path(result['run_dir']) / 'model.joblib'}');"
            "assert np.allclose(m.predict(x), local.predict(x)); print('ok')")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                         cwd=Path(__file__).resolve().parents[1])
    assert out.stdout.strip().endswith("ok"), out.stderr[-2000:]


def test_failed_run_is_closed_as_failed(tracked_project, frame):
    project, uri = tracked_project
    frame.loc[3, "cnt"] = -10
    frame.to_csv(project["root"] / "data" / "hour.csv", index=False)
    with pytest.raises(Exception):
        main(project["config_path"])
    runs = mlflow.tracking.MlflowClient(uri).search_runs(
        [mlflow.get_experiment_by_name("test").experiment_id])
    assert runs[0].info.status == "FAILED" and runs[0].data.tags["pipeline.failed_step"] == "ingestion"


def test_republication_does_not_duplicate_version(tracked_project):
    project, uri = tracked_project
    result = main(project["config_path"])
    config = json.loads(project["config_path"].read_text())
    found = registry.find_published_version({**config, "_config_path": "", "_base_dir": ""}, "bike-test",
                                            result["run_name"])
    assert found is not None and str(found.version) == result["model_version"]
