"""Tests de comportement du pipeline (contrat TP1)."""
import json

import joblib
import numpy as np
import pandas as pd
import pytest

from main import main
from src.ingestion import load_data
from src.preprocessing import preprocess_data
from src.schema import FEATURES, DataValidationError


def write(project, frame):
    frame.to_csv(project["root"] / "data" / "hour.csv", index=False)


def test_missing_column_is_explicit(project, frame):
    write(project, frame.drop(columns=["hum"]))
    with pytest.raises(DataValidationError, match="hum"):
        load_data(project["config_path"])


def test_non_numeric_value_blocks_training(project, frame):
    frame["temp"] = frame["temp"].astype(object)
    frame.loc[5, "temp"] = "chaud"
    write(project, frame)
    with pytest.raises(DataValidationError, match="temp"):
        main(project["config_path"])
    assert not (project["root"] / "runs").exists() or not any((project["root"] / "runs").iterdir())


def test_temporal_split_has_no_overlap_and_no_future_in_train(project):
    prepared = preprocess_data(load_data(project["config_path"]))
    split = prepared["split"]
    assert not set(split[split.split == "train"].instant) & set(split[split.split == "test"].instant)
    assert prepared["preprocessing"]["train_period"][1] < prepared["preprocessing"]["test_period"][0]


def test_nominal_run_publishes_artifacts_and_reload_is_identical(project, tmp_path):
    from src.evaluation import evaluate_models
    from src.training import train_models
    trained = train_models(preprocess_data(load_data(project["config_path"])))
    in_memory = trained["pipeline"].predict(trained["X_test"])
    result = evaluate_models(trained)
    run_dir = project["root"] / "runs" / trained["context"]["run_name"]
    assert result["run_dir"] == str(run_dir)
    assert {"model.joblib", "metrics.json", "split.csv", "config.json", "manifest.json"} <= {
        p.name for p in run_dir.iterdir()}
    assert np.isfinite(result["metrics"]["model"]["mae"])
    reloaded = joblib.load(run_dir / "model.joblib")
    np.testing.assert_allclose(in_memory, reloaded.predict(trained["X_test"]), atol=1e-10)


def test_unknown_category_is_still_predicted(project):
    main(project["config_path"])
    run_dir = next((project["root"] / "runs").iterdir())
    model = joblib.load(run_dir / "model.joblib")
    row = pd.DataFrame([{f: 0 for f in FEATURES}]).assign(season=1, mnth=1, hr=8, weekday=2, weathersit=4,
                                                          workingday=1, temp=0.3, atemp=0.3, hum=0.5)
    assert np.isfinite(model.predict(row)).all()


def test_existing_run_dir_is_refused(project):
    config = dict(project["config"], run_name="fixed")
    project["config_path"].write_text(json.dumps(config))
    main(project["config_path"])
    with pytest.raises(FileExistsError):
        main(project["config_path"])


def test_duplicates_are_rejected(project, frame):
    write(project, pd.concat([frame, frame.iloc[[0]].assign(instant=99999)]))
    with pytest.raises(DataValidationError, match="unique|Doublons"):
        main(project["config_path"])
