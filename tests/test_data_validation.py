"""La barrière GX bloque le pipeline avant tout prétraitement (contrat TP2)."""
from unittest import mock

import pandas as pd
import pytest

import main as pipeline
from src import data_validation
from src.schema import DataValidationError
from tests.conftest import make_frame


def month_batch(start="2012-04-01", days=30):
    frame = make_frame(days=days, start=start, seed=3)
    frame["instant"] = frame["instant"] + 100_000
    return frame


def drop_batch(project, batch, name="batch_2012-04.csv"):
    landing = project["root"] / "landing"
    landing.mkdir(exist_ok=True)
    batch.to_csv(landing / name, index=False)
    return landing / name


def test_conformant_batch_passes(tmp_path):
    report = data_validation.validate(month_batch(), "batch", "ok.csv", None, tmp_path)
    assert report["success"] and (tmp_path / "bike_batch-ok-ok.json").exists()


@pytest.mark.parametrize("mutate, expected", [
    (lambda f: f.assign(cnt=f["cnt"].where(f.index > 2, -5)), "cnt"),
    (lambda f: f.drop(columns=["weathersit"]), "weathersit"),
    (lambda f: f.assign(hum=f["hum"].where(f.index > 0, 1.7)), "hum"),
    (lambda f: pd.concat([f, f.head(3)]), "unique"),
    (lambda f: f.astype({"temp": object}).assign(temp=lambda x: x["temp"].where(x.index > 0, "chaud")), "temp"),
    (lambda f: f.head(48), "row_count"),
])
def test_invalid_batches_are_rejected_with_identifiable_cause(tmp_path, mutate, expected):
    with pytest.raises(DataValidationError, match=expected):
        data_validation.validate(mutate(month_batch()), "batch", "bad.csv", None, tmp_path)
    assert (tmp_path / "bike_batch-bad-rejected.json").exists()


def test_gx_crash_is_blocking(tmp_path):
    with mock.patch.object(data_validation, "_run_suite", side_effect=RuntimeError("GX indisponible")):
        with pytest.raises(DataValidationError, match="GX indisponible"):
            data_validation.validate(month_batch(), "batch", "x.csv", None, tmp_path)


def test_rejected_batch_stops_pipeline_before_preprocessing_and_is_quarantined(project):
    path = drop_batch(project, month_batch().assign(cnt=-1))
    with mock.patch.object(pipeline, "preprocess_data") as pre, \
            mock.patch.object(pipeline, "train_models") as train, \
            mock.patch.object(pipeline, "evaluate_models") as evaluate:
        with pytest.raises(DataValidationError):
            pipeline.main(project["config_path"])
    pre.assert_not_called(); train.assert_not_called(); evaluate.assert_not_called()
    assert not path.exists() and len(list((project["root"] / "quarantine").iterdir())) == 1
    assert not (project["root"] / "runs").exists()


def test_valid_batch_is_upserted_into_snapshot(project, frame):
    batch = month_batch(start="2012-03-01", days=20)  # recouvre partiellement l'historique (mars)
    drop_batch(project, batch, "batch_2012-03.csv")
    data = pipeline.load_data(project["config_path"])
    batch_source = data["sources"][1]
    assert batch_source["added"] + batch_source["updated"] == len(batch)
    assert not data["snapshot"].duplicated(["dteday", "hr"]).any()
