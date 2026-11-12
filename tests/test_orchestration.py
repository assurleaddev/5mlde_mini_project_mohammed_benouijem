"""Prefect : retry borné sur panne transitoire, aucun retry sur donnée invalide, flow en échec explicite."""
from unittest import mock

import pytest
from prefect import flow

import main as pipeline
from src import ingestion
from src.acquisition import TransientSourceError
from src.schema import DataValidationError


def run_acquisition(config, side_effect):
    task = ingestion.acquire_history.with_options(retry_delay_seconds=0)
    calls = mock.Mock(side_effect=side_effect)

    @flow
    def wrapper():
        return task(config)

    with mock.patch.object(ingestion, "ensure_dataset", calls):
        return wrapper, calls


def test_transient_error_is_retried_then_succeeds(project):
    config = ingestion.read_config(project["config_path"])
    config["data"]["auto_download"] = True
    config["data"]["manifest_path"] = str(project["root"] / "manifest.json")
    (project["root"] / "manifest.json").write_text('{"sha256": "x", "sources": []}')
    wrapper, calls = run_acquisition(config, [TransientSourceError("503"), {"source": "mirror", "path": "p"}])
    with mock.patch.object(ingestion, "ensure_dataset", calls):
        history, provenance = wrapper()
    assert calls.call_count == 2 and provenance["source"] == "mirror" and len(history) > 0


def test_retries_are_bounded_to_three_attempts(project):
    config = ingestion.read_config(project["config_path"])
    config["data"].update(auto_download=True, manifest_path=str(project["root"] / "manifest.json"))
    (project["root"] / "manifest.json").write_text('{"sha256": "x", "sources": []}')
    wrapper, calls = run_acquisition(config, TransientSourceError("503"))
    with mock.patch.object(ingestion, "ensure_dataset", calls):
        with pytest.raises(TransientSourceError):
            wrapper()
    assert calls.call_count == 3  # 2 retries => 3 tentatives au maximum


def test_invalid_data_fails_without_retry(project, frame):
    frame.loc[0, "cnt"] = -3
    frame.to_csv(project["root"] / "data" / "hour.csv", index=False)
    with mock.patch.object(pipeline, "train_models") as train:
        state = pipeline.main(project["config_path"], return_state=True)
    assert state.is_failed()
    with pytest.raises(DataValidationError):
        state.result()
    train.assert_not_called()
