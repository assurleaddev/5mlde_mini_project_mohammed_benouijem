"""Contrats HTTP et traçabilité. Requiert PostgreSQL (schéma docker/postgres/init-db.sql) ; sinon ignoré."""
import uuid

import numpy as np
import pytest

psycopg = pytest.importorskip("psycopg")
from fastapi.testclient import TestClient  # noqa: E402

from api_prediction import main as api, service  # noqa: E402
from api_prediction.settings import Settings  # noqa: E402

HOUR = {"season": 3, "yr": 1, "mnth": 7, "hr": 17, "holiday": 0, "weekday": 3, "workingday": 1,
        "weathersit": 1, "temp": 0.74, "atemp": 0.68, "hum": 0.45, "windspeed": 0.19}


class StubModel:
    def predict(self, frame):
        return np.asarray(frame["hr"] * 10.0 + frame["temp"] * 100)


def db_available():
    try:
        with psycopg.connect(Settings().conninfo) as conn:
            conn.execute("SELECT 1 FROM tracing.api_requests LIMIT 1")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not db_available(), reason="PostgreSQL de traçabilité indisponible")


@pytest.fixture(scope="module")
def client():
    stub = service.LoadedModel("bike-test-stub", str(np.random.randint(1, 10**9)), "run-stub", "models:/stub/1",
                               "test://stub", "production-ready", StubModel())
    original = service.load_model
    service.load_model = lambda settings: stub
    # Les données des tests sont marquées (campagne « pytest ») et exclues du dashboard.
    with TestClient(api.app, headers={"X-Test-Campaign": "pytest"}) as test_client:
        yield test_client
    service.load_model = original


def count(sql, *params):
    with psycopg.connect(Settings().conninfo) as conn:
        return conn.execute(sql, params).fetchone()[0]


def test_ready_and_identity(client):
    assert client.get("/health/ready").status_code == 200
    assert client.get("/model").json()["name"] == "bike-test-stub"


def test_unit_and_batch_are_consistent_and_traced(client):
    unit = client.post("/predict", json={"hour": HOUR}).json()
    batch = client.post("/predict/batch", json={"hours": [HOUR, {**HOUR, "hr": 8}]}).json()
    assert batch["predictions"][0]["predicted_cnt"] == unit["prediction"]["predicted_cnt"]
    assert [p["item_index"] for p in batch["predictions"]] == [0, 1]
    assert count("SELECT count(*) FROM tracing.predictions WHERE request_id = %s", batch["request_id"]) == 2
    assert count("SELECT n_items FROM tracing.api_requests WHERE request_id = %s", batch["request_id"]) == 2


@pytest.mark.parametrize("payload", [{"hours": []}, {"hours": [HOUR] * 101},
                                     {"hours": [HOUR, {**HOUR, "hum": 1.5}]}, {"hours": [{**HOUR, "cnt": 3}]}])
def test_invalid_batch_is_rejected_without_partial_predictions(client, payload):
    response = client.post("/predict/batch", json=payload)
    assert response.status_code == 422 and response.json()["request_id"]
    rid = response.json()["request_id"]
    assert count("SELECT count(*) FROM tracing.predictions WHERE request_id = %s", rid) == 0
    assert count("SELECT http_status FROM tracing.api_requests WHERE request_id = %s", rid) == 422


def test_feedback_contract(client):
    prediction_id = client.post("/predict", json={"hour": HOUR}).json()["prediction"]["prediction_id"]
    body = {"feedback_id": str(uuid.uuid4()), "prediction_id": prediction_id, "observed_cnt": 180,
            "observed_at": "2012-07-18T17:00:00Z", "label_source": "test"}
    assert client.post("/feedback", json=body).status_code == 201
    assert client.post("/feedback", json=body).status_code == 200       # rejeu identique
    assert client.post("/feedback", json={**body, "observed_cnt": 1}).status_code == 409
    other = {**body, "feedback_id": str(uuid.uuid4())}
    assert client.post("/feedback", json=other).status_code == 409       # 2e feedback sur la même prédiction
    unknown = {**body, "feedback_id": str(uuid.uuid4()), "prediction_id": str(uuid.uuid4())}
    assert client.post("/feedback", json=unknown).status_code == 404
    incomplete = {"feedback_id": str(uuid.uuid4()), "prediction_id": prediction_id, "observed_cnt": 3}
    assert client.post("/feedback", json=incomplete).status_code == 422
    assert count("SELECT count(*) FROM tracing.feedbacks WHERE prediction_id = %s", prediction_id) == 1


def test_storage_failure_gives_503_and_no_false_success(client, monkeypatch):
    def boom(conn, rows):
        raise psycopg.OperationalError("disque plein simulé")
    monkeypatch.setattr(service, "insert_predictions", boom)
    response = client.post("/predict/batch", json={"hours": [HOUR, HOUR]})
    assert response.status_code == 503
    rid = response.json()["request_id"]
    assert count("SELECT count(*) FROM tracing.predictions WHERE request_id = %s", rid) == 0
    assert count("SELECT http_status FROM tracing.api_requests WHERE request_id = %s", rid) == 503


def test_pipeline_run_is_recorded_for_the_dashboard():
    from src import run_tracing
    context = {"run_name": f"test-{uuid.uuid4().hex[:8]}", "started_at": "2026-11-19T10:00:00Z",
               "failed_step": "ingestion", "trigger": "test"}
    run_tracing.record({}, context, "FAILED", error="DataValidationError: lot invalide")
    run_tracing.record({}, context, "FAILED", error="rejeu")  # même run : aucune seconde ligne
    assert count("SELECT count(*) FROM tracing.pipeline_runs WHERE run_name = %s AND failed_step = 'ingestion'",
                 context["run_name"]) == 1
