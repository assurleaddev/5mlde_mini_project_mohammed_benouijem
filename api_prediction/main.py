"""API FastAPI d'inférence de la demande de vélos.

    uv run --locked uvicorn api_prediction.main:app --host 127.0.0.1 --port 8001 --workers 2
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pandas as pd
import psycopg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import errors
from psycopg_pool import ConnectionPool

from api_prediction import service
from api_prediction.schemas import (BatchRequest, BatchResponse, FeedbackRequest, FeedbackResponse, ModelIdentity,
                                    PredictionOut, PredictRequest, PredictResponse)
from api_prediction.settings import Settings
from src.schema import FEATURES

logger = logging.getLogger("api_prediction")
TRACED = {"/predict", "/predict/batch", "/feedback"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class State:
    settings: Settings
    loaded: service.LoadedModel | None = None
    pool: ConnectionPool | None = None
    instance_id = uuid.uuid4()
    startup_error: str | None = None


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Modèle et pool chargés une fois par worker ; un échec empêche le worker de se déclarer prêt."""
    state.settings = Settings()
    state.instance_id = uuid.uuid4()
    # Parallélisme interne borné : plusieurs workers × modèles multi-thread se concurrenceraient.
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    state.pool = ConnectionPool(state.settings.conninfo, min_size=state.settings.pool_min,
                                max_size=state.settings.pool_max, open=False, timeout=5)
    state.pool.open(wait=False)
    try:
        state.loaded = service.load_model(state.settings)
        state.loaded.model_id = service.register_model_version(state.pool, state.loaded)
        logger.info("Worker %s prêt : %s v%s", os.getpid(), state.loaded.name, state.loaded.version)
    except Exception as exc:
        state.startup_error = f"{type(exc).__name__}: {exc}"
        logger.error("Worker non prêt : %s", state.startup_error)
    yield
    state.pool.close()


app = FastAPI(title="Bike demand prediction API", version="1.0.0", lifespan=lifespan,
              description="Prévision du nombre de locations horaires (UCI Bike Sharing). "
                          "Chaque prédiction est tracée avec la version exacte du modèle.")


# ---------------------------------------------------------------------------- traçabilité
def _trace(request: Request, status: int, error_code: str | None, n_items: int | None,
           inference_ms: float | None, conn=None) -> dict:
    trace = {
        "request_id": request.state.request_id, "started_at": request.state.started_at,
        "endpoint": request.url.path, "http_status": status, "error_code": error_code, "n_items": n_items,
        "processing_duration_ms": (time.perf_counter() - request.state.t0) * 1000,
        "inference_duration_ms": inference_ms, "worker_pid": os.getpid(), "instance_id": state.instance_id,
        "api_revision": state.settings.api_revision,
        "campaign_id": (request.headers.get("X-Test-Campaign") or "manual")[:64],
    }
    request.state.traced = True
    if conn is not None:
        service.insert_request(conn, trace)
    return trace


def _trace_error(request: Request, status: int, code: str) -> None:
    """Point de journalisation commun des erreurs, dans une transaction séparée."""
    if getattr(request.state, "traced", False) or request.url.path not in TRACED:
        return
    trace = _trace(request, status, code, None, None)
    try:
        with state.pool.connection(timeout=2) as conn:
            service.insert_request(conn, trace)
    except Exception as exc:
        service.local_error_log(state.settings, {**trace, "trace_error": str(exc)})


@app.middleware("http")
async def request_context(request: Request, call_next):
    request.state.request_id = uuid.uuid4()
    request.state.started_at = datetime.now(timezone.utc)
    request.state.t0 = time.perf_counter()
    request.state.traced = False
    try:
        response = await call_next(request)
    except Exception:  # filet de sécurité : jamais de trace Python renvoyée au client
        logger.exception("Erreur inattendue %s", request.state.request_id)
        _trace_error(request, 500, "internal_error")
        response = JSONResponse(status_code=500, content={"request_id": str(request.state.request_id),
                                                          "error": {"code": "internal_error", "message": "Erreur interne"}})
    response.headers["X-Request-ID"] = str(request.state.request_id)
    return response


def _error_response(request: Request, status: int, code: str, message) -> JSONResponse:
    _trace_error(request, status, code)
    return JSONResponse(status_code=status, content={"request_id": str(request.state.request_id),
                                                     "error": {"code": code, "message": message}})


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    details = [{"loc": e.get("loc"), "msg": e.get("msg")} for e in exc.errors()]
    return _error_response(request, 422, "invalid_input", details)


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    return _error_response(request, exc.status, exc.code, exc.message)


# ---------------------------------------------------------------------------- prédiction
def _require_ready():
    if state.loaded is None:
        raise ApiError(503, "model_unavailable", "Modèle non chargé")


def _predict(request: Request, hours) -> tuple[list[PredictionOut], float]:
    _require_ready()
    loaded = state.loaded
    frame = pd.DataFrame([h.model_dump(exclude={"ref"}) for h in hours])[FEATURES]
    started = time.perf_counter()
    values = loaded.model.predict(frame).clip(min=0)
    inference_ms = (time.perf_counter() - started) * 1000
    identity = ModelIdentity(name=loaded.name, version=loaded.version, run_id=loaded.run_id)
    outputs = [PredictionOut(prediction_id=uuid.uuid4(), item_index=i, ref=h.ref, predicted_cnt=round(float(v), 3),
                             model=identity) for i, (h, v) in enumerate(zip(hours, values))]
    if len(outputs) != len(hours):  # invariant : un résultat par heure demandée
        raise ApiError(500, "internal_error", "Invariant de prédiction violé")
    rows = [(o.prediction_id, request.state.request_id, o.item_index, loaded.model_id,
             h.model_dump(exclude={"ref"}), o.predicted_cnt) for o, h in zip(outputs, hours)]
    try:  # une transaction : la requête et toutes ses prédictions, ou rien ; réponse après commit
        with state.pool.connection(timeout=3) as conn, conn.transaction():
            _trace(request, 200, None, len(hours), inference_ms, conn)
            service.insert_predictions(conn, rows)
    except Exception as exc:
        request.state.traced = False
        logger.error("Écriture de la trace impossible : %s", exc)
        raise ApiError(503, "storage_unavailable", "Traçabilité indisponible : prédiction non servie") from exc
    return outputs, inference_ms


@app.post("/predict", response_model=PredictResponse, tags=["prediction"])
def predict(body: PredictRequest, request: Request):
    outputs, _ = _predict(request, [body.hour])
    return PredictResponse(request_id=request.state.request_id, prediction=outputs[0])


@app.post("/predict/batch", response_model=BatchResponse, tags=["prediction"])
def predict_batch(body: BatchRequest, request: Request):
    """Tout ou rien : une entrée invalide rejette le lot entier avant tout calcul (422)."""
    outputs, _ = _predict(request, body.hours)
    return BatchResponse(request_id=request.state.request_id, predictions=outputs)


# ---------------------------------------------------------------------------- feedback
class _Replay(Exception):
    """Même feedback rejoué : aucune nouvelle ligne de feedback, réponse 200."""


@app.post("/feedback", response_model=FeedbackResponse, status_code=201, tags=["feedback"],
          responses={200: {"model": FeedbackResponse}, 404: {}, 409: {}})
def feedback(body: FeedbackRequest, request: Request):
    """Un feedback immuable par prédiction ; les conflits concurrents sont arbitrés par les contraintes SQL."""
    payload = (body.useful, body.observed_cnt, body.observed_at, body.label_source)
    try:
        with state.pool.connection(timeout=3) as conn, conn.transaction():
            _trace(request, 201, None, 1, None, conn)
            try:
                with conn.transaction():  # savepoint
                    conn.execute(
                        """INSERT INTO tracing.feedbacks (feedback_id, prediction_id, request_id, useful,
                               observed_cnt, observed_at, label_source) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (body.feedback_id, body.prediction_id, request.state.request_id, *payload))
            except errors.ForeignKeyViolation:
                raise ApiError(404, "prediction_not_found", "prediction_id inconnu")
            except errors.UniqueViolation:
                existing = conn.execute(
                    """SELECT prediction_id, useful, observed_cnt, observed_at, label_source
                       FROM tracing.feedbacks WHERE feedback_id = %s""", (body.feedback_id,)).fetchone()
                if existing and existing[0] == body.prediction_id and tuple(existing[1:]) == payload:
                    raise _Replay()
                raise ApiError(409, "feedback_conflict",
                               "feedback_id réutilisé avec un autre contenu, ou prédiction déjà commentée")
    except _Replay:  # la transaction (trace 201) est annulée ; on trace la tentative en 200
        request.state.traced = False
        _trace_error(request, 200, None)
        return JSONResponse(status_code=200, content={"feedback_id": str(body.feedback_id),
                                                      "status": "already_recorded"})
    except ApiError:
        request.state.traced = False  # la trace de la transaction annulée est réécrite par le handler
        raise
    except psycopg.Error as exc:
        request.state.traced = False
        raise ApiError(503, "storage_unavailable", "Stockage indisponible") from exc
    return FeedbackResponse(feedback_id=body.feedback_id, status="created")


# ---------------------------------------------------------------------------- santé et identité
@app.get("/health/live", tags=["health"])
def live():
    return {"status": "alive", "pid": os.getpid()}


@app.get("/health/ready", tags=["health"])
def ready():
    problems = []
    if state.loaded is None:
        problems.append(state.startup_error or "modèle non chargé")
    try:
        with state.pool.connection(timeout=2) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        problems.append(f"postgres: {type(exc).__name__}")
    if problems:
        return JSONResponse(status_code=503, content={"status": "not_ready", "problems": problems})
    return {"status": "ready", "model": f"{state.loaded.name}@{state.loaded.version}", "instance_id": str(state.instance_id)}


@app.get("/model", tags=["health"])
def model_info():
    _require_ready()
    loaded = state.loaded
    return {"name": loaded.name, "version": loaded.version, "run_id": loaded.run_id, "model_uri": loaded.model_uri,
            "qualification": loaded.qualification, "instance_id": str(state.instance_id), "pid": os.getpid()}
