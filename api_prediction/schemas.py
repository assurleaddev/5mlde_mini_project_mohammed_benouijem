"""Contrats HTTP : mêmes features que l'entraînement, cible jamais demandée à la prédiction."""
from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

EXAMPLE = {"season": 3, "yr": 1, "mnth": 7, "hr": 17, "holiday": 0, "weekday": 3, "workingday": 1,
           "weathersit": 1, "temp": 0.74, "atemp": 0.68, "hum": 0.45, "windspeed": 0.19}


class HourFeatures(BaseModel):
    """Conditions d'une heure donnée ; `ref` est un identifiant client facultatif, jamais une feature."""
    model_config = ConfigDict(extra="forbid", json_schema_extra={"example": EXAMPLE})

    ref: str | None = Field(default=None, max_length=64, description="Identifiant de traçabilité côté client")
    season: int = Field(ge=1, le=4)
    yr: int = Field(ge=0, le=1)
    mnth: int = Field(ge=1, le=12)
    hr: int = Field(ge=0, le=23)
    holiday: Literal[0, 1]
    weekday: int = Field(ge=0, le=6)
    workingday: Literal[0, 1]
    weathersit: int = Field(ge=1, le=4)
    temp: float = Field(ge=0, le=1)
    atemp: float = Field(ge=0, le=1)
    hum: float = Field(ge=0, le=1)
    windspeed: float = Field(ge=0, le=1)


class PredictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"example": {"hour": EXAMPLE}})
    hour: HourFeatures


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"example": {"hours": [EXAMPLE, EXAMPLE]}})
    hours: list[HourFeatures] = Field(min_length=1, max_length=100)


class ModelIdentity(BaseModel):
    name: str
    version: str
    run_id: str


class PredictionOut(BaseModel):
    prediction_id: UUID
    item_index: int
    ref: str | None
    predicted_cnt: float
    model: ModelIdentity


class PredictResponse(BaseModel):
    request_id: UUID
    prediction: PredictionOut


class BatchResponse(BaseModel):
    request_id: UUID
    predictions: list[PredictionOut]


class FeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"example": {
        "feedback_id": "7d7c2a0e-0000-4000-8000-000000000001", "prediction_id": "<uuid reçu de /predict>",
        "useful": True, "observed_cnt": 412, "observed_at": "2012-07-18T17:00:00Z", "label_source": "station-counter"}})

    feedback_id: UUID
    prediction_id: UUID
    useful: bool | None = None
    observed_cnt: int | None = Field(default=None, ge=0, le=5000)
    observed_at: datetime | None = None
    label_source: str | None = Field(default=None, min_length=1, max_length=64)

    @model_validator(mode="after")
    def coherent(self):
        if self.useful is None and self.observed_cnt is None:
            raise ValueError("Un avis (useful) et/ou une observation (observed_cnt) est requis")
        observation = [self.observed_cnt, self.observed_at, self.label_source]
        if any(v is not None for v in observation) and not all(v is not None for v in observation):
            raise ValueError("observed_cnt, observed_at et label_source vont ensemble")
        if self.observed_at is not None and self.observed_at.tzinfo is None:
            raise ValueError("observed_at doit porter un fuseau horaire")
        return self


class FeedbackResponse(BaseModel):
    feedback_id: UUID
    status: Literal["created", "already_recorded"]
