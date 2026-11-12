"""Étape 2 : contrôler les valeurs, construire les features et séparer train/test dans le temps."""
from __future__ import annotations

import pandas as pd

from prefect import task
from prefect.cache_policies import NO_CACHE

from src import orchestration, tracking

from src.schema import (
    CATEGORICAL_FEATURES, DATE_COLUMN, DOMAINS, FEATURES, ID_COLUMN, NUMERIC_FEATURES, TARGET,
    DataValidationError,
)


def check_values(frame: pd.DataFrame) -> pd.DataFrame:
    """Contrôles défensifs : types stricts, domaines, doublons. Aucune valeur invalide n'est corrigée en silence."""
    frame = frame.copy()
    for column in [*FEATURES, TARGET, ID_COLUMN]:
        try:
            frame[column] = pd.to_numeric(frame[column], errors="raise")
        except (ValueError, TypeError) as exc:
            raise DataValidationError(f"Valeur non numérique dans '{column}' : {exc}") from exc
    try:
        frame[DATE_COLUMN] = pd.to_datetime(frame[DATE_COLUMN], format="%Y-%m-%d", errors="raise")
    except (ValueError, TypeError) as exc:
        raise DataValidationError(f"Date invalide dans '{DATE_COLUMN}' : {exc}") from exc
    if frame[[*FEATURES, TARGET]].isna().any().any():
        raise DataValidationError("Valeurs manquantes dans les features ou la cible")
    for column, (low, high) in DOMAINS.items():
        if not frame[column].between(low, high).all():
            raise DataValidationError(f"'{column}' hors du domaine [{low}, {high}]")
    if (frame[TARGET] < 0).any():
        raise DataValidationError("Cible négative")
    if frame.duplicated([DATE_COLUMN, "hr"]).any() or frame[ID_COLUMN].duplicated().any():
        raise DataValidationError("Doublons sur (dteday, hr) ou sur instant")
    return frame


def temporal_split(frame: pd.DataFrame, test_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Les dernières `test_days` journées servent de test : on évalue sur le futur, jamais sur le passé mélangé."""
    frame = frame.sort_values([DATE_COLUMN, "hr"]).reset_index(drop=True)
    cutoff = frame[DATE_COLUMN].max() - pd.Timedelta(days=test_days - 1)
    train = frame[frame[DATE_COLUMN] < cutoff]
    test = frame[frame[DATE_COLUMN] >= cutoff]
    if len(train) < 24 * 30 or len(test) < 24:
        raise DataValidationError(f"Effectifs insuffisants pour entraîner : train={len(train)}, test={len(test)}")
    return train, test


PREPROCESSING_VERSION = "1.0.0"  # à incrémenter à chaque changement de check_values / temporal_split


def _split_cache_key(context, parameters) -> str:
    """Clé explicite : contenu du snapshot (empreinte), paramètres et version du code de préparation."""
    return f"split-{parameters['snapshot_sha256']}-{parameters['test_days']}-{PREPROCESSING_VERSION}"


@task(name="validate-and-split", cache_key_fn=_split_cache_key, persist_result=True, retries=0)
def validate_and_split(frame: pd.DataFrame, snapshot_sha256: str, test_days: int):
    """Calcul pur et déterministe (aucune écriture MLflow) : réutilisable entre deux flows sur le même snapshot."""
    checked = check_values(frame)
    return temporal_split(checked, test_days)


@task(name="preprocessing", retries=0, timeout_seconds=300, cache_policy=NO_CACHE, persist_result=False)
def preprocess_data(data):
    """Valider, séparer train/test ; les transformations apprises sont ajustées sur train dans le pipeline."""
    with tracking.step(data, "preprocessing"):
        prepared = _prepare(data)
    run_id = data["context"].get("mlflow_run_id")
    if run_id:
        info = prepared["preprocessing"]
        tracking.log_params(data["config"], run_id, {
            "prep.features.categorical": ",".join(info["categorical"]),
            "prep.features.numeric": ",".join(info["numeric"]),
            "prep.excluded_columns": "instant,dteday,casual,registered",
            "prep.encoding": "OneHotEncoder(handle_unknown=ignore) fit on train",
            "prep.target_transform": "log1p / expm1",
            "split.strategy": "temporal", "split.test_days": data["config"]["split"]["test_days"],
            "split.train_period": "/".join(info["train_period"]), "split.test_period": "/".join(info["test_period"]),
        })
        tracking.log_metrics(data["config"], run_id, {"split.train_rows": info["train_rows"],
                                                      "split.test_rows": info["test_rows"]})
    return prepared


def _prepare(data):
    config = data["config"]
    test_days = int(config["split"]["test_days"])
    sha = data["context"].get("snapshot_sha256")
    if sha and orchestration.flow_run_id():
        train, test = validate_and_split(data["snapshot"], sha, test_days)
    else:
        train, test = temporal_split(check_values(data["snapshot"]), test_days)
    orchestration.task_logger().info("Split temporel : train=%s, test=%s lignes", len(train), len(test))
    split = pd.concat([
        pd.DataFrame({ID_COLUMN: train[ID_COLUMN], "split": "train"}),
        pd.DataFrame({ID_COLUMN: test[ID_COLUMN], "split": "test"}),
    ])
    return {
        **data,
        "X_train": train[FEATURES], "y_train": train[TARGET],
        "X_test": test[FEATURES], "y_test": test[TARGET],
        "test_frame": test,
        "split": split,
        "preprocessing": {
            "categorical": CATEGORICAL_FEATURES, "numeric": NUMERIC_FEATURES,
            "train_rows": len(train), "test_rows": len(test),
            "train_period": [str(train[DATE_COLUMN].min().date()), str(train[DATE_COLUMN].max().date())],
            "test_period": [str(test[DATE_COLUMN].min().date()), str(test[DATE_COLUMN].max().date())],
        },
    }
