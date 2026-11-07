"""Étape 2 : contrôler les valeurs, construire les features et séparer train/test dans le temps."""
from __future__ import annotations

import pandas as pd

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


def preprocess_data(data):
    """Valider, séparer train/test ; les transformations apprises sont ajustées sur train dans le pipeline."""
    config = data["config"]
    frame = check_values(data["snapshot"])
    train, test = temporal_split(frame, int(config["split"]["test_days"]))
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
