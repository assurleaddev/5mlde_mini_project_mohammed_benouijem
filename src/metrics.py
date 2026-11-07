"""Calcul des métriques : globales, baseline et par segment."""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def regression_metrics(y_true, y_pred) -> dict:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "r2": float(r2_score(y_true, y_pred)),
    }


def segment_mae(frame: pd.DataFrame, y_true, y_pred, column: str) -> dict:
    errors = pd.Series(np.abs(np.asarray(y_true) - np.asarray(y_pred)), index=frame.index)
    grouped = errors.groupby(frame[column])
    return {f"{column}={key}": {"mae": float(err.mean()), "n": int(err.size)} for key, err in grouped}
