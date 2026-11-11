"""Étape 3 : ajuster le pipeline complet (encodage + modèle) et la baseline sur train uniquement."""
from __future__ import annotations

import time

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from src import tracking
from src.schema import CATEGORICAL_FEATURES, NUMERIC_FEATURES


def build_pipeline(model_params: dict, seed: int) -> Pipeline:
    """Pipeline sérialisable : les catégories inconnues à l'inférence sont ignorées, pas une erreur."""
    preprocessor = ColumnTransformer(
        [
            ("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
            ("numeric", "passthrough", NUMERIC_FEATURES),
        ],
        verbose_feature_names_out=False,
    )
    regressor = TransformedTargetRegressor(
        regressor=HistGradientBoostingRegressor(random_state=seed, **model_params),
        func=np.log1p, inverse_func=np.expm1,  # la demande est asymétrique : on apprend log(1 + cnt)
    )
    return Pipeline([("preprocessor", preprocessor), ("model", regressor)])


def fit_baseline(X_train: pd.DataFrame, y_train: pd.Series) -> dict:
    """Baseline métier : moyenne historique par (heure, jour ouvré)."""
    profile = y_train.groupby([X_train["hr"], X_train["workingday"]]).mean()
    return {"profile": {f"{h}-{w}": float(v) for (h, w), v in profile.items()}, "fallback": float(y_train.mean())}


def predict_baseline(baseline: dict, X: pd.DataFrame) -> np.ndarray:
    keys = X["hr"].astype(int).astype(str) + "-" + X["workingday"].astype(int).astype(str)
    return keys.map(baseline["profile"]).fillna(baseline["fallback"]).to_numpy()


def train_models(prepared_data):
    """Entraîner le modèle et la baseline ; transmettre modèle, données de test et configuration."""
    config = prepared_data["config"]
    seed = int(config["seed"])
    with tracking.step(prepared_data, "training"):
        pipeline = build_pipeline(config["model"], seed)
        started = time.perf_counter()
        pipeline.fit(prepared_data["X_train"], prepared_data["y_train"])
        duration = time.perf_counter() - started
        baseline = fit_baseline(prepared_data["X_train"], prepared_data["y_train"])
    run_id = prepared_data["context"].get("mlflow_run_id")
    if run_id:
        tracking.log_params(config, run_id, {"seed": seed, "model.algorithm": "HistGradientBoostingRegressor",
                                             **{f"model.{k}": v for k, v in config["model"].items()},
                                             "baseline.algorithm": "mean cnt by (hr, workingday)"})
        tracking.log_metrics(config, run_id, {"training.fit_seconds": duration})
    return {**prepared_data, "pipeline": pipeline, "baseline": baseline, "training_seconds": duration}
