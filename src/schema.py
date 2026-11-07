"""Contrat de données partagé par l'entraînement et l'inférence."""

ID_COLUMN = "instant"
DATE_COLUMN = "dteday"
TARGET = "cnt"

# Colonnes catégorielles encodées en one-hot (appris sur train uniquement).
CATEGORICAL_FEATURES = ["season", "mnth", "hr", "weekday", "weathersit"]
# Colonnes numériques ou binaires passées telles quelles.
NUMERIC_FEATURES = ["yr", "holiday", "workingday", "temp", "atemp", "hum", "windspeed"]
FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES

# casual + registered = cnt : les utiliser comme features serait une fuite de cible.
LEAKAGE_COLUMNS = ["casual", "registered"]

REQUIRED_COLUMNS = [ID_COLUMN, DATE_COLUMN, *FEATURES, TARGET]

DOMAINS = {
    "season": (1, 4),
    "mnth": (1, 12),
    "hr": (0, 23),
    "weekday": (0, 6),
    "weathersit": (1, 4),
    "yr": (0, 1),
    "holiday": (0, 1),
    "workingday": (0, 1),
    "temp": (0.0, 1.0),
    "atemp": (0.0, 1.0),
    "hum": (0.0, 1.0),
    "windspeed": (0.0, 1.0),
}


class DataValidationError(ValueError):
    """Données non conformes au contrat : incident bloquant, jamais retenté automatiquement."""
