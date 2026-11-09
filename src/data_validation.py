"""Barrière de qualité Great Expectations (GX Core 1.x) appelée à la fin de load_data.

Deux contrats :
- `bike_batch` : règles strictes sur chaque nouveau lot reçu (avant fusion dans le corpus) ;
- `bike_snapshot` : socle vérifié sur le snapshot complet qui sera entraîné.
Toutes les expectations sont bloquantes (pas de `mostly` < 1).
"""
from __future__ import annotations

import logging
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

from src.schema import DATE_COLUMN, DOMAINS, ID_COLUMN, REQUIRED_COLUMNS, TARGET, DataValidationError
from src.utils import utc_now, write_json

logger = logging.getLogger(__name__)

SUITE_VERSION = "1.0.0"
CATEGORICAL_DOMAINS = {"season": [1, 2, 3, 4], "weathersit": [1, 2, 3, 4], "holiday": [0, 1],
                       "workingday": [0, 1], "yr": [0, 1]}
INTEGER_TYPES = ["int64", "int32", "int16", "int8", "Int64"]
NUMERIC_TYPES = INTEGER_TYPES + ["float64", "float32", "Float64"]


def _common_expectations(min_rows: int, max_rows: int | None) -> list:
    expectations = [gxe.ExpectColumnToExist(column=c) for c in REQUIRED_COLUMNS]
    expectations += [gxe.ExpectColumnValuesToNotBeNull(column=c) for c in REQUIRED_COLUMNS]
    expectations += [
        gxe.ExpectTableRowCountToBeBetween(min_value=min_rows, max_value=max_rows),
        gxe.ExpectColumnValuesToBeUnique(column=ID_COLUMN),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=[DATE_COLUMN, "hr"]),
        gxe.ExpectColumnValuesToMatchStrftimeFormat(column=DATE_COLUMN, strftime_format="%Y-%m-%d"),
        gxe.ExpectColumnValuesToBeOfType(column=ID_COLUMN, type_="int64"),
        gxe.ExpectColumnValuesToBeInTypeList(column=TARGET, type_list=INTEGER_TYPES),
        gxe.ExpectColumnValuesToBeBetween(column=TARGET, min_value=0, max_value=5000),
    ]
    for column, (low, high) in DOMAINS.items():
        expectations.append(gxe.ExpectColumnValuesToBeInTypeList(column=column, type_list=NUMERIC_TYPES))
        expectations.append(gxe.ExpectColumnValuesToBeBetween(column=column, min_value=low, max_value=high))
    for column, values in CATEGORICAL_DOMAINS.items():
        expectations.append(gxe.ExpectColumnValuesToBeInSet(column=column, value_set=values))
    return expectations


def batch_suite() -> gx.ExpectationSuite:
    """Nouveau lot mensuel : entre 20 jours et 31 jours d'heures, cohérence cnt = casual + registered."""
    suite = gx.ExpectationSuite(name="bike_batch", meta={"version": SUITE_VERSION})
    for expectation in _common_expectations(min_rows=24 * 20, max_rows=24 * 31):
        suite.add_expectation(expectation)
    for column in ("casual", "registered"):
        suite.add_expectation(gxe.ExpectColumnToExist(column=column))
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column=column, min_value=0))
    return suite


def snapshot_suite(min_rows: int) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="bike_snapshot", meta={"version": SUITE_VERSION})
    for expectation in _common_expectations(min_rows=min_rows, max_rows=None):
        suite.add_expectation(expectation)
    return suite


def _run_suite(context, frame: pd.DataFrame, suite: gx.ExpectationSuite):
    suite = context.suites.add(suite)
    source = context.data_sources.add_pandas(name="memory")
    asset = source.add_dataframe_asset(name=f"{suite.name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{suite.name}_batch")
    validation = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{suite.name}_validation", data=batch_definition, suite=suite))
    return validation.run(batch_parameters={"dataframe": frame})


def _summarise(result, frame: pd.DataFrame) -> list[dict]:
    failures = []
    for item in result.results:
        if item.success:
            continue
        config = item.expectation_config
        details = item.result or {}
        failures.append({
            "expectation": config.type,
            "kwargs": {k: v for k, v in config.kwargs.items() if k != "batch_id"},
            "unexpected_count": details.get("unexpected_count"),
            "partial_unexpected_list": [str(v) for v in (details.get("partial_unexpected_list") or [])[:5]],
            "exception": (item.exception_info or {}).get("exception_message")
            if isinstance(item.exception_info, dict) else None,
        })
    return failures


def _audit(frame: pd.DataFrame, kind: str) -> list[dict]:
    """Contrôles métier complémentaires qu'une expectation de colonne ne couvre pas."""
    issues = []
    if kind == "batch" and {"casual", "registered", TARGET} <= set(frame.columns):
        try:
            mismatch = frame[pd.to_numeric(frame["casual"]) + pd.to_numeric(frame["registered"])
                             != pd.to_numeric(frame[TARGET])]
        except (ValueError, TypeError):
            mismatch = frame
        if len(mismatch):
            issues.append({"expectation": "audit.cnt_equals_casual_plus_registered",
                           "unexpected_count": int(len(mismatch)),
                           "partial_unexpected_list": [str(v) for v in mismatch[ID_COLUMN].head(5)]})
    return issues


def validate(frame: pd.DataFrame, kind: str, source: str, snapshot_sha256: str | None,
             report_dir: Path, min_rows: int = 24 * 60) -> dict:
    """Exécuter la suite, écrire le rapport et lever une exception si un seul contrôle échoue."""
    context = gx.get_context(mode="ephemeral")  # contexte requis avant de construire les suites
    try:
        from great_expectations.data_context.types.base import ProgressBarsConfig
        context.variables.progress_bars = ProgressBarsConfig(globally=False)
    except Exception:  # option purement cosmétique
        pass
    suite = batch_suite() if kind == "batch" else snapshot_suite(min_rows)
    report = {"date": utc_now(), "kind": kind, "source": source, "snapshot_sha256": snapshot_sha256,
              "suite": suite.name, "suite_version": SUITE_VERSION, "rows": int(len(frame))}
    try:
        result = _run_suite(context, frame, suite)
        failures = _summarise(result, frame) + _audit(frame, kind)
        report.update(success=bool(result.success) and not failures, failures=failures,
                      evaluated=len(result.results))
    except Exception as exc:  # une erreur GX ne vaut jamais autorisation d'entraîner
        report.update(success=False, failures=[], error=f"{type(exc).__name__}: {exc}")
    name = f"{suite.name}-{Path(source).stem}-{'ok' if report['success'] else 'rejected'}.json"
    write_json(report_dir / name, report)
    report["report_path"] = str(report_dir / name)
    if not report["success"]:
        reasons = report.get("error") or ", ".join(
            f"{f['expectation']}({f.get('kwargs', {}).get('column', '')})" for f in report["failures"][:5])
        raise DataValidationError(f"Validation '{suite.name}' en échec pour {source} : {reasons} "
                                  f"— rapport : {report['report_path']}")
    logger.info("Validation %s réussie (%s contrôles) pour %s", suite.name, report["evaluated"], source)
    return report
