"""La politique de qualification est une fonction pure : cas acceptés et refusés sur résultats contrôlés."""
import math

import pytest

from src.qualification import PENDING, PRODUCTION_READY, REJECTED, decide, load_policy, should_replace_champion

POLICY = load_policy()


def evidence(**overrides):
    base = {
        "model": {"mae": 40.0, "rmse": 60.0, "r2": 0.90},
        "baseline": {"mae": 100.0, "rmse": 140.0, "r2": 0.5},
        "segments": {"weathersit=1": {"mae": 35.0, "n": 400}, "weathersit=3": {"mae": 80.0, "n": 40}},
        "test_rows": 672, "reload_ok": True, "data_origin": "uci-replay",
    }
    base.update(overrides)
    return base


def test_accepted_candidate():
    assert decide(evidence(), POLICY)["decision"] == PRODUCTION_READY


@pytest.mark.parametrize("overrides, failed_check", [
    ({"model": {"mae": 75.0, "rmse": 90.0, "r2": 0.85}}, "mae_below_business_threshold"),
    ({"baseline": {"mae": 55.0, "rmse": 80.0, "r2": 0.7}}, "beats_baseline"),
    ({"segments": {"weathersit=3": {"mae": 110.0, "n": 40}}}, "no_segment_collapse"),
    ({"reload_ok": False}, "reload_identical"),
    ({"data_origin": "synthetic"}, "data_origin_allowed"),
    ({"test_rows": 100}, "enough_test_rows"),
])
def test_rejected_candidates(overrides, failed_check):
    result = decide(evidence(**overrides), POLICY)
    assert result["decision"] == REJECTED and failed_check in result["reason"]


def test_small_segment_is_ignored():
    result = decide(evidence(segments={"weathersit=4": {"mae": 500.0, "n": 3}}), POLICY)
    assert result["decision"] == PRODUCTION_READY


def test_missing_or_non_finite_metric_stays_pending():
    assert decide(evidence(model={"mae": math.nan, "r2": 0.9}), POLICY)["decision"] == PENDING
    assert decide(evidence(model={}), POLICY)["decision"] == PENDING


def test_champion_replacement_compares_on_same_test():
    assert should_replace_champion(40.0, None, POLICY)[0]
    assert should_replace_champion(40.0, 41.0, POLICY)[0]
    assert should_replace_champion(40.5, 40.0, POLICY)[0]       # dégradation < 2 % tolérée
    assert not should_replace_champion(45.0, 40.0, POLICY)[0]
