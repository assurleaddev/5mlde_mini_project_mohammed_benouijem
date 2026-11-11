"""Politique de qualification versionnée : fonction pure, testable, qui ne réentraîne rien."""
from __future__ import annotations

import json
import math
from pathlib import Path

POLICY_PATH = Path(__file__).resolve().parents[1] / "config" / "qualification_policy.json"

PRODUCTION_READY, REJECTED, PENDING = "production-ready", "rejected", "pending"


def load_policy(path: Path | None = None) -> dict:
    return json.loads(Path(path or POLICY_PATH).read_text(encoding="utf-8"))


def _finite(*values) -> bool:
    return all(isinstance(v, (int, float)) and math.isfinite(v) for v in values)


def decide(evidence: dict, policy: dict) -> dict:
    """evidence : métriques du modèle et de la baseline, segments, effectifs, rechargement, origine des données."""
    t = policy["thresholds"]
    model, baseline = evidence.get("model", {}), evidence.get("baseline", {})
    checks = []

    def check(name, ok, observed, threshold):
        checks.append({"check": name, "passed": bool(ok), "observed": observed, "threshold": threshold})

    complete = _finite(model.get("mae"), model.get("r2"), baseline.get("mae"))
    check("metrics_complete_and_finite", complete, {"model": model, "baseline": baseline}, "finite")
    if not complete:
        return {"decision": PENDING, "reason": "Métriques manquantes ou non finies", "checks": checks,
                "policy_id": policy["policy_id"], "policy_version": policy["version"]}

    check("data_origin_allowed", evidence.get("data_origin") in policy["allowed_data_origins"],
          evidence.get("data_origin"), policy["allowed_data_origins"])
    check("enough_test_rows", evidence.get("test_rows", 0) >= t["min_test_rows"], evidence.get("test_rows"),
          t["min_test_rows"])
    check("reload_identical", evidence.get("reload_ok") is True, evidence.get("reload_ok"), True)
    check("mae_below_business_threshold", model["mae"] <= t["max_mae"], model["mae"], t["max_mae"])
    check("r2_above_threshold", model["r2"] >= t["min_r2"], model["r2"], t["min_r2"])
    ratio = model["mae"] / baseline["mae"] if baseline["mae"] else math.inf
    check("beats_baseline", ratio <= t["max_mae_ratio_vs_baseline"], round(ratio, 4), t["max_mae_ratio_vs_baseline"])
    worst = {k: v for k, v in evidence.get("segments", {}).items() if v["n"] >= t["min_segment_rows"]}
    worst_ratio = max((v["mae"] / model["mae"] for v in worst.values()), default=0.0)
    check("no_segment_collapse", worst_ratio <= t["max_segment_mae_ratio"], round(worst_ratio, 4),
          t["max_segment_mae_ratio"])

    failed = [c["check"] for c in checks if not c["passed"]]
    return {"decision": REJECTED if failed else PRODUCTION_READY,
            "reason": ("Contrôles en échec : " + ", ".join(failed)) if failed else "Tous les contrôles sont satisfaits",
            "checks": checks, "policy_id": policy["policy_id"], "policy_version": policy["version"]}


def should_replace_champion(candidate_mae: float, champion_mae_same_test: float | None, policy: dict) -> tuple[bool, str]:
    """Une qualification absolue ne prouve pas que le candidat bat le modèle en service : on compare sur le même test."""
    if champion_mae_same_test is None:
        return True, "Aucun champion en service"
    limit = champion_mae_same_test * (1 + policy["promotion"]["max_relative_degradation_vs_champion"])
    if candidate_mae <= limit:
        return True, f"MAE candidat {candidate_mae:.2f} <= {limit:.2f} (champion {champion_mae_same_test:.2f} sur le même test)"
    return False, f"MAE candidat {candidate_mae:.2f} > {limit:.2f} : champion conservé"
