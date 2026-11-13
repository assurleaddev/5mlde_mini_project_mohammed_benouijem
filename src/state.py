"""État durable du déclenchement : quels lots ont déjà servi à un entraînement réussi."""
from __future__ import annotations

import json
from pathlib import Path

from src.utils import sha256_file, utc_now, write_json


def landing_fingerprint(landing_dir: Path) -> list[str]:
    return sorted(f"{p.name}:{sha256_file(p)[:16]}" for p in landing_dir.glob("batch_*.csv"))


def read_state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def mark_trained(path: Path, fingerprint: list[str], run_name: str) -> None:
    write_json(path, {"fingerprint": fingerprint, "run_name": run_name, "updated_at": utc_now()})
