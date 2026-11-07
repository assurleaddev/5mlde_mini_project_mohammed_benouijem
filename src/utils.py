"""Helpers techniques : chemins, empreintes, écriture atomique des runs."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve(base_dir: Path, value: str | os.PathLike) -> Path:
    """Résoudre un chemin depuis le dossier du fichier de configuration, pas depuis le dossier courant."""
    path = Path(value)
    return path if path.is_absolute() else (base_dir / path).resolve()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def publish_dir(tmp_dir: Path, final_dir: Path) -> None:
    """Publier un run complet en une opération : rien n'apparaît dans final_dir en cas d'échec."""
    if final_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise FileExistsError(f"Le dossier de run existe déjà, refus de l'écraser : {final_dir}")
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp_dir, final_dir)
