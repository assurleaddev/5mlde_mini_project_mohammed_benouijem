"""Acquisition de la source avec retries bornés, fallback entre sources et vérification d'empreinte."""
from __future__ import annotations

import io
import logging
import time
import zipfile
from pathlib import Path

import httpx

from src.utils import sha256_bytes, utc_now

logger = logging.getLogger(__name__)

TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504}


class TransientSourceError(RuntimeError):
    """Panne temporaire (timeout, 429, 5xx) : peut être retentée."""


class PermanentSourceError(RuntimeError):
    """Erreur définitive (404, contenu invalide, empreinte différente) : ne jamais retenter à l'aveugle."""


def _get(client: httpx.Client, url: str) -> bytes:
    try:
        response = client.get(url)
    except (httpx.TimeoutException, httpx.TransportError) as exc:
        raise TransientSourceError(f"{url} : {exc}") from exc
    if response.status_code in TRANSIENT_STATUS:
        retry_after = response.headers.get("Retry-After")
        raise TransientSourceError(f"{url} : HTTP {response.status_code}, Retry-After={retry_after}")
    if response.status_code != 200:
        raise PermanentSourceError(f"{url} : HTTP {response.status_code}")
    return response.content


def _extract(source: dict, payload: bytes) -> bytes:
    if source.get("format") == "zip":
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                return archive.read(source["member"])
        except (zipfile.BadZipFile, KeyError) as exc:
            raise PermanentSourceError(f"Archive invalide depuis {source['name']} : {exc}") from exc
    return payload


def fetch_source(source: dict, expected_sha256: str, client: httpx.Client,
                 max_attempts: int = 3, backoff_seconds: float = 2.0) -> bytes:
    """Télécharger une source : au plus `max_attempts` tentatives, uniquement sur panne transitoire."""
    for attempt in range(1, max_attempts + 1):
        try:
            content = _extract(source, _get(client, source["url"]))
        except TransientSourceError as exc:
            if attempt == max_attempts:
                raise
            delay = backoff_seconds * 2 ** (attempt - 1)
            logger.warning("Panne transitoire (%s), tentative %s/%s, attente %.1fs", exc, attempt, max_attempts, delay)
            time.sleep(delay)
            continue
        digest = sha256_bytes(content)
        if digest != expected_sha256:
            raise PermanentSourceError(f"Empreinte inattendue depuis {source['name']} : {digest}")
        return content
    raise AssertionError("unreachable")


def ensure_dataset(manifest: dict, target: Path, timeout_seconds: float = 30.0, **retry) -> dict:
    """Garantir la présence du CSV de référence : cache local, puis source principale, puis fallback."""
    if target.exists() and sha256_bytes(target.read_bytes()) == manifest["sha256"]:
        return {"source": "cache", "path": str(target), "sha256": manifest["sha256"]}
    errors = []
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
        for source in manifest["sources"]:
            try:
                content = fetch_source(source, manifest["sha256"], client, **retry)
            except (TransientSourceError, PermanentSourceError) as exc:
                logger.warning("Source %s indisponible : %s — passage à la suivante", source["name"], exc)
                errors.append(f"{source['name']}: {exc}")
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_suffix(".part")
            tmp.write_bytes(content)
            tmp.replace(target)
            return {"source": source["name"], "url": source["url"], "path": str(target),
                    "sha256": manifest["sha256"], "downloaded_at": utc_now()}
    raise TransientSourceError("Aucune source disponible : " + " | ".join(errors))
