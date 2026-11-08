"""Retries bornés sur panne transitoire, pas de retry sur erreur définitive, fallback entre sources."""
import hashlib

import httpx
import pytest

from src.acquisition import PermanentSourceError, TransientSourceError, ensure_dataset, fetch_source

PAYLOAD = b"instant,cnt\n1,2\n"
SHA = hashlib.sha256(PAYLOAD).hexdigest()


def client_for(responses):
    calls = []

    def handler(request):
        calls.append(str(request.url))
        status, body = responses[min(len(calls), len(responses)) - 1]
        return httpx.Response(status, content=body)
    return httpx.Client(transport=httpx.MockTransport(handler)), calls


def test_503_then_success_is_retried():
    client, calls = client_for([(503, b""), (200, PAYLOAD)])
    assert fetch_source({"name": "a", "url": "https://a/x.csv"}, SHA, client, backoff_seconds=0) == PAYLOAD
    assert len(calls) == 2


def test_retries_are_bounded():
    client, calls = client_for([(503, b"")])
    with pytest.raises(TransientSourceError):
        fetch_source({"name": "a", "url": "https://a/x.csv"}, SHA, client, max_attempts=3, backoff_seconds=0)
    assert len(calls) == 3


def test_404_and_bad_checksum_are_not_retried():
    client, calls = client_for([(404, b"")])
    with pytest.raises(PermanentSourceError):
        fetch_source({"name": "a", "url": "https://a/x.csv"}, SHA, client, backoff_seconds=0)
    client, calls2 = client_for([(200, b"autre contenu")])
    with pytest.raises(PermanentSourceError, match="Empreinte"):
        fetch_source({"name": "a", "url": "https://a/x.csv"}, SHA, client, backoff_seconds=0)
    assert len(calls) == 1 and len(calls2) == 1


def test_fallback_to_second_source(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(404 if "primary" in str(request.url) else 200, content=PAYLOAD)
    real_client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kw: real_client(transport=httpx.MockTransport(handler)))
    manifest = {"sha256": SHA, "sources": [{"name": "primary", "url": "https://primary/x.csv"},
                                           {"name": "mirror", "url": "https://mirror/x.csv"}]}
    result = ensure_dataset(manifest, tmp_path / "hour.csv", backoff_seconds=0)
    assert result["source"] == "mirror" and (tmp_path / "hour.csv").read_bytes() == PAYLOAD
