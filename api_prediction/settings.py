"""Configuration du service lue dans l'environnement (le .env du Compose n'est pas lu automatiquement)."""
import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    db_host: str = field(default_factory=lambda: os.environ.get("API_DB_HOST", "127.0.0.1"))
    db_port: int = field(default_factory=lambda: int(os.environ.get("API_DB_PORT", "5432")))
    db_name: str = field(default_factory=lambda: os.environ.get("API_DB_NAME", "bike_api"))
    db_user: str = field(default_factory=lambda: os.environ.get("API_DB_USER", "api_writer"))
    db_password: str = field(default_factory=lambda: os.environ.get("API_DB_PASSWORD", "5mlde-local-api"))
    pool_min: int = field(default_factory=lambda: int(os.environ.get("API_POOL_MIN", "1")))
    pool_max: int = field(default_factory=lambda: int(os.environ.get("API_POOL_MAX", "5")))
    manifest_path: Path = field(default_factory=lambda: Path(
        os.environ.get("SERVING_MANIFEST", ROOT / "config" / "serving_manifest.json")))
    api_revision: str = field(default_factory=lambda: os.environ.get("API_REVISION", "dev"))
    allow_unqualified: bool = field(default_factory=lambda: os.environ.get("ALLOW_UNQUALIFIED_MODEL") == "1")
    error_log: Path = field(default_factory=lambda: Path(os.environ.get("API_ERROR_LOG", ROOT / "artifacts" / "api_errors.jsonl")))

    @property
    def conninfo(self) -> str:
        return (f"host={self.db_host} port={self.db_port} dbname={self.db_name} user={self.db_user} "
                f"password={self.db_password} connect_timeout=3")
