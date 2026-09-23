"""Local caching of remotely-fetched manifests."""

import json
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel

DEFAULT_CACHE_DIRECTORY = Path("target") / ".dbt_loom"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ManifestCache:
    """
    A local cache for a single manifest reference, keyed by a hash of its
    reference config.
    """

    def __init__(self, config: BaseModel, directory: Optional[Path] = None) -> None:
        # add type name to key to avoid collisions
        key = f"{type(config).__name__}{config.model_dump_json()}"
        digest = sha256(key.encode()).hexdigest()[:12]
        self.directory = (directory or DEFAULT_CACHE_DIRECTORY) / digest
        self.manifest_path = self.directory / "manifest.json"
        self.lock_path = self.directory / "manifest.json.lock"

    def _read_json(self, path: Path) -> Optional[Dict]:
        """Read a JSON file, or None if it is missing or unreadable."""

        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    def read(self, token: str) -> Optional[Dict]:
        """Return the cached manifest if it matches the validation token."""

        lock = self._read_json(self.lock_path)
        if lock is None or lock.get("token") != token:
            return None

        return self._read_json(self.manifest_path)

    def read_if_fresh(self, ttl: int) -> Optional[Dict]:
        """
        Return the cached manifest if it was written less than `ttl` seconds
        ago, otherwise None. This avoids contacting the remote source at all.
        """

        lock = self._read_json(self.lock_path) if ttl > 0 else None
        if lock is None:
            return None

        try:
            cached_at = datetime.fromisoformat(lock["cached_at"])
        except (KeyError, TypeError, ValueError):
            return None

        if _now() - cached_at > timedelta(seconds=ttl):
            return None

        return self._read_json(self.manifest_path)

    def write(self, manifest: Dict, token: str) -> None:
        """Store a manifest and its cache-validation token."""

        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            self.manifest_path.write_text(json.dumps(manifest))
        except OSError:
            # A cache write failure must never break manifest loading.
            return

        self.touch(token)

    def touch(self, token: str) -> None:
        """
        Restart the TTL window for an already-cached manifest, without
        rewriting the manifest itself.
        """

        try:
            self.lock_path.write_text(
                json.dumps({"token": token, "cached_at": _now().isoformat()})
            )
        except OSError:
            pass
