"""Round-level staging and publication for pipeline outputs."""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


class RunPublication:
    """Keep generated artifacts private until the whole round succeeds."""

    def __init__(self, published_dir: str | Path, *, started_at: datetime | None = None) -> None:
        self.published_dir = Path(published_dir).resolve()
        self.started_at = started_at or datetime.now(timezone.utc)
        self.run_id = f"{self.started_at.strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:12]}"
        self.private_root = self.published_dir.parent / ".run_publication"
        self.staging_dir = self.private_root / "staging" / self.run_id
        self.backup_dir = self.private_root / "backup" / self.run_id
        self.attempt_path = self.private_root / "run_attempt_latest.json"
        self._active = False

    @property
    def active(self) -> bool:
        return self._active

    def begin(self) -> Path:
        self.private_root.mkdir(parents=True, exist_ok=True)
        self.staging_dir.parent.mkdir(parents=True, exist_ok=True)
        if self.published_dir.exists():
            shutil.copytree(self.published_dir, self.staging_dir)
        else:
            self.staging_dir.mkdir(parents=True)
        self._active = True
        self._write_attempt("in_progress")
        return self.staging_dir

    def publish(self) -> None:
        if not self._active:
            raise RuntimeError("No active run publication")

        self._write_json(
            self.staging_dir / "run_manifest_latest.json",
            self._state_payload("complete", published=True),
        )
        moved_previous = False
        try:
            if self.published_dir.exists():
                self.backup_dir.parent.mkdir(parents=True, exist_ok=True)
                os.replace(self.published_dir, self.backup_dir)
                moved_previous = True
            os.replace(self.staging_dir, self.published_dir)
        except BaseException:
            if moved_previous and self.backup_dir.exists() and not self.published_dir.exists():
                os.replace(self.backup_dir, self.published_dir)
            raise
        else:
            self._active = False
            if self.backup_dir.exists():
                try:
                    shutil.rmtree(self.backup_dir)
                except OSError:
                    # A private backup cannot be mistaken for published output.
                    pass
            self._write_attempt("complete", published=True)

    def abort(self, error: BaseException) -> None:
        if not self._active:
            return
        self._active = False
        if self.staging_dir.exists():
            try:
                shutil.rmtree(self.staging_dir)
            except OSError:
                # Failed staging remains private; recording the original error has priority.
                pass
        self._write_attempt(
            "incomplete",
            error={"type": type(error).__name__, "message": str(error)},
        )

    def _state_payload(
        self,
        status: str,
        *,
        published: bool = False,
        error: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": "1.0",
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "published": published,
        }
        if error is not None:
            payload["error"] = error
        return payload

    def _write_attempt(self, status: str, **details: Any) -> None:
        self._write_json(self.attempt_path, self._state_payload(status, **details))

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
