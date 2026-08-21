"""Portable, root-scoped references to persisted artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


ARTIFACT_ROOT = "artifact_root"


class PortablePathError(ValueError):
    """Raised when an artifact reference is not portable or escapes its root."""


def _portable_relative(value: str) -> PurePosixPath:
    if not value or "\\" in value:
        raise PortablePathError("relative_path must use non-empty POSIX syntax")
    relative = PurePosixPath(value)
    if relative.is_absolute() or relative.anchor or ":" in relative.parts[0]:
        raise PortablePathError("relative_path must not be absolute")
    if any(part in ("", ".", "..") for part in relative.parts):
        raise PortablePathError("relative_path must not contain traversal segments")
    return relative


@dataclass(frozen=True)
class PortableArtifactRef:
    """A location relative to an explicit artifact root, never an identity."""

    relative_path: str
    root_kind: str = ARTIFACT_ROOT

    def __post_init__(self) -> None:
        relative = _portable_relative(str(self.relative_path))
        if self.root_kind != ARTIFACT_ROOT:
            raise PortablePathError(f"unsupported root_kind: {self.root_kind!r}")
        object.__setattr__(self, "relative_path", relative.as_posix())

    @classmethod
    def from_path(cls, path: str | Path, *, root: str | Path) -> "PortableArtifactRef":
        root_path = Path(root).resolve(strict=False)
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = root_path.joinpath(*_portable_relative(candidate.as_posix()).parts)
        candidate = candidate.resolve(strict=False)
        try:
            relative = candidate.relative_to(root_path)
        except ValueError as exc:
            raise PortablePathError("artifact path is outside the explicit root") from exc
        if not relative.parts:
            raise PortablePathError("artifact path must identify a file below the root")
        return cls(relative.as_posix())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PortableArtifactRef":
        return cls(relative_path=str(value.get("relative_path", "")), root_kind=str(value.get("root_kind", "")))

    def to_dict(self) -> dict[str, str]:
        return {"root_kind": self.root_kind, "relative_path": self.relative_path}

    def resolve(self, root: str | Path) -> Path:
        root_path = Path(root).resolve(strict=False)
        relative = _portable_relative(self.relative_path)
        target = root_path.joinpath(*relative.parts).resolve(strict=False)
        try:
            target.relative_to(root_path)
        except ValueError as exc:
            raise PortablePathError("resolved artifact escapes the explicit root") from exc
        return target
