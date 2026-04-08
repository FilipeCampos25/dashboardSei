from __future__ import annotations

from contextlib import contextmanager
import json
import os
import time
from typing import Any, Dict


_active_profiler: "PerformanceProfiler | None" = None


class PerformanceProfiler:
    def __init__(self) -> None:
        self._active_spans: Dict[str, float] = {}
        self._data = {"spans": {}}

    def start_span(self, name: str) -> None:
        self._active_spans[name] = time.time()

    def end_span(self, name: str) -> None:
        started_at = self._active_spans.pop(name, None)
        if started_at is None:
            return

        self.add_time(name, time.time() - started_at)

    def add_time(self, name: str, seconds: float) -> None:
        span = self._data["spans"].setdefault(
            name,
            {
                "total_seconds": 0.0,
                "calls": 0,
            },
        )
        span["total_seconds"] += float(seconds)
        span["calls"] += 1

    def get_summary(self) -> dict:
        return {
            "spans": {
                name: {
                    "total_seconds": values["total_seconds"],
                    "calls": values["calls"],
                }
                for name, values in self._data["spans"].items()
            }
        }

    def export_json(self, path: str) -> None:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(path, "w", encoding="utf-8") as file:
            json.dump(self.get_summary(), file, ensure_ascii=False, indent=2)


def set_active_profiler(profiler: "PerformanceProfiler | None") -> None:
    global _active_profiler
    _active_profiler = profiler


def profiler_sleep(seconds: float) -> None:
    started_at = time.time()
    time.sleep(seconds)
    if _active_profiler is not None:
        _active_profiler.add_time("espera:sleep", time.time() - started_at)


def get_profiler_from_target(target: Any) -> PerformanceProfiler | None:
    profiler = getattr(target, "_performance_profiler", None)
    if isinstance(profiler, PerformanceProfiler):
        return profiler
    return None


def start_target_span(target: Any, name: str) -> None:
    profiler = get_profiler_from_target(target)
    if profiler is not None:
        profiler.start_span(name)


def end_target_span(target: Any, name: str) -> None:
    profiler = get_profiler_from_target(target)
    if profiler is not None:
        profiler.end_span(name)


@contextmanager
def target_span(target: Any, name: str):
    start_target_span(target, name)
    try:
        yield
    finally:
        end_target_span(target, name)
