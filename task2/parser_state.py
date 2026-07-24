"""Persistent per-file graph state used to emit replay delete events."""

from __future__ import annotations

import json
from pathlib import Path


STATE_VERSION = 1


class ParserStateStore:
    def __init__(self, directory: Path):
        self.directory = directory

    def _path(self, file_id: str) -> Path:
        return self.directory / f"{file_id}.json"

    def load(self, file_id: str) -> dict | None:
        path = self._path(file_id)
        if not path.exists():
            return None
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Cannot read parser state {path}: {exc}") from exc
        if state.get("state_version") != STATE_VERSION:
            raise RuntimeError(f"Unsupported parser state version in {path}")
        return state

    def save(self, file_id: str, state: dict) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self._path(file_id)
        temporary = path.with_suffix(".json.tmp")
        payload = {"state_version": STATE_VERSION, **state}
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
