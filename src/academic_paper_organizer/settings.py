from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class AppSettings:
    watch_dir: str = ""
    output_dir: str = ""
    limit: str = "50"
    keyword: str = ""
    author: str = ""
    year: str = ""
    field: str = ""
    venue: str = ""

    recursive: str = "True"
    watch_mode: str = "all"
    selected_subdirs: list[str] | None = None

    @classmethod
    def load(cls, path: Path) -> "AppSettings":
        if not path.exists():
            return cls()

        data = json.loads(path.read_text(encoding="utf-8"))
        if "selected_subdirs" not in data or data["selected_subdirs"] is None:
            data["selected_subdirs"] = []

        return cls(**data)

    def save(self, path: Path) -> None:
        payload = asdict(self)
        if payload["selected_subdirs"] is None:
            payload["selected_subdirs"] = []

        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )