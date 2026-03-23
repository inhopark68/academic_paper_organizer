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

    @classmethod
    def load(cls, path: Path) -> "AppSettings":
        if not path.exists():
            return cls()

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return cls()

        defaults = cls()
        values: dict[str, str] = {}
        for field_name in cls.__dataclass_fields__:
            values[field_name] = str(data.get(field_name, getattr(defaults, field_name)))
        return cls(**values)

    def save(self, path: Path) -> None:
        path.write_text(
            json.dumps(asdict(self), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )