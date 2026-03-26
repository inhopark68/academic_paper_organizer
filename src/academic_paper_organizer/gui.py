from __future__ import annotations

import csv
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
import zipfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import quote_plus

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .core import (
    FIELD_CODES,
    PDFCreatedHandler,
    PaperIndex,
    PaperOrganizer,
    repair_and_reindex,
    scan_existing_pdfs,
    search_pubmed_by_title,
)
try:
    from .core import scan_existing_pdfs_fast
except Exception:
    scan_existing_pdfs_fast = None


@dataclass
class AppConfig:
    watch_dir: str = ""
    output_dir: str = ""
    keyword: str = ""
    author: str = ""
    year: str = ""
    field: str = ""
    venue: str = ""
    doc_type: str = ""
    limit: str = "50"

    recursive: bool = True
    watch_mode: str = "all"  # all | selected
    selected_subdirs: list[str] | None = None

    crossref_mailto: str = "your-email@example.com"
    crossref_cache_days: int = 180

    @classmethod
    def load(cls, path: Path) -> "AppConfig":
        if not path.exists():
            return cls(selected_subdirs=[])

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return cls(
                watch_dir=str(data.get("watch_dir", "")).strip(),
                output_dir=str(data.get("output_dir", "")).strip(),
                keyword=str(data.get("keyword", "")).strip(),
                author=str(data.get("author", "")).strip(),
                year=str(data.get("year", "")).strip(),
                field=str(data.get("field", "")).strip(),
                venue=str(data.get("venue", "")).strip(),
                doc_type=str(data.get("doc_type", "")).strip(),
                limit=str(data.get("limit", "50")).strip() or "50",
                recursive=bool(data.get("recursive", True)),
                watch_mode=str(data.get("watch_mode", "all")).strip() or "all",
                selected_subdirs=[
                    str(p).strip()
                    for p in data.get("selected_subdirs", []) or []
                    if str(p).strip()
                ],
                crossref_mailto=str(
                    data.get("crossref_mailto", "your-email@example.com")
                ).strip()
                or "your-email@example.com",
                crossref_cache_days=int(data.get("crossref_cache_days", 180)),
            )
        except Exception:
            return cls(selected_subdirs=[])

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(asdict(self), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


class FilteringEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        delegate: FileSystemEventHandler,
        allowed_roots: list[Path],
    ) -> None:
        super().__init__()
        self.delegate = delegate
        self.allowed_roots = [p.resolve() for p in allowed_roots]

    def _is_allowed(self, src_path: str) -> bool:
        try:
            target = Path(src_path).resolve()
        except Exception:
            return False

        for root in self.allowed_roots:
            try:
                target.relative_to(root)
                return True
            except ValueError:
                continue
        return False

    def on_created(self, event):
        if event.is_directory:
            return
        if self._is_allowed(event.src_path):
            handler = getattr(self.delegate, "on_created", None)
            if handler:
                handler(event)

    def on_moved(self, event):
        if event.is_directory:
            return
        dest_path = getattr(event, "dest_path", "")
        if dest_path and self._is_allowed(dest_path):
            handler = getattr(self.delegate, "on_moved", None)
            if handler:
                handler(event)

    def on_modified(self, event):
        if event.is_directory:
            return
        if self._is_allowed(event.src_path):
            handler = getattr(self.delegate, "on_modified", None)
            if handler:
                handler(event)


class OrganizerGUI:
    COLUMNS = (
        "doc_type",
        "field",
        "year",
        "author",
        "venue",
        "title",
        "doi",
        "path",
        "original_path",
    )

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Academic Paper Organizer")
        self.root.geometry("1460x900")
        self.root.minsize(1240, 760)

        self.config_path = Path.home() / ".academic_paper_organizer_gui.json"
        self.app_config = AppConfig.load(self.config_path)

        self.watch_var = tk.StringVar(value=self.app_config.watch_dir)
        self.output_var = tk.StringVar(value=self.app_config.output_dir)
        self.keyword_var = tk.StringVar(value=self.app_config.keyword)
        self.author_var = tk.StringVar(value=self.app_config.author)
        self.year_var = tk.StringVar(value=self.app_config.year)
        self.field_var = tk.StringVar(value=self.app_config.field)
        self.venue_var = tk.StringVar(value=self.app_config.venue)
        self.doc_type_var = tk.StringVar(value=self.app_config.doc_type)
        self.limit_var = tk.StringVar(value=self.app_config.limit)
        self.search_file_var = tk.StringVar(value="")

        self.crossref_email_var = tk.StringVar(value=self.app_config.crossref_mailto)
        self.crossref_cache_days_var = tk.StringVar(
            value=str(self.app_config.crossref_cache_days)
        )

        self.status_var = tk.StringVar(value="대기 중")
        self.progress_var = tk.StringVar(value="진행률: 대기 중")
        self.scan_counter_var = tk.StringVar(value="처리 0 | skip 0 | 오류 0")

        self.result_count_var = tk.StringVar(value="검색 결과: 0건")
        self.academic_count_var = tk.StringVar(value="학술 논문: 0건")
        self.non_academic_count_var = tk.StringVar(value="비학술 문서: 0건")
        self.unknown_count_var = tk.StringVar(value="미분류: 0건")

        self.recursive_var = tk.BooleanVar(value=self.app_config.recursive)
        self.watch_mode_var = tk.StringVar(
            value=(
                self.app_config.watch_mode
                if self.app_config.watch_mode in ("all", "selected")
                else "all"
            )
        )
        self.selected_subdirs: list[Path] = [
            Path(p) for p in (self.app_config.selected_subdirs or []) if str(p).strip()
        ]
        self.selected_subdirs_label_var = tk.StringVar(value="선택된 하위 폴더 없음")

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.gui_log_path: Path | None = None

        self.observer: Observer | None = None
        self.organizer: PaperOrganizer | None = None
        self.worker_thread: threading.Thread | None = None
        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()

        self.snippets: dict[str, str] = {}
        self.sort_state: dict[str, bool] = {}
        self.current_sort_column: str | None = None
        self.checked_items: set[str] = set()

        self.use_boxed_widgets = True

        self._configure_styles()
        self._build_ui()
        self._update_gui_log_path()
        self._format_selected_subdirs_label()
        self._on_watch_option_changed()
        self._update_button_states()
        self._reset_progress_text()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(150, self._drain_log_queue)

        self.append_log("[GUI] 애플리케이션 시작")

    def _configure_styles(self) -> None:
        style = ttk.Style()

        if "clam" in style.theme_names():
            style.theme_use("clam")

        bg = "#F7F9FC"
        panel = "#FFFFFF"
        border = "#D9E2EC"
        text = "#243447"
        muted = "#7A869A"

        self.root.configure(bg=bg)

        style.configure("TFrame", background=bg)
        style.configure("TLabel", background=bg, foreground=text)
        style.configure(
            "TLabelframe",
            background=bg,
            bordercolor=border,
            relief="groove",
            borderwidth=1,
        )
        style.configure(
            "TLabelframe.Label",
            background=bg,
            foreground=text,
            font=("맑은 고딕", 10, "bold"),
        )
        style.configure("TCheckbutton", background=bg, foreground=text)
        style.configure("TRadiobutton", background=bg, foreground=text)

        style.configure(
            "TEntry",
            padding=7,
            relief="flat",
            borderwidth=0,
            fieldbackground=panel,
            foreground=text,
        )
        style.configure(
            "TCombobox",
            padding=7,
            fieldbackground=panel,
            foreground=text,
            borderwidth=0,
            relief="flat",
        )
        style.map(
            "TCombobox",
            fieldbackground=[("readonly", panel)],
            foreground=[("readonly", text)],
        )

        style.configure(
            "Treeview",
            rowheight=28,
            background=panel,
            fieldbackground=panel,
            foreground=text,
            borderwidth=1,
            relief="solid",
        )
        style.configure(
            "Treeview.Heading",
            relief="raised",
            borderwidth=1,
            padding=(8, 6),
            font=("맑은 고딕", 9, "bold"),
        )

        style.configure(
            "TNotebook",
            background=bg,
            borderwidth=0,
        )
        style.configure(
            "TNotebook.Tab",
            padding=(18, 8),
            relief="raised",
            borderwidth=1,
            font=("맑은 고딕", 10, "bold"),
        )
        style.map(
            "TNotebook.Tab",
            expand=[("selected", (1, 1, 1, 0))],
        )

        common_button = dict(
            padding=(14, 8),
            relief="raised",
            borderwidth=1,
            focusthickness=0,
            font=("맑은 고딕", 9, "bold"),
        )

        style.configure(
            "Soft.TButton",
            background="#F4F6F8",
            foreground=text,
            **common_button,
        )
        style.map(
            "Soft.TButton",
            relief=[("pressed", "sunken"), ("active", "raised")],
            background=[
                ("active", "#E9EEF3"),
                ("pressed", "#DDE5EC"),
                ("disabled", "#F5F7F9"),
            ],
            foreground=[("disabled", muted)],
        )

        style.configure(
            "Blue.TButton",
            background="#EAF3FF",
            foreground="#23405F",
            **common_button,
        )
        style.map(
            "Blue.TButton",
            relief=[("pressed", "sunken"), ("active", "raised")],
            background=[
                ("active", "#DCEBFF"),
                ("pressed", "#C9DCF8"),
                ("disabled", "#F3F6FA"),
            ],
            foreground=[("disabled", muted)],
        )

        style.configure(
            "Green.TButton",
            background="#EAF8EE",
            foreground="#234B2C",
            **common_button,
        )
        style.map(
            "Green.TButton",
            relief=[("pressed", "sunken"), ("active", "raised")],
            background=[
                ("active", "#DCF2E3"),
                ("pressed", "#C8E4D1"),
                ("disabled", "#F3F7F4"),
            ],
            foreground=[("disabled", muted)],
        )

        style.configure(
            "Orange.TButton",
            background="#FFF4E8",
            foreground="#6B4A2B",
            **common_button,
        )
        style.map(
            "Orange.TButton",
            relief=[("pressed", "sunken"), ("active", "raised")],
            background=[
                ("active", "#FFEAD6"),
                ("pressed", "#F6D8B7"),
                ("disabled", "#FAF6F2"),
            ],
            foreground=[("disabled", muted)],
        )

        style.configure(
            "Red.TButton",
            background="#FDEEEF",
            foreground="#7A3940",
            **common_button,
        )
        style.map(
            "Red.TButton",
            relief=[("pressed", "sunken"), ("active", "raised")],
            background=[
                ("active", "#F9E0E2"),
                ("pressed", "#EECACE"),
                ("disabled", "#FAF4F4"),
            ],
            foreground=[("disabled", muted)],
        )

    def _make_box(
        self,
        parent,
        *,
        border_color: str = "#D9E2EC",
        bd: int = 1,
        relief: str = "raised",
    ) -> tk.Frame:
        return tk.Frame(
            parent,
            bg=border_color,
            bd=bd,
            relief=relief,
            highlightthickness=0,
        )

    def _create_button(
        self,
        parent,
        *,
        text: str,
        command,
        style_name: str | None = None,
        width: int = 14,
    ):
        kwargs = {
            "text": text,
            "command": command,
            "width": width,
        }
        if style_name:
            kwargs["style"] = style_name
        return ttk.Button(parent, **kwargs)

    def _create_entry(
        self,
        parent,
        *,
        textvariable,
        width: int | None = None,
    ):
        kwargs = {"textvariable": textvariable}
        if width is not None:
            kwargs["width"] = width
        return ttk.Entry(parent, **kwargs)

    def _create_combobox(
        self,
        parent,
        *,
        textvariable,
        values,
        state: str = "readonly",
    ):
        return ttk.Combobox(
            parent,
            textvariable=textvariable,
            values=values,
            state=state,
        )

    def _grid_button(
        self,
        parent,
        *,
        text: str,
        command,
        row: int,
        column: int,
        style_name: str | None = None,
        width: int = 14,
        rowspan: int = 1,
        columnspan: int = 1,
        sticky: str = "",
        padx=0,
        pady=0,
    ):
        if self.use_boxed_widgets:
            box = self._make_box(parent, relief="raised")
            btn = self._create_button(
                box,
                text=text,
                command=command,
                style_name=style_name,
                width=width,
            )
            btn.pack(fill="both", expand=True, padx=2, pady=2)
            box.grid(
                row=row,
                column=column,
                rowspan=rowspan,
                columnspan=columnspan,
                sticky=sticky,
                padx=padx,
                pady=pady,
            )
            return btn

        btn = self._create_button(
            parent,
            text=text,
            command=command,
            style_name=style_name,
            width=width,
        )
        btn.grid(
            row=row,
            column=column,
            rowspan=rowspan,
            columnspan=columnspan,
            sticky=sticky,
            padx=padx,
            pady=pady,
        )
        return btn

    def _grid_entry(
        self,
        parent,
        *,
        textvariable,
        row: int,
        column: int,
        width: int | None = None,
        rowspan: int = 1,
        columnspan: int = 1,
        sticky: str = "",
        padx=0,
        pady=0,
    ):
        if self.use_boxed_widgets:
            box = self._make_box(parent, relief="sunken")
            box.columnconfigure(0, weight=1)
            entry = self._create_entry(box, textvariable=textvariable, width=width)
            entry.grid(row=0, column=0, sticky="ew", padx=2, pady=2)
            box.grid(
                row=row,
                column=column,
                rowspan=rowspan,
                columnspan=columnspan,
                sticky=sticky,
                padx=padx,
                pady=pady,
            )
            return entry

        entry = self._create_entry(parent, textvariable=textvariable, width=width)
        entry.grid(
            row=row,
            column=column,
            rowspan=rowspan,
            columnspan=columnspan,
            sticky=sticky,
            padx=padx,
            pady=pady,
        )
        return entry

    def _grid_combobox(
        self,
        parent,
        *,
        textvariable,
        values,
        row: int,
        column: int,
        state: str = "readonly",
        rowspan: int = 1,
        columnspan: int = 1,
        sticky: str = "",
        padx=0,
        pady=0,
    ):
        if self.use_boxed_widgets:
            box = self._make_box(parent, relief="sunken")
            box.columnconfigure(0, weight=1)
            combo = self._create_combobox(
                box,
                textvariable=textvariable,
                values=values,
                state=state,
            )
            combo.grid(row=0, column=0, sticky="ew", padx=2, pady=2)
            box.grid(
                row=row,
                column=column,
                rowspan=rowspan,
                columnspan=columnspan,
                sticky=sticky,
                padx=padx,
                pady=pady,
            )
            return combo

        combo = self._create_combobox(
            parent,
            textvariable=textvariable,
            values=values,
            state=state,
        )
        combo.grid(
            row=row,
            column=column,
            rowspan=rowspan,
            columnspan=columnspan,
            sticky=sticky,
            padx=padx,
            pady=pady,
        )
        return combo

    def _pack_button(
        self,
        parent,
        *,
        text: str,
        command,
        style_name: str | None = None,
        width: int = 14,
        side="left",
        padx=0,
        pady=0,
    ):
        if self.use_boxed_widgets:
            box = self._make_box(parent, relief="raised")
            btn = self._create_button(
                box,
                text=text,
                command=command,
                style_name=style_name,
                width=width,
            )
            btn.pack(fill="both", expand=True, padx=2, pady=2)
            box.pack(side=side, padx=padx, pady=pady)
            return btn

        btn = self._create_button(
            parent,
            text=text,
            command=command,
            style_name=style_name,
            width=width,
        )
        btn.pack(side=side, padx=padx, pady=pady)
        return btn


    def _set_progress_text(self, text: str) -> None:
        self.root.after(0, lambda: self.progress_var.set(f"진행률: {text}"))

    def _reset_progress_text(self) -> None:
        self.root.after(0, lambda: self.progress_var.set("진행률: 대기 중"))
        self.root.after(0, lambda: self.scan_counter_var.set("처리 0 | skip 0 | 오류 0"))

    def _set_scan_summary(self, processed: int = 0, skipped: int = 0, errors: int = 0) -> None:
        self.root.after(
            0,
            lambda: self.scan_counter_var.set(
                f"처리 {processed} | skip {skipped} | 오류 {errors}"
            ),
        )

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill="both", expand=True)

        notebook = ttk.Notebook(outer)
        notebook.pack(fill="both", expand=True)

        settings_tab = ttk.Frame(notebook, padding=10)
        search_tab = ttk.Frame(notebook, padding=10)
        log_tab = ttk.Frame(notebook, padding=10)
        notebook.add(settings_tab, text="기본 설정")
        notebook.add(search_tab, text="검색")
        notebook.add(log_tab, text="로그")

        top = ttk.LabelFrame(settings_tab, text="기본 설정", padding=10)
        top.pack(fill="x")
        top.columnconfigure(1, weight=1)
        top.columnconfigure(4, weight=1)

        ttk.Label(top, text="감시 폴더").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self._grid_entry(
            top,
            textvariable=self.watch_var,
            row=0,
            column=1,
            sticky="ew",
            pady=4,
        )
        self._grid_button(
            top,
            text="찾아보기",
            command=self.pick_watch_dir,
            style_name="Soft.TButton",
            width=10,
            row=0,
            column=2,
            padx=6,
            pady=4,
        )

        ttk.Label(top, text="출력 폴더").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self._grid_entry(
            top,
            textvariable=self.output_var,
            row=1,
            column=1,
            sticky="ew",
            pady=4,
        )
        self._grid_button(
            top,
            text="찾아보기",
            command=self.pick_output_dir,
            style_name="Soft.TButton",
            width=10,
            row=1,
            column=2,
            padx=6,
            pady=4,
        )

        ttk.Label(top, text="Crossref 이메일").grid(
            row=0, column=3, sticky="w", padx=(16, 8), pady=4
        )
        self._grid_entry(
            top,
            textvariable=self.crossref_email_var,
            row=0,
            column=4,
            sticky="ew",
            pady=4,
        )

        ttk.Label(top, text="캐시 보관일").grid(
            row=1, column=3, sticky="w", padx=(16, 8), pady=4
        )
        self._grid_entry(
            top,
            textvariable=self.crossref_cache_days_var,
            width=12,
            row=1,
            column=4,
            sticky="w",
            pady=4,
        )

        self._grid_button(
            top,
            text="설정 저장",
            command=self.save_settings,
            style_name="Green.TButton",
            width=12,
            row=0,
            column=5,
            rowspan=2,
            padx=6,
            pady=4,
            sticky="ns",
        )

        controls_box = ttk.LabelFrame(settings_tab, text="실행 / 관리", padding=10)
        controls_box.pack(fill="x", pady=(10, 0))

        controls_row1 = ttk.Frame(controls_box)
        controls_row1.pack(fill="x")
        controls_row2 = ttk.Frame(controls_box)
        controls_row2.pack(fill="x", pady=(6, 0))

        self.run_once_btn = self._pack_button(
            controls_row1,
            text="1회 처리",
            command=self.run_once,
            style_name="Blue.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )
        self.start_watch_btn = self._pack_button(
            controls_row1,
            text="감시 시작",
            command=self.start_watch,
            style_name="Green.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )
        self.stop_watch_btn = self._pack_button(
            controls_row1,
            text="감시 중지",
            command=self.stop_watch,
            style_name="Orange.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )
        self.cancel_btn = self._pack_button(
            controls_row1,
            text="작업 취소",
            command=self.cancel_current_task,
            style_name="Red.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )
        self.pause_btn = self._pack_button(
            controls_row1,
            text="작업 정지",
            command=self.pause_current_task,
            style_name="Orange.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )
        self.resume_btn = self._pack_button(
            controls_row1,
            text="작업 재시작",
            command=self.resume_current_task,
            style_name="Green.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )

        self.reindex_btn = self._pack_button(
            controls_row2,
            text="연도보정+재인덱싱",
            command=self.reindex,
            style_name="Blue.TButton",
            width=18,
            side="left",
            padx=4,
            pady=4,
        )
        self.reparse_btn = self._pack_button(
            controls_row2,
            text="전체 재파싱(빠름)",
            command=self.reparse_all,
            style_name="Green.TButton",
            width=16,
            side="left",
            padx=4,
            pady=4,
        )
        self._pack_button(
            controls_row2,
            text="캐시 삭제",
            command=self.clear_crossref_cache,
            style_name="Orange.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )
        self._pack_button(
            controls_row2,
            text="DB 폴더 열기",
            command=self.open_db_folder,
            style_name="Soft.TButton",
            width=12,
            side="left",
            padx=4,
            pady=4,
        )
        self._pack_button(
            controls_row2,
            text="출력 폴더 열기",
            command=self.open_output_folder,
            style_name="Soft.TButton",
            width=14,
            side="left",
            padx=4,
            pady=4,
        )

        status_box = ttk.LabelFrame(settings_tab, text="상태", padding=10)
        status_box.pack(fill="x", pady=(10, 0))

        ttk.Label(
            status_box,
            textvariable=self.status_var,
            anchor="w",
        ).pack(fill="x", padx=4, pady=(0, 4))

        ttk.Label(
            status_box,
            textvariable=self.progress_var,
            anchor="w",
        ).pack(fill="x", padx=4, pady=4)

        ttk.Label(
            status_box,
            textvariable=self.scan_counter_var,
            anchor="w",
        ).pack(fill="x", padx=4, pady=(4, 0))

        options_box = ttk.LabelFrame(settings_tab, text="감시 옵션", padding=10)
        options_box.pack(fill="x", pady=(10, 0))
        options_box.columnconfigure(4, weight=1)

        ttk.Checkbutton(
            options_box,
            text="하위 폴더까지 포함",
            variable=self.recursive_var,
            command=self._on_watch_option_changed,
        ).grid(row=0, column=0, sticky="w", padx=(0, 12))

        ttk.Radiobutton(
            options_box,
            text="전체 감시",
            variable=self.watch_mode_var,
            value="all",
            command=self._on_watch_option_changed,
        ).grid(row=0, column=1, sticky="w", padx=(0, 8))

        ttk.Radiobutton(
            options_box,
            text="선택한 하위 폴더만 감시",
            variable=self.watch_mode_var,
            value="selected",
            command=self._on_watch_option_changed,
        ).grid(row=0, column=2, sticky="w", padx=(0, 8))

        self.select_subdirs_btn = self._grid_button(
            options_box,
            text="하위 폴더 선택",
            command=self.pick_subdirs,
            style_name="Soft.TButton",
            width=14,
            row=0,
            column=3,
            sticky="w",
            padx=(8, 8),
            pady=0,
        )

        ttk.Label(options_box, textvariable=self.selected_subdirs_label_var).grid(
            row=0, column=4, sticky="ew"
        )

        self._build_search_tab(search_tab)
        self._build_log_tab(log_tab)

    def _build_search_tab(self, parent: ttk.Frame) -> None:
        filters = ttk.LabelFrame(parent, text="검색 조건", padding=10)
        filters.pack(fill="x")
        for i in range(6):
            filters.columnconfigure(i, weight=1 if i in (1, 3, 5) else 0)

        ttk.Label(filters, text="키워드").grid(
            row=0, column=0, sticky="w", padx=(0, 6), pady=4
        )
        keyword_entry = self._grid_entry(
            filters,
            textvariable=self.keyword_var,
            row=0,
            column=1,
            sticky="ew",
            pady=4,
        )

        ttk.Label(filters, text="저자").grid(
            row=0, column=2, sticky="w", padx=(16, 6), pady=4
        )
        author_entry = self._grid_entry(
            filters,
            textvariable=self.author_var,
            row=0,
            column=3,
            sticky="ew",
            pady=4,
        )

        ttk.Label(filters, text="연도").grid(
            row=0, column=4, sticky="w", padx=(16, 6), pady=4
        )
        year_entry = self._grid_entry(
            filters,
            textvariable=self.year_var,
            width=10,
            row=0,
            column=5,
            sticky="ew",
            pady=4,
        )

        ttk.Label(filters, text="분야").grid(
            row=1, column=0, sticky="w", padx=(0, 6), pady=4
        )
        field_combo = self._grid_combobox(
            filters,
            textvariable=self.field_var,
            values=[""] + list(FIELD_CODES.keys()),
            state="readonly",
            row=1,
            column=1,
            sticky="ew",
            pady=4,
        )

        ttk.Label(filters, text="저널/학회").grid(
            row=1, column=2, sticky="w", padx=(16, 6), pady=4
        )
        venue_entry = self._grid_entry(
            filters,
            textvariable=self.venue_var,
            row=1,
            column=3,
            sticky="ew",
            pady=4,
        )

        ttk.Label(filters, text="문서유형").grid(
            row=1, column=4, sticky="w", padx=(16, 6), pady=4
        )
        doc_type_combo = self._grid_combobox(
            filters,
            textvariable=self.doc_type_var,
            values=["", "academic", "non_academic", "unknown"],
            state="readonly",
            row=1,
            column=5,
            sticky="ew",
            pady=4,
        )

        ttk.Label(filters, text="최대 건수").grid(
            row=2, column=4, sticky="w", padx=(16, 6), pady=4
        )
        limit_entry = self._grid_entry(
            filters,
            textvariable=self.limit_var,
            width=10,
            row=2,
            column=5,
            sticky="ew",
            pady=4,
        )

        for entry in [
            keyword_entry,
            author_entry,
            year_entry,
            field_combo,
            venue_entry,
            doc_type_combo,
            limit_entry,
        ]:
            entry.bind("<Return>", lambda event: self.search())

        action_row = ttk.Frame(parent)
        action_row.pack(fill="x", pady=(8, 8))

        action_row_top = ttk.Frame(action_row)
        action_row_top.pack(fill="x", anchor="w")

        action_row_bottom = ttk.Frame(action_row)
        action_row_bottom.pack(fill="x", anchor="w", pady=(6, 0))

        dashboard = ttk.LabelFrame(parent, text="검색 요약", padding=10)
        dashboard.pack(fill="x", pady=(0, 8))

        ttk.Label(dashboard, textvariable=self.result_count_var).grid(
            row=0, column=0, sticky="w", padx=(0, 20)
        )
        ttk.Label(dashboard, textvariable=self.academic_count_var).grid(
            row=0, column=1, sticky="w", padx=(0, 20)
        )
        ttk.Label(dashboard, textvariable=self.non_academic_count_var).grid(
            row=0, column=2, sticky="w", padx=(0, 20)
        )
        ttk.Label(dashboard, textvariable=self.unknown_count_var).grid(
            row=0, column=3, sticky="w"
        )


        self._pack_button(
            action_row_top,
            text="검색",
            command=self.search,
            style_name="Blue.TButton",
            width=10,
            side="left",
        )
        self._pack_button(
            action_row_top,
            text="필터 초기화",
            command=self.clear_filters,
            style_name="Soft.TButton",
            width=12,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_top,
            text="정리본 파일 열기",
            command=self.open_selected_file,
            style_name="Soft.TButton",
            width=14,
            side="left",
            padx=(24, 6),
        )
        self._pack_button(
            action_row_top,
            text="정리본 폴더 열기",
            command=self.open_selected_folder,
            style_name="Soft.TButton",
            width=14,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_top,
            text="원본 파일 열기",
            command=self.open_selected_original_file,
            style_name="Soft.TButton",
            width=14,
            side="left",
            padx=(24, 6),
        )
        self._pack_button(
            action_row_top,
            text="원본 폴더 열기",
            command=self.open_selected_original_folder,
            style_name="Soft.TButton",
            width=14,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_top,
            text="DOI 열기",
            command=self.open_selected_doi,
            style_name="Soft.TButton",
            width=10,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_top,
            text="선택 파일 PubMed",
            command=self.open_checked_pubmed,
            style_name="Blue.TButton",
            width=16,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_bottom,
            text="선택 파일 모으기",
            command=self.collect_selected_files,
            style_name="Green.TButton",
            width=14,
            side="left",
            padx=(24, 6),
        )
        self._pack_button(
            action_row_bottom,
            text="선택 파일 지우기",
            command=self.delete_checked_files,
            style_name="Red.TButton",
            width=14,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_bottom,
            text="선택 파일 ZIP",
            command=self.export_selected_zip,
            style_name="Orange.TButton",
            width=12,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_bottom,
            text="선택 목록 CSV 저장",
            command=self.export_selected_csv,
            style_name="Soft.TButton",
            width=15,
            side="left",
            padx=6,
        )
        self._pack_button(
            action_row_bottom,
            text="검색 결과 전체 CSV 저장",
            command=self.export_all_results_csv,
            style_name="Soft.TButton",
            width=22,
            side="left",
            padx=6,
        )

        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=self.COLUMNS,
            show="tree headings",
            height=18,
            selectmode="extended",
        )
        self.tree.heading("#0", text="선택")
        self.tree.column("#0", width=64, minwidth=56, anchor="center", stretch=False)

        self.base_headings = {
            "doc_type": "문서유형",
            "field": "분야",
            "year": "연도",
            "author": "저자",
            "venue": "저널/학회",
            "title": "제목",
            "doi": "DOI",
            "path": "정리본 경로",
            "original_path": "원본 경로",
        }
        widths = {
            "doc_type": 90,
            "field": 70,
            "year": 70,
            "author": 110,
            "venue": 160,
            "title": 340,
            "doi": 180,
            "path": 280,
            "original_path": 320,
        }

        for col in self.COLUMNS:
            self.tree.heading(
                col,
                text=self.base_headings[col],
                command=lambda c=col: self.sort_tree_by(c),
            )
            self.tree.column(col, width=widths[col], anchor="w")

        tree_v_scrollbar = ttk.Scrollbar(
            tree_frame,
            orient="vertical",
            command=self.tree.yview,
        )
        tree_h_scrollbar = ttk.Scrollbar(
            tree_frame,
            orient="horizontal",
            command=self.tree.xview,
        )
        self.tree.configure(
            yscrollcommand=tree_v_scrollbar.set,
            xscrollcommand=tree_h_scrollbar.set,
        )

        tree_v_scrollbar.pack(side="right", fill="y")
        tree_h_scrollbar.pack(side="bottom", fill="x")
        self.tree.pack(side="left", fill="both", expand=True)
        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Button-1>", self._on_tree_click, add="+")
        self.tree.bind("<space>", self._toggle_checked_for_selection)

        snippet_box = ttk.LabelFrame(parent, text="미리보기", padding=8)
        snippet_box.pack(fill="both", expand=False, pady=(8, 0))

        snippet_frame = ttk.Frame(snippet_box)
        snippet_frame.pack(fill="both", expand=True)

        self.snippet_text = tk.Text(
            snippet_frame,
            height=8,
            wrap="word",
            state="disabled",
        )
        snippet_scrollbar = ttk.Scrollbar(
            snippet_frame,
            orient="vertical",
            command=self.snippet_text.yview,
        )
        self.snippet_text.configure(yscrollcommand=snippet_scrollbar.set)

        snippet_scrollbar.pack(side="right", fill="y")
        self.snippet_text.pack(side="left", fill="both", expand=True)

    def _reset_search_dashboard(self) -> None:
        self.result_count_var.set("검색 결과: 0건")
        self.academic_count_var.set("학술 논문: 0건")
        self.non_academic_count_var.set("비학술 문서: 0건")
        self.unknown_count_var.set("미분류: 0건")

    def _update_search_dashboard(self, rows) -> None:
        total = len(rows)
        academic = 0
        non_academic = 0
        unknown = 0

        for row in rows:
            doc_type = getattr(row, "doc_type", "unknown") or "unknown"
            if doc_type == "academic":
                academic += 1
            elif doc_type == "non_academic":
                non_academic += 1
            else:
                unknown += 1

        self.result_count_var.set(f"검색 결과: {total}건")
        self.academic_count_var.set(f"학술 논문: {academic}건")
        self.non_academic_count_var.set(f"비학술 문서: {non_academic}건")
        self.unknown_count_var.set(f"미분류: {unknown}건")

    def _refresh_dashboard_from_tree(self) -> None:
        total = 0
        academic = 0
        non_academic = 0
        unknown = 0

        for item_id in self.tree.get_children():
            values = self.tree.item(item_id, "values")
            if not values:
                continue
            total += 1
            doc_type = str(values[0]).strip() if len(values) > 0 else "unknown"
            if doc_type == "academic":
                academic += 1
            elif doc_type == "non_academic":
                non_academic += 1
            else:
                unknown += 1

        self.result_count_var.set(f"검색 결과: {total}건")
        self.academic_count_var.set(f"학술 논문: {academic}건")
        self.non_academic_count_var.set(f"비학술 문서: {non_academic}건")
        self.unknown_count_var.set(f"미분류: {unknown}건")

    def _build_log_tab(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True)

        self.log_text = tk.Text(frame, wrap="none", state="disabled")
        log_v_scrollbar = ttk.Scrollbar(
            frame,
            orient="vertical",
            command=self.log_text.yview,
        )
        log_h_scrollbar = ttk.Scrollbar(
            frame,
            orient="horizontal",
            command=self.log_text.xview,
        )
        self.log_text.configure(
            yscrollcommand=log_v_scrollbar.set,
            xscrollcommand=log_h_scrollbar.set,
        )

        log_v_scrollbar.pack(side="right", fill="y")
        log_h_scrollbar.pack(side="bottom", fill="x")
        self.log_text.pack(side="left", fill="both", expand=True)

    def _set_text_widget(self, widget: tk.Text, content: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", content)
        widget.configure(state="disabled")

    def _append_text_widget(self, widget: tk.Text, content: str) -> None:
        widget.configure(state="normal")
        widget.insert("end", content)
        widget.see("end")
        widget.configure(state="disabled")

    def _update_gui_log_path(self) -> None:
        output = self.output_var.get().strip()
        if not output:
            self.gui_log_path = None
            return
        try:
            output_dir = Path(output).expanduser().resolve()
            log_dir = output_dir / "LOG"
            log_dir.mkdir(parents=True, exist_ok=True)
            self.gui_log_path = log_dir / "gui.log"
        except Exception:
            self.gui_log_path = None

    def _write_log_file(self, message: str) -> None:
        if self.gui_log_path is None:
            return
        try:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            with self.gui_log_path.open("a", encoding="utf-8") as f:
                f.write(f"{timestamp} {message}\n")
        except Exception:
            pass

    def append_log(self, message: str) -> None:
        self._update_gui_log_path()
        self._write_log_file(message)
        self.log_queue.put(message)
        self.root.after(0, lambda: self.status_var.set(message))

    def _drain_log_queue(self) -> None:
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_text_widget(self.log_text, message + "\n")
        self.root.after(150, self._drain_log_queue)

    def set_status(self, text: str) -> None:
        self.root.after(0, lambda: self.status_var.set(text))

    def show_error(self, title: str, message: str) -> None:
        self.root.after(0, lambda: messagebox.showerror(title, message))

    def save_settings(self) -> None:
        try:
            cache_days = int(self.crossref_cache_days_var.get().strip() or "180")
            if cache_days < 1:
                raise ValueError("캐시 보관일은 1 이상이어야 합니다.")

            self.app_config.watch_dir = self.watch_var.get().strip()
            self.app_config.output_dir = self.output_var.get().strip()
            self.app_config.keyword = self.keyword_var.get().strip()
            self.app_config.author = self.author_var.get().strip()
            self.app_config.year = self.year_var.get().strip()
            self.app_config.field = self.field_var.get().strip()
            self.app_config.venue = self.venue_var.get().strip()
            self.app_config.doc_type = self.doc_type_var.get().strip()
            self.app_config.limit = self.limit_var.get().strip() or "50"
            self.app_config.recursive = self.recursive_var.get()
            self.app_config.watch_mode = self.watch_mode_var.get().strip() or "all"
            self.app_config.selected_subdirs = [str(p) for p in self.selected_subdirs]
            self.app_config.crossref_mailto = (
                self.crossref_email_var.get().strip() or "your-email@example.com"
            )
            self.app_config.crossref_cache_days = cache_days
            self.app_config.save(self.config_path)

            self.append_log("[CONFIG] 설정 저장 완료")
            messagebox.showinfo("완료", "설정이 저장되었습니다.")
        except Exception as exc:
            messagebox.showerror("오류", f"설정 저장 실패: {exc}")

    def pick_watch_dir(self) -> None:
        path = filedialog.askdirectory(title="감시 폴더 선택")
        if path:
            self.watch_var.set(path)
            self.selected_subdirs = []
            self._format_selected_subdirs_label()

    def pick_output_dir(self) -> None:
        path = filedialog.askdirectory(title="출력 폴더 선택")
        if path:
            self.output_var.set(path)
            self._update_gui_log_path()

    def _validate_paths(self) -> tuple[Path, Path] | None:
        watch = self.watch_var.get().strip()
        output = self.output_var.get().strip()

        if not watch or not output:
            messagebox.showwarning("경고", "감시 폴더와 출력 폴더를 모두 지정해 주세요.")
            return None

        watch_path = Path(watch).expanduser().resolve()
        output_path = Path(output).expanduser().resolve()

        if not watch_path.exists() or not watch_path.is_dir():
            messagebox.showwarning("경고", f"감시 폴더가 올바르지 않습니다: {watch_path}")
            return None

        output_path.mkdir(parents=True, exist_ok=True)
        return watch_path, output_path

    def _get_crossref_cache_days(self) -> int:
        try:
            value = int(self.crossref_cache_days_var.get().strip() or "180")
            return max(1, value)
        except Exception:
            return 180

    def _get_parallel_workers(self) -> int:
        cpu = os.cpu_count() or 4
        return max(2, min(8, cpu))

    def _create_organizer(self, watch_dir: Path, output_dir: Path) -> PaperOrganizer:
        self.cancel_event.clear()
        return PaperOrganizer(
            watch_dir=watch_dir,
            output_dir=output_dir,
            log_fn=self.append_log,
            cancel_event=self.cancel_event,
            pause_event=self.pause_event,
            crossref_mailto=self.crossref_email_var.get().strip()
            or "your-email@example.com",
            crossref_cache_days=self._get_crossref_cache_days(),
        )


    def pause_current_task(self) -> None:
        self.pause_event.set()
        self.append_log("[PAUSE] 일시정지 요청됨")
        self.set_status("일시정지됨")
        self._update_button_states()

    def resume_current_task(self) -> None:
        self.pause_event.clear()
        self.append_log("[RESUME] 작업 재시작")
        self.set_status("작업 재시작")
        self._update_button_states()

    def _update_button_states(self) -> None:
        watching = self.observer is not None and self.observer.is_alive()
        worker_running = self.worker_thread is not None and self.worker_thread.is_alive()
        busy = watching or worker_running
        paused = self.pause_event.is_set()

        self.start_watch_btn.configure(state="disabled" if busy else "normal")
        self.stop_watch_btn.configure(state="normal" if watching else "disabled")
        self.run_once_btn.configure(state="disabled" if busy else "normal")
        self.reindex_btn.configure(state="disabled" if worker_running else "normal")
        self.reparse_btn.configure(state="disabled" if worker_running else "normal")
        self.cancel_btn.configure(state="normal" if busy else "disabled")
        self.pause_btn.configure(state="normal" if busy and not paused else "disabled")
        self.resume_btn.configure(state="normal" if busy and paused else "disabled")

    def _run_in_thread(self, target: Callable[[], None], status_text: str) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("안내", "이미 작업이 실행 중입니다.")
            return

        self.cancel_event.clear()
        self.pause_event.clear()
        self.status_var.set(status_text)
        self.worker_thread = threading.Thread(target=target, daemon=True)
        self.worker_thread.start()
        self._update_button_states()

        def poll_worker() -> None:
            if self.worker_thread and self.worker_thread.is_alive():
                self.root.after(200, poll_worker)
            else:
                self._update_button_states()

        self.root.after(200, poll_worker)

    def cancel_current_task(self) -> None:
        self.cancel_event.set()

        if self.organizer and hasattr(self.organizer, "request_cancel"):
            try:
                self.organizer.request_cancel()
            except Exception:
                pass

        if self.observer:
            try:
                self.observer.stop()
            except Exception:
                pass

        self.append_log("[CANCEL] 취소 요청됨")
        self.set_status("취소 요청됨")
        self._update_button_states()

    def _list_all_subdirs(self, root: Path) -> list[Path]:
        results: list[Path] = []
        try:
            for p in sorted(root.rglob("*")):
                if p.is_dir():
                    results.append(p.resolve())
        except Exception as exc:
            self.append_log(f"[WARN] 하위 폴더 조회 실패: {exc}")
        return results

    def _format_selected_subdirs_label(self) -> None:
        if not self.selected_subdirs:
            self.selected_subdirs_label_var.set("선택된 하위 폴더 없음")
            return

        names = []
        for p in self.selected_subdirs[:3]:
            try:
                root = Path(self.watch_var.get().strip()).expanduser().resolve()
                names.append(str(p.resolve().relative_to(root)))
            except Exception:
                names.append(p.name)

        suffix = ""
        if len(self.selected_subdirs) > 3:
            suffix = f" 외 {len(self.selected_subdirs) - 3}개"
        self.selected_subdirs_label_var.set(", ".join(names) + suffix)

    def _on_watch_option_changed(self) -> None:
        recursive = self.recursive_var.get()
        selected_mode = self.watch_mode_var.get() == "selected"
        state = "normal" if recursive and selected_mode else "disabled"
        if hasattr(self, "select_subdirs_btn"):
            self.select_subdirs_btn.configure(state=state)

    def pick_subdirs(self) -> None:
        watch = self.watch_var.get().strip()
        if not watch:
            messagebox.showinfo("안내", "먼저 감시 폴더를 선택해 주세요.")
            return

        root = Path(watch).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            messagebox.showwarning("경고", "감시 폴더가 올바르지 않습니다.")
            return

        subdirs = self._list_all_subdirs(root)
        if not subdirs:
            messagebox.showinfo("안내", "선택 가능한 하위 폴더가 없습니다.")
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("감시할 하위 폴더 선택")
        dialog.geometry("560x480")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(
            dialog,
            text="감시할 하위 폴더를 선택하세요. 선택한 폴더의 하위 폴더도 함께 감시됩니다.",
        ).pack(anchor="w", padx=10, pady=(10, 6))

        frame = ttk.Frame(dialog)
        frame.pack(fill="both", expand=True, padx=10, pady=10)

        listbox = tk.Listbox(frame, selectmode=tk.MULTIPLE)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=listbox.yview)
        listbox.configure(yscrollcommand=scrollbar.set)

        listbox.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        rel_paths: list[str] = []
        for subdir in subdirs:
            rel = str(subdir.relative_to(root))
            rel_paths.append(rel)
            listbox.insert("end", rel)

        selected_rel = set()
        for p in self.selected_subdirs:
            try:
                selected_rel.add(str(p.resolve().relative_to(root)))
            except Exception:
                pass

        for idx, rel in enumerate(rel_paths):
            if rel in selected_rel:
                listbox.selection_set(idx)

        btns = ttk.Frame(dialog)
        btns.pack(fill="x", padx=10, pady=(0, 10))

        def select_all() -> None:
            listbox.selection_set(0, "end")

        def clear_all() -> None:
            listbox.selection_clear(0, "end")

        def confirm() -> None:
            indices = listbox.curselection()
            self.selected_subdirs = [(root / rel_paths[i]).resolve() for i in indices]
            self._format_selected_subdirs_label()
            dialog.destroy()

        self._pack_button(
            btns,
            text="전체 선택",
            command=select_all,
            style_name="Soft.TButton",
            width=10,
            side="left",
        )
        self._pack_button(
            btns,
            text="선택 해제",
            command=clear_all,
            style_name="Soft.TButton",
            width=10,
            side="left",
            padx=6,
        )
        self._pack_button(
            btns,
            text="확인",
            command=confirm,
            style_name="Green.TButton",
            width=8,
            side="right",
        )
        self._pack_button(
            btns,
            text="취소",
            command=dialog.destroy,
            style_name="Red.TButton",
            width=8,
            side="right",
            padx=6,
        )

    def _get_effective_watch_roots(self, watch_dir: Path) -> list[Path]:
        if not self.recursive_var.get():
            return [watch_dir.resolve()]
        if self.watch_mode_var.get() == "all":
            return [watch_dir.resolve()]
        return [p.resolve() for p in self.selected_subdirs if p.exists() and p.is_dir()]

    def run_once(self) -> None:
        paths = self._validate_paths()
        if not paths:
            return
        watch_dir, output_dir = paths

        effective_roots = self._get_effective_watch_roots(watch_dir)
        if (
            self.recursive_var.get()
            and self.watch_mode_var.get() == "selected"
            and not effective_roots
        ):
            messagebox.showinfo("안내", "처리할 하위 폴더를 먼저 선택해 주세요.")
            return

        def task() -> None:
            organizer = self._create_organizer(watch_dir, output_dir)
            try:
                self.append_log(f"[SCAN] 1회 처리 시작: {watch_dir}")
                self._set_progress_text("스캔 준비 중")
                self._set_scan_summary(0, 0, 0)

                if not self.recursive_var.get() or self.watch_mode_var.get() == "all":
                    scan_existing_pdfs(
                        organizer,
                        watch_dir,
                        log_fn=self.append_log,
                        workers=self._get_parallel_workers(),
                        skip_unchanged=True,
                        force_reparse=False,
                    )
                else:
                    for subdir in effective_roots:
                        if self.cancel_event.is_set():
                            self.append_log("[CANCEL] 선택 하위 폴더 처리 취소됨")
                            break
                        self.append_log(f"[SCAN] 선택 하위 폴더 처리: {subdir}")
                        scan_existing_pdfs(
                            organizer,
                            subdir,
                            log_fn=self.append_log,
                            workers=self._get_parallel_workers(),
                            skip_unchanged=True,
                            force_reparse=False,
                        )

                if self.cancel_event.is_set():
                    self.append_log("[SCAN] 1회 처리 취소")
                    self.set_status("1회 처리 취소")
                else:
                    self.append_log("[SCAN] 1회 처리 완료")
                    self.set_status("1회 처리 완료")
                    self.root.after(0, self.search)
            except Exception as exc:
                self.append_log(f"[ERROR] 1회 처리 실패: {exc}")
                self.set_status("1회 처리 실패")
                self.show_error("오류", f"1회 처리 중 오류가 발생했습니다:\n{exc}")
            finally:
                organizer.close()

        self._run_in_thread(task, "1회 처리 중")

    def start_watch(self) -> None:
        if self.observer and self.observer.is_alive():
            messagebox.showinfo("안내", "이미 감시 중입니다.")
            return

        paths = self._validate_paths()
        if not paths:
            return
        watch_dir, output_dir = paths

        effective_roots = self._get_effective_watch_roots(watch_dir)
        if (
            self.recursive_var.get()
            and self.watch_mode_var.get() == "selected"
            and not effective_roots
        ):
            messagebox.showinfo("안내", "감시할 하위 폴더를 먼저 선택해 주세요.")
            return

        try:
            self.cancel_event.clear()
            self.pause_event.clear()
            self.organizer = self._create_organizer(watch_dir, output_dir)

            base_handler = PDFCreatedHandler(self.organizer)
            handler = FilteringEventHandler(base_handler, effective_roots)

            self.observer = Observer()
            self.observer.schedule(
                handler, str(watch_dir), recursive=self.recursive_var.get()
            )
            self.observer.start()

            mode_text = "루트만"
            if self.recursive_var.get() and self.watch_mode_var.get() == "all":
                mode_text = "전체 하위 폴더 포함"
            elif self.recursive_var.get() and self.watch_mode_var.get() == "selected":
                mode_text = f"선택 하위 폴더 {len(effective_roots)}개"

            self.status_var.set(f"감시 중: {watch_dir} ({mode_text})")
            self.append_log(f"[WATCH] 감시 시작: {watch_dir} | 모드={mode_text}")
        except Exception as exc:
            if self.organizer:
                self.organizer.close()
                self.organizer = None
            self.observer = None
            messagebox.showerror("오류", f"감시 시작에 실패했습니다:\n{exc}")
            self.status_var.set("감시 시작 실패")
        finally:
            self._update_button_states()

    def stop_watch(self) -> None:
        try:
            self.cancel_event.set()

            if self.observer:
                self.observer.stop()
                self.observer.join(timeout=3)
                self.observer = None

            if self.organizer:
                self.organizer.close()
                self.organizer = None

            self.pause_event.clear()
            self.status_var.set("감시 중지")
            self.append_log("[WATCH] 감시 중지")
        except Exception as exc:
            messagebox.showerror("오류", f"감시 중지 중 오류가 발생했습니다:\n{exc}")
        finally:
            self._update_button_states()

    def reparse_all(self) -> None:
        paths = self._validate_paths()
        if not paths:
            return

        watch_dir, output_dir = paths

        if not messagebox.askyesno(
            "확인",
            "기존 DB를 비우고 모든 PDF를 병렬로 다시 파싱합니다.\n계속하시겠습니까?"
        ):
            return

        effective_roots = self._get_effective_watch_roots(watch_dir)
        if (
            self.recursive_var.get()
            and self.watch_mode_var.get() == "selected"
            and not effective_roots
        ):
            messagebox.showinfo("안내", "처리할 하위 폴더를 먼저 선택해 주세요.")
            return

        def task() -> None:
            organizer = self._create_organizer(watch_dir, output_dir)
            try:
                self.append_log(
                    f"[REPARSE] 전체 재파싱 시작: {watch_dir} | workers={self._get_parallel_workers()}"
                )

                db_path = output_dir / "LOG" / "paper_index.sqlite3"
                if db_path.exists():
                    try:
                        organizer.close()
                    except Exception:
                        pass
                    db_path.unlink(missing_ok=True)
                    organizer = self._create_organizer(watch_dir, output_dir)

                if not self.recursive_var.get() or self.watch_mode_var.get() == "all":
                    scan_existing_pdfs(
                        organizer,
                        watch_dir,
                        log_fn=self.append_log,
                        workers=self._get_parallel_workers(),
                        skip_unchanged=False,
                        force_reparse=True,
                    )
                else:
                    for subdir in effective_roots:
                        if self.cancel_event.is_set():
                            self.append_log("[CANCEL] 전체 재파싱 취소됨")
                            break
                        self.append_log(f"[REPARSE] 선택 하위 폴더 재파싱: {subdir}")
                        scan_existing_pdfs(
                            organizer,
                            subdir,
                            log_fn=self.append_log,
                            workers=self._get_parallel_workers(),
                            skip_unchanged=False,
                            force_reparse=True,
                        )

                if self.cancel_event.is_set():
                    self.append_log("[REPARSE] 전체 재파싱 취소")
                    self.set_status("전체 재파싱 취소")
                else:
                    self.append_log("[REPARSE] 전체 재파싱 완료")
                    self.set_status("전체 재파싱 완료")
                    self.root.after(0, self.search)

            except Exception as exc:
                self.append_log(f"[ERROR] 전체 재파싱 실패: {exc}")
                self.set_status("전체 재파싱 실패")
                self.show_error("오류", f"전체 재파싱 중 오류가 발생했습니다:\n{exc}")
            finally:
                try:
                    organizer.close()
                except Exception:
                    pass

        self._run_in_thread(task, "전체 재파싱 중")

    def reindex(self) -> None:
        output = self.output_var.get().strip()
        if not output:
            messagebox.showwarning("경고", "출력 폴더를 지정해 주세요.")
            return

        output_dir = Path(output).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        cache_days = self._get_crossref_cache_days()

        def task() -> None:
            cancel_event = self.cancel_event

            class Args:
                pass

            args = Args()
            args.output = str(output_dir)

            self.append_log(f"[REPAIR] 연도 폴더 보정 + 재인덱싱 시작: {output_dir}")

            try:
                repair_and_reindex(
                    args,
                    log_fn=self.append_log,
                    cancel_event=cancel_event,
                    crossref_cache_days=cache_days,
                )

                if cancel_event.is_set():
                    self.append_log("[REPAIR] 연도 폴더 보정/재인덱싱 취소")
                    self.set_status("연도 폴더 보정/재인덱싱 취소")
                else:
                    self.append_log("[REPAIR] 연도 폴더 보정 + 재인덱싱 완료")
                    self.set_status("연도 폴더 보정 + 재인덱싱 완료")
                    self.root.after(0, self.search)

            except Exception as exc:
                self.append_log(f"[ERROR] 연도 폴더 보정/재인덱싱 실패: {exc}")
                self.set_status("연도 폴더 보정/재인덱싱 실패")
                self.show_error(
                    "오류", f"연도 폴더 보정/재인덱싱 중 오류가 발생했습니다:\n{exc}"
                )

        self._run_in_thread(task, "연도 폴더 보정 + 재인덱싱 중")

    def clear_crossref_cache(self) -> None:
        try:
            output_dir = self.output_var.get().strip()
            if not output_dir:
                messagebox.showwarning("경고", "출력 폴더를 먼저 설정하세요.")
                return

            db_path = Path(output_dir).resolve() / "LOG" / "paper_index.sqlite3"
            index = PaperIndex(db_path)
            try:
                index.clear_crossref_cache()
            finally:
                index.close()

            self.append_log("[CACHE] Crossref 캐시 삭제 완료")
            messagebox.showinfo("완료", "Crossref 캐시를 삭제했습니다.")
        except Exception as exc:
            messagebox.showerror("오류", f"캐시 삭제 실패: {exc}")

    def _clear_results(self) -> None:
        self.snippets.clear()
        self.sort_state.clear()
        self.current_sort_column = None
        self._refresh_heading_arrows()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._reset_search_dashboard()


    def clear_filters(self) -> None:
        self.keyword_var.set("")
        self.author_var.set("")
        self.year_var.set("")
        self.field_var.set("")
        self.venue_var.set("")
        self.doc_type_var.set("")
        self.limit_var.set("50")
        self._clear_results()
        self._set_text_widget(self.snippet_text, "")
        self.status_var.set("필터 초기화 완료")

    def search(self) -> None:
        output = self.output_var.get().strip()
        if not output:
            messagebox.showwarning("경고", "출력 폴더를 지정해 주세요.")
            return

        db_path = Path(output).expanduser().resolve() / "LOG" / "paper_index.sqlite3"
        if not db_path.exists():
            self.append_log("[WARN] 검색 인덱스가 없습니다.")
            self._clear_results()
            self._set_text_widget(self.snippet_text, "")
            return

        try:
            limit = int(self.limit_var.get().strip() or "50")
        except ValueError:
            messagebox.showwarning("경고", "최대 건수는 숫자로 입력해 주세요.")
            return

        index = PaperIndex(db_path)
        try:
            rows = index.search(
                keyword=self.keyword_var.get().strip(),
                author=self.author_var.get().strip() or None,
                year=self.year_var.get().strip() or None,
                field_code=self.field_var.get().strip() or None,
                venue=self.venue_var.get().strip() or None,
                doc_type=self.doc_type_var.get().strip() or None,
                limit=limit,
            )
        except Exception as exc:
            messagebox.showerror("오류", f"검색 중 오류가 발생했습니다:\n{exc}")
            return
        finally:
            index.close()

        self._clear_results()
        self._update_search_dashboard(rows)

        for row in rows:
            item_id = self.tree.insert(
                "",
                "end",
                text="☐",
                values=(
                    getattr(row, "doc_type", "unknown"),
                    row.field_code,
                    row.year,
                    row.first_author,
                    row.venue,
                    row.title,
                    row.doi,
                    row.path,
                    getattr(row, "original_path", ""),
                ),
            )
            self.snippets[item_id] = row.snippet or ""

        self.status_var.set(f"[SEARCH] 검색 완료: {len(rows)}건")
        self.append_log(f"[SEARCH] 검색 완료: {len(rows)}건")

        if rows:
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._on_tree_select(None)
        else:
            self._set_text_widget(self.snippet_text, "")

    def _on_tree_select(self, _event) -> None:
        selected = self.tree.selection()
        if not selected:
            self._set_text_widget(self.snippet_text, "")
            return
        item = selected[0]
        snippet = self.snippets.get(item, "")
        self._set_text_widget(self.snippet_text, snippet)

    def _selected_values(self):
        selected = self.tree.selection()
        if not selected:
            return None
        values = self.tree.item(selected[0], "values")
        return values or None

    def _set_item_checked(self, item_id: str, checked: bool) -> None:
        if checked:
            self.checked_items.add(item_id)
            self.tree.item(item_id, text="☑")
        else:
            self.checked_items.discard(item_id)
            self.tree.item(item_id, text="☐")

    def _toggle_item_checked(self, item_id: str) -> None:
        self._set_item_checked(item_id, item_id not in self.checked_items)

    def _checked_item_ids(self) -> list[str]:
        valid_items = set(self.tree.get_children())
        stale = [item_id for item_id in self.checked_items if item_id not in valid_items]
        for item_id in stale:
            self.checked_items.discard(item_id)
        return [item_id for item_id in self.tree.get_children() if item_id in self.checked_items]

    def _on_tree_click(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        column = self.tree.identify_column(event.x)
        item_id = self.tree.identify_row(event.y)

        if region == "tree" and column == "#0" and item_id:
            self._toggle_item_checked(item_id)
            return "break"

    def _toggle_checked_for_selection(self, event=None):
        selected = list(self.tree.selection())
        if not selected:
            return "break"

        should_check = any(item_id not in self.checked_items for item_id in selected)
        for item_id in selected:
            self._set_item_checked(item_id, should_check)
        return "break"

    def _selected_path(self) -> Path | None:
        values = self._selected_values()
        if not values:
            return None
        return Path(values[7])

    def _selected_original_path(self) -> Path | None:
        values = self._selected_values()
        if not values:
            return None
        original = str(values[8]).strip()
        if not original:
            return None
        return Path(original)

    def _selected_doi(self) -> str | None:
        values = self._selected_values()
        if not values:
            return None
        doi = str(values[6]).strip()
        return doi or None

    def _selected_paths(self) -> list[Path]:
        selected = self._checked_item_ids()
        results: list[Path] = []

        for item_id in selected:
            values = self.tree.item(item_id, "values")
            if not values:
                continue
            try:
                path = Path(values[7])
            except Exception:
                continue
            if path.exists() and path.is_file():
                results.append(path)

        return results

    def _unique_destination(self, path: Path) -> Path:
        if not path.exists():
            return path

        stem = path.stem
        suffix = path.suffix
        parent = path.parent

        index = 2
        while True:
            candidate = parent / f"{stem}_v{index}{suffix}"
            if not candidate.exists():
                return candidate
            index += 1

    def _unique_arcname(self, base_arcname: str, used_names: set[str]) -> str:
        if base_arcname not in used_names:
            return base_arcname

        path = Path(base_arcname)
        parent = path.parent
        stem = path.stem
        suffix = path.suffix

        index = 2
        while True:
            candidate_name = f"{stem}_v{index}{suffix}"
            candidate = str(parent / candidate_name).replace("\\", "/")
            if candidate not in used_names:
                return candidate
            index += 1

    def _zip_arcname_for_item(self, item_id: str) -> tuple[Path | None, str | None]:
        values = self.tree.item(item_id, "values")
        if not values:
            return None, None

        try:
            src = Path(values[6])
        except Exception:
            return None, None

        if not src.exists() or not src.is_file():
            return None, None

        field_code = str(values[1]).strip() or "ETC"
        year = str(values[2]).strip() or "UnknownYear"
        arcname = str(Path(field_code) / year / src.name).replace("\\", "/")
        return src, arcname

    def collect_selected_files(self) -> None:
        paths = self._selected_paths()
        if not paths:
            messagebox.showinfo(
                "안내", "먼저 검색 결과 왼쪽 체크칸으로 하나 이상의 파일을 선택해 주세요."
            )
            return

        target_dir = filedialog.askdirectory(title="파일을 모을 대상 폴더 선택")
        if not target_dir:
            return

        target_path = Path(target_dir).expanduser().resolve()
        target_path.mkdir(parents=True, exist_ok=True)

        copied = 0
        failed = 0

        for src in paths:
            try:
                dst = self._unique_destination(target_path / src.name)
                shutil.copy2(src, dst)
                copied += 1
            except Exception as exc:
                failed += 1
                self.append_log(f"[WARN] 파일 복사 실패: {src} | {exc}")

        self.append_log(f"[GUI] 선택 파일 모으기 완료: 성공 {copied}건, 실패 {failed}건")
        self.set_status(f"선택 파일 모으기 완료: 성공 {copied}건, 실패 {failed}건")
        messagebox.showinfo("완료", f"파일 복사 완료\n성공: {copied}건\n실패: {failed}건")

    def delete_checked_files(self) -> None:
        selected = self._checked_item_ids()
        if not selected:
            messagebox.showinfo(
                "안내", "먼저 검색 결과 왼쪽 체크칸으로 하나 이상의 파일을 선택해 주세요."
            )
            return

        confirmed = messagebox.askyesno(
            "확인",
            "체크한 정리본 파일과 DB 인덱스 항목을 삭제합니다.\n원본 파일은 삭제하지 않습니다.\n계속하시겠습니까?",
        )
        if not confirmed:
            return

        output = self.output_var.get().strip()
        if not output:
            messagebox.showwarning("경고", "출력 폴더를 지정해 주세요.")
            return

        db_path = Path(output).expanduser().resolve() / "LOG" / "paper_index.sqlite3"
        if not db_path.exists():
            messagebox.showwarning("경고", "검색 인덱스가 없습니다.")
            return

        deleted = 0
        failed = 0

        index = PaperIndex(db_path)
        try:
            for item_id in selected:
                values = self.tree.item(item_id, "values")
                if not values:
                    continue

                stored_path = Path(str(values[7]).strip()) if len(values) > 7 and str(values[7]).strip() else None
                original_path = str(values[8]).strip() if len(values) > 8 else ""

                try:
                    if stored_path and stored_path.exists() and stored_path.is_file():
                        stored_path.unlink()

                    if original_path:
                        index.delete_by_original_path(original_path)
                    elif stored_path is not None:
                        cur = index.conn.cursor()
                        cur.execute("DELETE FROM papers WHERE path = ?", (str(stored_path),))
                        index.conn.commit()

                    self.tree.delete(item_id)
                    self.snippets.pop(item_id, None)
                    self.checked_items.discard(item_id)
                    deleted += 1
                except Exception as exc:
                    failed += 1
                    self.append_log(f"[WARN] 선택 파일 삭제 실패: {stored_path} | {exc}")
        finally:
            index.close()

        self._refresh_dashboard_from_tree()
        if self.tree.get_children():
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._on_tree_select(None)
        else:
            self._set_text_widget(self.snippet_text, "")

        self.append_log(f"[GUI] 선택 파일 지우기 완료: 성공 {deleted}건, 실패 {failed}건")
        self.set_status(f"선택 파일 지우기 완료: 성공 {deleted}건, 실패 {failed}건")
        messagebox.showinfo("완료", f"선택 파일 삭제 완료\n성공: {deleted}건\n실패: {failed}건")

    def export_selected_zip(self) -> None:
        selected = self._checked_item_ids()
        if not selected:
            messagebox.showinfo(
                "안내", "먼저 검색 결과 왼쪽 체크칸으로 하나 이상의 파일을 선택해 주세요."
            )
            return

        zip_path = filedialog.asksaveasfilename(
            title="ZIP 파일 저장",
            defaultextension=".zip",
            filetypes=[("ZIP files", "*.zip")],
            initialfile="selected_papers.zip",
        )
        if not zip_path:
            return

        zip_file = Path(zip_path).expanduser().resolve()

        added = 0
        failed = 0
        used_names: set[str] = set()

        try:
            with zipfile.ZipFile(zip_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for item_id in selected:
                    src, base_arcname = self._zip_arcname_for_item(item_id)
                    if src is None or base_arcname is None:
                        failed += 1
                        continue

                    try:
                        arcname = self._unique_arcname(base_arcname, used_names)
                        zf.write(src, arcname=arcname)
                        used_names.add(arcname)
                        added += 1
                    except Exception as exc:
                        failed += 1
                        self.append_log(f"[WARN] ZIP 추가 실패: {src} | {exc}")

            self.append_log(
                f"[GUI] ZIP 생성 완료: {zip_file} | 성공 {added}건, 실패 {failed}건"
            )
            self.set_status(f"ZIP 생성 완료: 성공 {added}건, 실패 {failed}건")
            messagebox.showinfo(
                "완료",
                f"ZIP 생성 완료\n성공: {added}건\n실패: {failed}건\n\n구조: 분야/연도/파일명.pdf",
            )
        except Exception as exc:
            self.append_log(f"[ERROR] ZIP 생성 실패: {exc}")
            self.show_error("오류", f"ZIP 생성에 실패했습니다:\n{exc}")

    def _export_tree_items_to_csv(self, item_ids: list[str], default_filename: str) -> None:
        if not item_ids:
            messagebox.showinfo("안내", "저장할 항목이 없습니다.")
            return

        csv_path = filedialog.asksaveasfilename(
            title="CSV 파일 저장",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=default_filename,
        )
        if not csv_path:
            return

        output_file = Path(csv_path).expanduser().resolve()

        header = [
            "doc_type",
            "field",
            "year",
            "author",
            "venue",
            "title",
            "doi",
            "stored_path",
            "original_path",
        ]

        rows: list[list[str]] = []

        for item_id in item_ids:
            values = self.tree.item(item_id, "values")
            if not values:
                continue

            rows.append(
                [
                    str(values[0]) if len(values) > 0 else "",
                    str(values[1]) if len(values) > 1 else "",
                    str(values[2]) if len(values) > 2 else "",
                    str(values[3]) if len(values) > 3 else "",
                    str(values[4]) if len(values) > 4 else "",
                    str(values[5]) if len(values) > 5 else "",
                    str(values[6]) if len(values) > 6 else "",
                    str(values[7]) if len(values) > 7 else "",
                    str(values[8]) if len(values) > 8 else "",
                ]
            )

        try:
            with output_file.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)

            self.append_log(f"[GUI] CSV 저장 완료: {output_file} | {len(rows)}건")
            self.set_status(f"CSV 저장 완료: {len(rows)}건")
            messagebox.showinfo(
                "완료", f"CSV 저장 완료\n파일: {output_file}\n건수: {len(rows)}"
            )
        except Exception as exc:
            self.append_log(f"[ERROR] CSV 저장 실패: {exc}")
            self.show_error("오류", f"CSV 저장에 실패했습니다:\n{exc}")

    def export_selected_csv(self) -> None:
        selected = self._checked_item_ids()
        if not selected:
            messagebox.showinfo(
                "안내", "먼저 검색 결과 왼쪽 체크칸으로 하나 이상의 항목을 선택해 주세요."
            )
            return

        self._export_tree_items_to_csv(selected, "selected_papers.csv")

    def export_all_results_csv(self) -> None:
        item_ids = list(self.tree.get_children())
        if not item_ids:
            messagebox.showinfo("안내", "현재 검색 결과가 없습니다.")
            return

        self._export_tree_items_to_csv(item_ids, "search_results.csv")

    def _normalize_doi_url(self, doi: str) -> str:
        doi = doi.strip()
        if doi.startswith("http://") or doi.startswith("https://"):
            return doi
        if doi.lower().startswith("doi:"):
            doi = doi[4:].strip()
        return f"https://doi.org/{doi}"

    def open_selected_file(self) -> None:
        path = self._selected_path()
        if not path:
            messagebox.showinfo("안내", "먼저 검색 결과에서 파일을 선택해 주세요.")
            return
        if not path.exists():
            messagebox.showwarning("경고", "정리본 파일이 존재하지 않습니다.")
            return
        self._open_path(path)

    def open_selected_folder(self) -> None:
        path = self._selected_path()
        if not path:
            messagebox.showinfo("안내", "먼저 검색 결과에서 파일을 선택해 주세요.")
            return
        target = path.parent
        if not target.exists():
            messagebox.showwarning("경고", f"정리본 폴더가 존재하지 않습니다: {target}")
            return
        self._open_path(target)

    def open_selected_original_file(self) -> None:
        path = self._selected_original_path()
        if not path:
            messagebox.showinfo("안내", "선택한 항목에 원본 경로 정보가 없습니다.")
            return
        if not path.exists():
            messagebox.showwarning("경고", f"원본 파일이 존재하지 않습니다:\n{path}")
            return
        self._open_path(path)

    def open_selected_original_folder(self) -> None:
        path = self._selected_original_path()
        if not path:
            messagebox.showinfo("안내", "선택한 항목에 원본 경로 정보가 없습니다.")
            return
        target = path.parent
        if not target.exists():
            messagebox.showwarning("경고", f"원본 폴더가 존재하지 않습니다:\n{target}")
            return
        self._open_path(target)

    def open_selected_doi(self) -> None:
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("안내", "먼저 검색 결과를 선택해 주세요.")
            return

        doi = self._selected_doi()
        if not doi:
            messagebox.showinfo("안내", "선택한 항목에 DOI 정보가 없습니다.")
            return

        try:
            webbrowser.open(self._normalize_doi_url(doi))
        except Exception as exc:
            messagebox.showerror("오류", f"DOI 열기에 실패했습니다: {exc}")

    def _checked_titles(self) -> list[str]:
        titles: list[str] = []
        seen: set[str] = set()

        for item_id in self._checked_item_ids():
            values = self.tree.item(item_id, "values")
            if not values:
                continue
            title = str(values[5]).strip()
            if not title:
                continue
            key = title.casefold()
            if key in seen:
                continue
            seen.add(key)
            titles.append(title)

        return titles

    def open_checked_pubmed(self) -> None:
        titles = self._checked_titles()
        if not titles:
            messagebox.showinfo("안내", "먼저 체크할 파일을 선택해 주세요.")
            return

        email = self.crossref_email_var.get().strip()
        opened = 0
        matched = 0

        for title in titles:
            result = search_pubmed_by_title(title, email=email)
            url = result.get("url", "").strip()
            if not url:
                cleaned_title = re.sub(r"<[^>]+>", "", title or "")
                cleaned_title = re.sub(r"\s+", " ", cleaned_title).strip()
                url = f"https://pubmed.ncbi.nlm.nih.gov/?term={quote_plus(cleaned_title)}&sort=jour&sort_order=asc"

            try:
                webbrowser.open(url)
                opened += 1
                if result.get("matched") == "true":
                    matched += 1
                    self.append_log(
                        f"[PUBMED] 직접 매칭: {title} -> PMID {result.get('pmid', '').strip()}"
                    )
                else:
                    if result.get("error"):
                        self.append_log(
                            f"[PUBMED] 검색 결과 페이지 열기(오류 fallback): {title} | {result.get('error')}"
                        )
                    else:
                        self.append_log(f"[PUBMED] 검색 결과 페이지 열기: {title}")
            except Exception as exc:
                self.append_log(f"[PUBMED-ERROR] {title}: {exc}")

        self.set_status(
            f"PubMed 열기 완료: {opened}건 열기, 직접 매칭 {matched}건"
        )

    def get_db_path(self) -> Path | None:
        output_dir = self.output_var.get().strip()
        if not output_dir:
            return None
        return Path(output_dir).resolve() / "LOG" / "paper_index.sqlite3"

    def open_db_folder(self) -> None:
        db_path = self.get_db_path()
        if db_path is None:
            messagebox.showwarning("경고", "출력 폴더가 설정되지 않았습니다.")
            return
        self._open_path(db_path.parent)

    def open_output_folder(self) -> None:
        output_dir = self.output_var.get().strip()
        if not output_dir:
            messagebox.showwarning("경고", "출력 폴더가 설정되지 않았습니다.")
            return
        self._open_path(Path(output_dir))

    def _open_path(self, path: Path) -> None:
        try:
            if not path.exists():
                messagebox.showwarning("경고", f"경로가 존재하지 않습니다:\n{path}")
                return

            if os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path)])
            else:
                subprocess.Popen(["xdg-open", str(path)])
        except Exception as exc:
            messagebox.showerror("오류", f"열기에 실패했습니다: {exc}")

    def _on_tree_double_click(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        column = self.tree.identify_column(event.x)
        try:
            column_index = int(column.replace("#", "")) - 1
        except ValueError:
            self.open_selected_file()
            return

        if (
            0 <= column_index < len(self.COLUMNS)
            and self.COLUMNS[column_index] == "doi"
        ):
            self.open_selected_doi()
        else:
            self.open_selected_file()

    def _column_index(self, column: str) -> int:
        return self.COLUMNS.index(column)

    def _refresh_heading_arrows(self) -> None:
        for col in self.COLUMNS:
            heading = self.base_headings[col]
            if col == self.current_sort_column:
                reverse = self.sort_state.get(col, False)
                arrow = "▼" if reverse else "▲"
                heading = f"{heading} {arrow}"
            self.tree.heading(
                col, text=heading, command=lambda c=col: self.sort_tree_by(c)
            )

    def sort_tree_by(self, column: str) -> None:
        items = list(self.tree.get_children())
        if not items:
            return

        reverse = self.sort_state.get(column, False)

        def sort_key(item_id: str):
            values = self.tree.item(item_id, "values")
            value = values[self._column_index(column)] if values else ""

            if column == "year":
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return -1

            return str(value).casefold()

        items.sort(key=sort_key, reverse=reverse)

        for index, item_id in enumerate(items):
            self.tree.move(item_id, "", index)

        self.current_sort_column = column
        self.sort_state[column] = not reverse
        self._refresh_heading_arrows()

    def on_close(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            if not messagebox.askyesno(
                "종료 확인", "작업이 실행 중입니다. 그래도 종료하시겠습니까?"
            ):
                return

        self.append_log("[GUI] 애플리케이션 종료")
        self.save_settings()
        self.stop_watch()
        self.root.destroy()


def main() -> int:
    root = tk.Tk()
    style = ttk.Style()
    if "clam" in style.theme_names():
        style.theme_use("clam")
    OrganizerGUI(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())