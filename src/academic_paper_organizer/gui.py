from __future__ import annotations

import csv
import os
import queue
import shutil
import subprocess
import threading
import time
import webbrowser
import zipfile
from collections.abc import Callable
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .core import PDFCreatedHandler, PaperIndex, PaperOrganizer, run_reindex, scan_existing_pdfs
from .settings import AppSettings


class FilteringEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        delegate: FileSystemEventHandler,
        allowed_roots: list[Path],
    ):
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
        if self._is_allowed(event.dest_path):
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
    COLUMNS = ("field", "year", "author", "venue", "title", "doi", "path", "original_path")

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Academic Paper Organizer GUI")
        self.root.geometry("1360x800")

        self.watch_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.keyword_var = tk.StringVar()
        self.author_var = tk.StringVar()
        self.year_var = tk.StringVar()
        self.field_var = tk.StringVar()
        self.venue_var = tk.StringVar()
        self.limit_var = tk.StringVar(value="50")
        self.status_var = tk.StringVar(value="대기 중")

        self.recursive_var = tk.BooleanVar(value=True)
        self.watch_mode_var = tk.StringVar(value="all")
        self.selected_subdirs: list[Path] = []
        self.selected_subdirs_label_var = tk.StringVar(value="선택된 하위 폴더 없음")

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.observer: Observer | None = None
        self.organizer: PaperOrganizer | None = None
        self.worker_thread: threading.Thread | None = None
        self.cancel_event = threading.Event()

        self.snippets: dict[str, str] = {}
        self.sort_state: dict[str, bool] = {}
        self.current_sort_column: str | None = None

        self.config_path = Path.home() / ".academic_paper_organizer_gui.json"
        self.gui_log_path: Path | None = None

        self._build_ui()
        self._load_settings()
        self._update_gui_log_path()
        self._update_button_states()
        self._format_selected_subdirs_label()
        self._on_watch_option_changed()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(150, self._drain_log_queue)
        self.append_log("[GUI] 애플리케이션 시작")

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill="both", expand=True)

        top = ttk.LabelFrame(outer, text="기본 설정", padding=10)
        top.pack(fill="x")
        top.columnconfigure(1, weight=1)
        top.columnconfigure(4, weight=1)

        ttk.Label(top, text="감시 폴더").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(top, textvariable=self.watch_var).grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Button(top, text="찾아보기", command=self.pick_watch_dir).grid(row=0, column=2, padx=6, pady=4)

        ttk.Label(top, text="출력 폴더").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(top, textvariable=self.output_var).grid(row=1, column=1, sticky="ew", pady=4)
        ttk.Button(top, text="찾아보기", command=self.pick_output_dir).grid(row=1, column=2, padx=6, pady=4)

        controls = ttk.Frame(top)
        controls.grid(row=0, column=4, rowspan=2, sticky="e")

        self.run_once_btn = ttk.Button(controls, text="1회 처리", command=self.run_once)
        self.run_once_btn.pack(side="left", padx=4)

        self.start_watch_btn = ttk.Button(controls, text="감시 시작", command=self.start_watch)
        self.start_watch_btn.pack(side="left", padx=4)

        self.stop_watch_btn = ttk.Button(controls, text="감시 중지", command=self.stop_watch)
        self.stop_watch_btn.pack(side="left", padx=4)

        self.cancel_btn = ttk.Button(controls, text="작업 취소", command=self.cancel_current_task)
        self.cancel_btn.pack(side="left", padx=4)

        self.reindex_btn = ttk.Button(controls, text="재인덱싱", command=self.reindex)
        self.reindex_btn.pack(side="left", padx=4)

        options_frame = ttk.Frame(top)
        options_frame.grid(row=2, column=0, columnspan=5, sticky="ew", pady=(8, 0))
        options_frame.columnconfigure(4, weight=1)

        ttk.Checkbutton(
            options_frame,
            text="하위 폴더까지 포함",
            variable=self.recursive_var,
            command=self._on_watch_option_changed,
        ).grid(row=0, column=0, sticky="w", padx=(0, 12))

        ttk.Radiobutton(
            options_frame,
            text="전체 감시",
            variable=self.watch_mode_var,
            value="all",
            command=self._on_watch_option_changed,
        ).grid(row=0, column=1, sticky="w", padx=(0, 8))

        ttk.Radiobutton(
            options_frame,
            text="선택한 하위 폴더만 감시",
            variable=self.watch_mode_var,
            value="selected",
            command=self._on_watch_option_changed,
        ).grid(row=0, column=2, sticky="w", padx=(0, 8))

        self.select_subdirs_btn = ttk.Button(
            options_frame,
            text="하위 폴더 선택",
            command=self.pick_subdirs,
        )
        self.select_subdirs_btn.grid(row=0, column=3, sticky="w", padx=(8, 8))

        ttk.Label(options_frame, textvariable=self.selected_subdirs_label_var).grid(row=0, column=4, sticky="ew")

        notebook = ttk.Notebook(outer)
        notebook.pack(fill="both", expand=True, pady=(10, 0))

        search_tab = ttk.Frame(notebook, padding=10)
        log_tab = ttk.Frame(notebook, padding=10)
        notebook.add(search_tab, text="검색")
        notebook.add(log_tab, text="로그")

        self._build_search_tab(search_tab)
        self._build_log_tab(log_tab)

        status_bar = ttk.Label(outer, textvariable=self.status_var, anchor="w")
        status_bar.pack(fill="x", pady=(8, 0))

    def _build_search_tab(self, parent: ttk.Frame) -> None:
        filters = ttk.LabelFrame(parent, text="검색 조건", padding=10)
        filters.pack(fill="x")
        for i in range(6):
            filters.columnconfigure(i, weight=1 if i in (1, 3, 5) else 0)

        ttk.Label(filters, text="키워드").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        keyword_entry = ttk.Entry(filters, textvariable=self.keyword_var)
        keyword_entry.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(filters, text="저자").grid(row=0, column=2, sticky="w", padx=(16, 6), pady=4)
        author_entry = ttk.Entry(filters, textvariable=self.author_var)
        author_entry.grid(row=0, column=3, sticky="ew", pady=4)

        ttk.Label(filters, text="연도").grid(row=0, column=4, sticky="w", padx=(16, 6), pady=4)
        year_entry = ttk.Entry(filters, textvariable=self.year_var, width=10)
        year_entry.grid(row=0, column=5, sticky="ew", pady=4)

        ttk.Label(filters, text="분야").grid(row=1, column=0, sticky="w", padx=(0, 6), pady=4)
        field_entry = ttk.Entry(filters, textvariable=self.field_var)
        field_entry.grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(filters, text="저널/학회").grid(row=1, column=2, sticky="w", padx=(16, 6), pady=4)
        venue_entry = ttk.Entry(filters, textvariable=self.venue_var)
        venue_entry.grid(row=1, column=3, sticky="ew", pady=4)

        ttk.Label(filters, text="최대 건수").grid(row=1, column=4, sticky="w", padx=(16, 6), pady=4)
        limit_entry = ttk.Entry(filters, textvariable=self.limit_var, width=10)
        limit_entry.grid(row=1, column=5, sticky="ew", pady=4)

        for entry in [keyword_entry, author_entry, year_entry, field_entry, venue_entry, limit_entry]:
            entry.bind("<Return>", lambda event: self.search())

        action_row = ttk.Frame(parent)
        action_row.pack(fill="x", pady=(8, 8))
        ttk.Button(action_row, text="검색", command=self.search).pack(side="left")
        ttk.Button(action_row, text="필터 초기화", command=self.clear_filters).pack(side="left", padx=6)
        ttk.Button(action_row, text="정리본 파일 열기", command=self.open_selected_file).pack(side="left", padx=(24, 6))
        ttk.Button(action_row, text="정리본 폴더 열기", command=self.open_selected_folder).pack(side="left", padx=6)
        ttk.Button(action_row, text="원본 파일 열기", command=self.open_selected_original_file).pack(side="left", padx=(24, 6))
        ttk.Button(action_row, text="원본 폴더 열기", command=self.open_selected_original_folder).pack(side="left", padx=6)
        ttk.Button(action_row, text="DOI 열기", command=self.open_selected_doi).pack(side="left", padx=6)
        ttk.Button(action_row, text="선택 파일 모으기", command=self.collect_selected_files).pack(side="left", padx=(24, 6))
        ttk.Button(action_row, text="선택 파일 ZIP", command=self.export_selected_zip).pack(side="left", padx=6)
        ttk.Button(action_row, text="선택 목록 CSV 저장", command=self.export_selected_csv).pack(side="left", padx=6)
        ttk.Button(action_row, text="검색 결과 전체 CSV 저장", command=self.export_all_results_csv).pack(side="left", padx=6)

        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=self.COLUMNS,
            show="headings",
            height=18,
            selectmode="extended",
        )

        self.base_headings = {
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
            "field": 70,
            "year": 70,
            "author": 100,
            "venue": 160,
            "title": 260,
            "doi": 180,
            "path": 260,
            "original_path": 320,
        }

        for col in self.COLUMNS:
            self.tree.heading(col, text=self.base_headings[col], command=lambda c=col: self.sort_tree_by(c))
            self.tree.column(col, width=widths[col], anchor="w")

        self.tree.pack(side="left", fill="both", expand=True)
        self.tree.bind("<Double-1>", self._on_tree_double_click)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        scrollbar.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=scrollbar.set)

        snippet_box = ttk.LabelFrame(parent, text="미리보기", padding=8)
        snippet_box.pack(fill="both", expand=False, pady=(8, 0))

        self.snippet_text = tk.Text(snippet_box, height=8, wrap="word", state="disabled")
        self.snippet_text.pack(fill="both", expand=True)

    def _build_log_tab(self, parent: ttk.Frame) -> None:
        self.log_text = tk.Text(parent, wrap="word", state="disabled")
        self.log_text.pack(fill="both", expand=True)

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

    def _update_button_states(self) -> None:
        watching = self.observer is not None and self.observer.is_alive()
        worker_running = self.worker_thread is not None and self.worker_thread.is_alive()
        busy = watching or worker_running

        self.start_watch_btn.configure(state="disabled" if busy else "normal")
        self.stop_watch_btn.configure(state="normal" if watching else "disabled")
        self.run_once_btn.configure(state="disabled" if busy else "normal")
        self.reindex_btn.configure(state="disabled" if worker_running else "normal")
        self.cancel_btn.configure(state="normal" if busy else "disabled")

    def _run_in_thread(self, target: Callable[[], None], status_text: str) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("안내", "이미 작업이 실행 중입니다.")
            return

        self.cancel_event.clear()
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

        self.append_log("[GUI] 취소 요청됨")
        self.set_status("취소 요청됨")
        self._update_button_states()

    def _load_settings(self) -> None:
        try:
            settings = AppSettings.load(self.config_path)
            self.watch_var.set(settings.watch_dir)
            self.output_var.set(settings.output_dir)
            self.limit_var.set(settings.limit)
            self.keyword_var.set(settings.keyword)
            self.author_var.set(settings.author)
            self.year_var.set(settings.year)
            self.field_var.set(settings.field)
            self.venue_var.set(settings.venue)

            recursive = getattr(settings, "recursive", "True")
            watch_mode = getattr(settings, "watch_mode", "all")
            selected_subdirs = getattr(settings, "selected_subdirs", [])

            self.recursive_var.set(str(recursive).lower() == "true")
            self.watch_mode_var.set(watch_mode if watch_mode in ("all", "selected") else "all")
            self.selected_subdirs = [Path(p) for p in selected_subdirs if p]
        except Exception as exc:
            self.append_log(f"[WARN] 설정 불러오기 실패: {exc}")

    def _save_settings(self) -> None:
        settings = AppSettings(
            watch_dir=self.watch_var.get().strip(),
            output_dir=self.output_var.get().strip(),
            limit=self.limit_var.get().strip() or "50",
            keyword=self.keyword_var.get().strip(),
            author=self.author_var.get().strip(),
            year=self.year_var.get().strip(),
            field=self.field_var.get().strip(),
            venue=self.venue_var.get().strip(),
            recursive=str(self.recursive_var.get()),
            watch_mode=self.watch_mode_var.get().strip(),
            selected_subdirs=[str(p) for p in self.selected_subdirs],
        )
        try:
            settings.save(self.config_path)
        except Exception as exc:
            self.append_log(f"[WARN] 설정 저장 실패: {exc}")

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

        ttk.Button(btns, text="전체 선택", command=select_all).pack(side="left")
        ttk.Button(btns, text="선택 해제", command=clear_all).pack(side="left", padx=6)
        ttk.Button(btns, text="확인", command=confirm).pack(side="right")
        ttk.Button(btns, text="취소", command=dialog.destroy).pack(side="right", padx=6)

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
        if self.recursive_var.get() and self.watch_mode_var.get() == "selected" and not effective_roots:
            messagebox.showinfo("안내", "처리할 하위 폴더를 먼저 선택해 주세요.")
            return

        def task() -> None:
            organizer = PaperOrganizer(
                watch_dir=watch_dir,
                output_dir=output_dir,
                log_fn=self.append_log,
                cancel_event=self.cancel_event,
            )
            try:
                self.append_log(f"[GUI] 1회 처리 시작: {watch_dir}")

                if not self.recursive_var.get():
                    scan_existing_pdfs(organizer, watch_dir, log_fn=self.append_log)
                elif self.watch_mode_var.get() == "all":
                    scan_existing_pdfs(organizer, watch_dir, log_fn=self.append_log)
                else:
                    for subdir in effective_roots:
                        if self.cancel_event.is_set():
                            self.append_log("[CANCEL] 선택 하위 폴더 처리 취소됨")
                            break
                        self.append_log(f"[GUI] 선택 하위 폴더 처리: {subdir}")
                        scan_existing_pdfs(organizer, subdir, log_fn=self.append_log)

                if self.cancel_event.is_set():
                    self.append_log("[GUI] 1회 처리 취소")
                    self.set_status("1회 처리 취소")
                else:
                    self.append_log("[GUI] 1회 처리 완료")
                    self.set_status("1회 처리 완료")
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
        if self.recursive_var.get() and self.watch_mode_var.get() == "selected" and not effective_roots:
            messagebox.showinfo("안내", "감시할 하위 폴더를 먼저 선택해 주세요.")
            return

        try:
            self.cancel_event.clear()
            self.organizer = PaperOrganizer(
                watch_dir=watch_dir,
                output_dir=output_dir,
                log_fn=self.append_log,
                cancel_event=self.cancel_event,
            )

            base_handler = PDFCreatedHandler(self.organizer)
            handler = FilteringEventHandler(base_handler, effective_roots)

            self.observer = Observer()
            self.observer.schedule(handler, str(watch_dir), recursive=self.recursive_var.get())
            self.observer.start()

            mode_text = "루트만"
            if self.recursive_var.get() and self.watch_mode_var.get() == "all":
                mode_text = "전체 하위 폴더 포함"
            elif self.recursive_var.get() and self.watch_mode_var.get() == "selected":
                mode_text = f"선택 하위 폴더 {len(effective_roots)}개"

            self.status_var.set(f"감시 중: {watch_dir} ({mode_text})")
            self.append_log(f"[GUI] 감시 시작: {watch_dir} | 모드={mode_text}")
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

            self.status_var.set("감시 중지")
            self.append_log("[GUI] 감시 중지")
        except Exception as exc:
            messagebox.showerror("오류", f"감시 중지 중 오류가 발생했습니다:\n{exc}")
        finally:
            self._update_button_states()

    def reindex(self) -> None:
        output = self.output_var.get().strip()
        if not output:
            messagebox.showwarning("경고", "출력 폴더를 지정해 주세요.")
            return

        output_dir = Path(output).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        def task() -> None:
            class Args:
                pass

            args = Args()
            args.output = str(output_dir)

            self.append_log(f"[GUI] 재인덱싱 시작: {output_dir}")

            try:
                run_reindex(args, log_fn=self.append_log, cancel_event=self.cancel_event)
                if self.cancel_event.is_set():
                    self.append_log("[GUI] 재인덱싱 취소")
                    self.set_status("재인덱싱 취소")
                else:
                    self.append_log("[GUI] 재인덱싱 완료")
                    self.set_status("재인덱싱 완료")
            except Exception as exc:
                self.append_log(f"[ERROR] 재인덱싱 실패: {exc}")
                self.set_status("재인덱싱 실패")
                self.show_error("오류", f"재인덱싱 중 오류가 발생했습니다:\n{exc}")

        self._run_in_thread(task, "재인덱싱 중")

    def _clear_results(self) -> None:
        self.snippets.clear()
        self.sort_state.clear()
        self.current_sort_column = None
        self._refresh_heading_arrows()
        for item in self.tree.get_children():
            self.tree.delete(item)

    def clear_filters(self) -> None:
        self.keyword_var.set("")
        self.author_var.set("")
        self.year_var.set("")
        self.field_var.set("")
        self.venue_var.set("")
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
            messagebox.showwarning("경고", "검색 인덱스가 없습니다. 먼저 PDF를 처리하거나 재인덱싱해 주세요.")
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
                limit=limit,
            )
        except Exception as exc:
            messagebox.showerror("오류", f"검색 중 오류가 발생했습니다:\n{exc}")
            return
        finally:
            index.close()

        self._clear_results()

        for row in rows:
            item_id = self.tree.insert(
                "",
                "end",
                values=(
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

        self.status_var.set(f"검색 완료: {len(rows)}건")
        self.append_log(f"[GUI] 검색 완료: {len(rows)}건")

        if rows:
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._on_tree_select(None)
        else:
            self._set_text_widget(self.snippet_text, "")

    def _on_tree_select(self, event) -> None:
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

    def _selected_path(self) -> Path | None:
        values = self._selected_values()
        if not values:
            return None
        return Path(values[6])

    def _selected_original_path(self) -> Path | None:
        values = self._selected_values()
        if not values:
            return None
        original = str(values[7]).strip()
        if not original:
            return None
        return Path(original)

    def _selected_doi(self) -> str | None:
        values = self._selected_values()
        if not values:
            return None
        doi = str(values[5]).strip()
        return doi or None

    def _selected_paths(self) -> list[Path]:
        selected = self.tree.selection()
        results: list[Path] = []

        for item_id in selected:
            values = self.tree.item(item_id, "values")
            if not values:
                continue
            try:
                path = Path(values[6])
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

        field_code = str(values[0]).strip() or "ETC"
        year = str(values[1]).strip() or "UnknownYear"
        arcname = str(Path(field_code) / year / src.name).replace("\\", "/")
        return src, arcname

    def collect_selected_files(self) -> None:
        paths = self._selected_paths()
        if not paths:
            messagebox.showinfo("안내", "먼저 검색 결과에서 하나 이상의 파일을 선택해 주세요.")
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

    def export_selected_zip(self) -> None:
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("안내", "먼저 검색 결과에서 하나 이상의 파일을 선택해 주세요.")
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

            self.append_log(f"[GUI] ZIP 생성 완료: {zip_file} | 성공 {added}건, 실패 {failed}건")
            self.set_status(f"ZIP 생성 완료: 성공 {added}건, 실패 {failed}건")
            messagebox.showinfo("완료", f"ZIP 생성 완료\n성공: {added}건\n실패: {failed}건\n\n구조: 분야/연도/파일명.pdf")
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

            rows.append([
                str(values[0]) if len(values) > 0 else "",
                str(values[1]) if len(values) > 1 else "",
                str(values[2]) if len(values) > 2 else "",
                str(values[3]) if len(values) > 3 else "",
                str(values[4]) if len(values) > 4 else "",
                str(values[5]) if len(values) > 5 else "",
                str(values[6]) if len(values) > 6 else "",
                str(values[7]) if len(values) > 7 else "",
            ])

        try:
            with output_file.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)

            self.append_log(f"[GUI] CSV 저장 완료: {output_file} | {len(rows)}건")
            self.set_status(f"CSV 저장 완료: {len(rows)}건")
            messagebox.showinfo("완료", f"CSV 저장 완료\n파일: {output_file}\n건수: {len(rows)}")
        except Exception as exc:
            self.append_log(f"[ERROR] CSV 저장 실패: {exc}")
            self.show_error("오류", f"CSV 저장에 실패했습니다:\n{exc}")

    def export_selected_csv(self) -> None:
        selected = list(self.tree.selection())
        if not selected:
            messagebox.showinfo("안내", "먼저 검색 결과에서 하나 이상의 항목을 선택해 주세요.")
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

    def _open_path(self, path: Path) -> None:
        try:
            if os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif os.name == "posix":
                subprocess.Popen(["xdg-open", str(path)])
            else:
                subprocess.Popen(["open", str(path)])
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

        if 0 <= column_index < len(self.COLUMNS) and self.COLUMNS[column_index] == "doi":
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
            self.tree.heading(col, text=heading, command=lambda c=col: self.sort_tree_by(c))

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
            if not messagebox.askyesno("종료 확인", "작업이 실행 중입니다. 그래도 종료하시겠습니까?"):
                return

        self.append_log("[GUI] 애플리케이션 종료")
        self._save_settings()
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