from __future__ import annotations

import json
import os
import subprocess
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from types import SimpleNamespace

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from watchdog.observers import Observer

from .core import (
    FIELD_CODES,
    PDFCreatedHandler,
    PaperIndex,
    PaperOrganizer,
    run_reindex,
    scan_existing_pdfs,
)


@dataclass
class AppConfig:
    watch_dir: str = ""
    output_dir: str = ""
    crossref_mailto: str = "your-email@example.com"
    crossref_cache_days: int = 180

    @classmethod
    def load(cls, path: Path) -> "AppConfig":
        if not path.exists():
            return cls()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return cls(
                watch_dir=str(data.get("watch_dir", "")).strip(),
                output_dir=str(data.get("output_dir", "")).strip(),
                crossref_mailto=str(data.get("crossref_mailto", "your-email@example.com")).strip() or "your-email@example.com",
                crossref_cache_days=int(data.get("crossref_cache_days", 180)),
            )
        except Exception:
            return cls()

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(asdict(self), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


class PaperOrganizerApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Academic Paper Organizer")
        self.geometry("1400x860")
        self.minsize(1200, 760)

        self.config_path = Path("app_config.json").resolve()
        self.app_config = AppConfig.load(self.config_path)

        self.observer: Observer | None = None
        self.organizer: PaperOrganizer | None = None
        self.worker_thread: threading.Thread | None = None
        self.cancel_event = threading.Event()

        self.watch_dir_var = tk.StringVar(value=self.app_config.watch_dir)
        self.output_dir_var = tk.StringVar(value=self.app_config.output_dir)
        self.crossref_email_var = tk.StringVar(value=self.app_config.crossref_mailto)
        self.crossref_cache_days_var = tk.StringVar(value=str(self.app_config.crossref_cache_days))

        self.search_keyword_var = tk.StringVar()
        self.search_author_var = tk.StringVar()
        self.search_year_var = tk.StringVar()
        self.search_field_var = tk.StringVar()
        self.search_venue_var = tk.StringVar()

        self.status_var = tk.StringVar(value="대기 중")

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        top = ttk.Frame(self, padding=10)
        top.grid(row=0, column=0, sticky="nsew")
        top.columnconfigure(1, weight=1)
        top.columnconfigure(4, weight=1)

        ttk.Label(top, text="감시 폴더").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(top, textvariable=self.watch_dir_var).grid(row=0, column=1, sticky="ew", padx=4, pady=4)
        ttk.Button(top, text="찾기", command=self.choose_watch_dir).grid(row=0, column=2, sticky="ew", padx=4, pady=4)

        ttk.Label(top, text="출력 폴더").grid(row=0, column=3, sticky="w", padx=4, pady=4)
        ttk.Entry(top, textvariable=self.output_dir_var).grid(row=0, column=4, sticky="ew", padx=4, pady=4)
        ttk.Button(top, text="찾기", command=self.choose_output_dir).grid(row=0, column=5, sticky="ew", padx=4, pady=4)

        ttk.Label(top, text="Crossref email").grid(row=1, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(top, textvariable=self.crossref_email_var).grid(row=1, column=1, sticky="ew", padx=4, pady=4)

        ttk.Label(top, text="캐시 보관일").grid(row=1, column=3, sticky="w", padx=4, pady=4)
        ttk.Entry(top, textvariable=self.crossref_cache_days_var, width=12).grid(row=1, column=4, sticky="w", padx=4, pady=4)

        ttk.Button(top, text="설정 저장", command=self.save_settings).grid(row=1, column=5, sticky="ew", padx=4, pady=4)

        buttons = ttk.Frame(self, padding=(10, 0, 10, 10))
        buttons.grid(row=1, column=0, sticky="ew")
        for i in range(9):
            buttons.columnconfigure(i, weight=1)

        ttk.Button(buttons, text="감시 시작", command=self.start_watch).grid(row=0, column=0, sticky="ew", padx=4, pady=4)
        ttk.Button(buttons, text="감시 중지", command=self.stop_watch).grid(row=0, column=1, sticky="ew", padx=4, pady=4)
        ttk.Button(buttons, text="기존 PDF 스캔", command=self.scan_now).grid(row=0, column=2, sticky="ew", padx=4, pady=4)
        ttk.Button(buttons, text="재인덱싱", command=self.reindex_now).grid(row=0, column=3, sticky="ew", padx=4, pady=4)
        ttk.Button(buttons, text="작업 취소", command=self.cancel_current_job).grid(row=0, column=4, sticky="ew", padx=4, pady=4)
        ttk.Button(buttons, text="캐시 삭제", command=self.clear_crossref_cache).grid(row=0, column=5, sticky="ew", padx=4, pady=4)
        ttk.Button(buttons, text="DB 열기", command=self.open_db_folder).grid(row=0, column=6, sticky="ew", padx=4, pady=4)
        ttk.Button(buttons, text="출력 폴더 열기", command=self.open_output_folder).grid(row=0, column=7, sticky="ew", padx=4, pady=4)
        ttk.Label(buttons, textvariable=self.status_var, anchor="e").grid(row=0, column=8, sticky="ew", padx=8, pady=4)

        body = ttk.Panedwindow(self, orient=tk.VERTICAL)
        body.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))

        upper = ttk.Frame(body)
        lower = ttk.Frame(body)
        body.add(upper, weight=3)
        body.add(lower, weight=2)

        upper.columnconfigure(0, weight=1)
        upper.rowconfigure(1, weight=1)

        search_frame = ttk.LabelFrame(upper, text="검색", padding=8)
        search_frame.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for i in range(10):
            search_frame.columnconfigure(i, weight=1)

        ttk.Label(search_frame, text="키워드").grid(row=0, column=0, sticky="w", padx=4, pady=4)
        ttk.Entry(search_frame, textvariable=self.search_keyword_var).grid(row=0, column=1, sticky="ew", padx=4, pady=4)

        ttk.Label(search_frame, text="저자").grid(row=0, column=2, sticky="w", padx=4, pady=4)
        ttk.Entry(search_frame, textvariable=self.search_author_var).grid(row=0, column=3, sticky="ew", padx=4, pady=4)

        ttk.Label(search_frame, text="연도").grid(row=0, column=4, sticky="w", padx=4, pady=4)
        ttk.Entry(search_frame, textvariable=self.search_year_var, width=10).grid(row=0, column=5, sticky="ew", padx=4, pady=4)

        ttk.Label(search_frame, text="분야").grid(row=0, column=6, sticky="w", padx=4, pady=4)
        field_values = [""] + list(FIELD_CODES.keys())
        ttk.Combobox(
            search_frame,
            textvariable=self.search_field_var,
            values=field_values,
            state="readonly",
        ).grid(row=0, column=7, sticky="ew", padx=4, pady=4)

        ttk.Label(search_frame, text="Venue").grid(row=0, column=8, sticky="w", padx=4, pady=4)
        ttk.Entry(search_frame, textvariable=self.search_venue_var).grid(row=0, column=9, sticky="ew", padx=4, pady=4)

        ttk.Button(search_frame, text="검색", command=self.search_index).grid(row=1, column=8, sticky="ew", padx=4, pady=4)
        ttk.Button(search_frame, text="초기화", command=self.clear_search).grid(row=1, column=9, sticky="ew", padx=4, pady=4)

        result_frame = ttk.LabelFrame(upper, text="검색 결과", padding=8)
        result_frame.grid(row=1, column=0, sticky="nsew")
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)

        columns = ("field_code", "year", "first_author", "venue", "title", "doi", "path")
        self.result_tree = ttk.Treeview(result_frame, columns=columns, show="headings")
        self.result_tree.grid(row=0, column=0, sticky="nsew")
        self.result_tree.bind("<Double-1>", self.on_result_double_click)

        headings = {
            "field_code": "분야",
            "year": "연도",
            "first_author": "첫 저자",
            "venue": "Venue",
            "title": "제목",
            "doi": "DOI",
            "path": "저장 경로",
        }
        widths = {
            "field_code": 80,
            "year": 70,
            "first_author": 130,
            "venue": 140,
            "title": 420,
            "doi": 180,
            "path": 320,
        }

        for key in columns:
            self.result_tree.heading(key, text=headings[key])
            self.result_tree.column(key, width=widths[key], anchor="w")

        yscroll = ttk.Scrollbar(result_frame, orient="vertical", command=self.result_tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.result_tree.configure(yscrollcommand=yscroll.set)

        lower.columnconfigure(0, weight=1)
        lower.rowconfigure(0, weight=1)

        log_frame = ttk.LabelFrame(lower, text="로그", padding=8)
        log_frame.grid(row=0, column=0, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, wrap="word", height=12)
        self.log_text.grid(row=0, column=0, sticky="nsew")

        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def log(self, message: str) -> None:
        def _append() -> None:
            self.log_text.insert("end", message + "\n")
            self.log_text.see("end")
            self.status_var.set(message)

        self.after(0, _append)

    def choose_watch_dir(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.watch_dir_var.set(path)

    def choose_output_dir(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.output_dir_var.set(path)

    def save_settings(self) -> None:
        try:
            cache_days = int(self.crossref_cache_days_var.get().strip())
            if cache_days < 1:
                raise ValueError("캐시 보관일은 1 이상이어야 합니다.")

            self.app_config.watch_dir = self.watch_dir_var.get().strip()
            self.app_config.output_dir = self.output_dir_var.get().strip()
            self.app_config.crossref_mailto = self.crossref_email_var.get().strip() or "your-email@example.com"
            self.app_config.crossref_cache_days = cache_days
            self.app_config.save(self.config_path)

            self.log("[CONFIG] 설정 저장 완료")
            messagebox.showinfo("완료", "설정이 저장되었습니다.")
        except Exception as exc:
            messagebox.showerror("오류", f"설정 저장 실패: {exc}")

    def validate_paths(self) -> bool:
        watch_dir = self.watch_dir_var.get().strip()
        output_dir = self.output_dir_var.get().strip()

        if not watch_dir:
            messagebox.showwarning("경고", "감시 폴더를 선택하세요.")
            return False
        if not output_dir:
            messagebox.showwarning("경고", "출력 폴더를 선택하세요.")
            return False
        if not Path(watch_dir).exists():
            messagebox.showwarning("경고", "감시 폴더가 존재하지 않습니다.")
            return False

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        return True

    def create_organizer(self) -> PaperOrganizer:
        self.cancel_event = threading.Event()
        return PaperOrganizer(
            watch_dir=Path(self.watch_dir_var.get().strip()),
            output_dir=Path(self.output_dir_var.get().strip()),
            log_fn=self.log,
            cancel_event=self.cancel_event,
            crossref_mailto=self.crossref_email_var.get().strip() or "your-email@example.com",
            crossref_cache_days=int(self.crossref_cache_days_var.get().strip() or "180"),
        )

    def start_watch(self) -> None:
        if not self.validate_paths():
            return

        if self.observer is not None:
            messagebox.showinfo("안내", "이미 감시 중입니다.")
            return

        try:
            self.stop_watch(close_message=False)

            self.organizer = self.create_organizer()
            handler = PDFCreatedHandler(self.organizer)

            self.observer = Observer()
            self.observer.schedule(handler, self.watch_dir_var.get().strip(), recursive=True)
            self.observer.start()

            self.log("[WATCH] 감시 시작")
            self.status_var.set("감시 중")
        except Exception as exc:
            self.observer = None
            if self.organizer:
                self.organizer.close()
                self.organizer = None
            messagebox.showerror("오류", f"감시 시작 실패: {exc}")

    def stop_watch(self, close_message: bool = True) -> None:
        try:
            if self.observer is not None:
                self.observer.stop()
                self.observer.join(timeout=3)
                self.observer = None
                self.log("[WATCH] 감시 중지")

            if self.organizer is not None:
                self.organizer.close()
                self.organizer = None

            self.status_var.set("대기 중")
            if close_message:
                pass
        except Exception as exc:
            messagebox.showerror("오류", f"감시 중지 실패: {exc}")

    def cancel_current_job(self) -> None:
        self.cancel_event.set()
        if self.organizer:
            self.organizer.request_cancel()
        self.log("[CANCEL] 취소 요청됨")

    def run_in_thread(self, target, *, name: str) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("안내", "이미 다른 작업이 실행 중입니다.")
            return

        thread = threading.Thread(target=target, name=name, daemon=True)
        self.worker_thread = thread
        thread.start()

    def scan_now(self) -> None:
        if not self.validate_paths():
            return

        def job() -> None:
            organizer: PaperOrganizer | None = None
            try:
                organizer = self.create_organizer()
                self.log("[SCAN] 기존 PDF 스캔 시작")
                scan_existing_pdfs(organizer, Path(self.watch_dir_var.get().strip()), log_fn=self.log)
                self.log("[SCAN] 작업 종료")
                self.after(0, self.search_index)
            except Exception as exc:
                self.log(f"[ERROR] 스캔 실패: {exc}")
            finally:
                if organizer is not None:
                    organizer.close()

        self.run_in_thread(job, name="scan-existing-pdfs")

    def reindex_now(self) -> None:
        if not self.validate_paths():
            return

        def job() -> None:
            try:
                self.cancel_event = threading.Event()
                args = SimpleNamespace(output=self.output_dir_var.get().strip())
                self.log("[REINDEX] 재인덱싱 시작")
                run_reindex(
                    args,
                    log_fn=self.log,
                    cancel_event=self.cancel_event,
                    crossref_cache_days=int(self.crossref_cache_days_var.get().strip() or "180"),
                )
                self.log("[REINDEX] 작업 종료")
                self.after(0, self.search_index)
            except Exception as exc:
                self.log(f"[ERROR] 재인덱싱 실패: {exc}")

        self.run_in_thread(job, name="reindex")

    def clear_crossref_cache(self) -> None:
        try:
            output_dir = self.output_dir_var.get().strip()
            if not output_dir:
                messagebox.showwarning("경고", "출력 폴더를 먼저 설정하세요.")
                return

            db_path = Path(output_dir).resolve() / "LOG" / "paper_index.sqlite3"
            index = PaperIndex(db_path)
            try:
                index.clear_crossref_cache()
            finally:
                index.close()

            self.log("[CACHE] Crossref 캐시 삭제 완료")
            messagebox.showinfo("완료", "Crossref 캐시를 삭제했습니다.")
        except Exception as exc:
            messagebox.showerror("오류", f"캐시 삭제 실패: {exc}")

    def get_db_path(self) -> Path | None:
        output_dir = self.output_dir_var.get().strip()
        if not output_dir:
            return None
        return Path(output_dir).resolve() / "LOG" / "paper_index.sqlite3"

    def search_index(self) -> None:
        db_path = self.get_db_path()
        if db_path is None or not db_path.exists():
            self.log("[WARN] 검색할 DB가 없습니다.")
            self.clear_results()
            return

        try:
            index = PaperIndex(db_path)
            try:
                rows = index.search(
                    keyword=self.search_keyword_var.get().strip(),
                    author=self.search_author_var.get().strip() or None,
                    year=self.search_year_var.get().strip() or None,
                    field_code=self.search_field_var.get().strip() or None,
                    venue=self.search_venue_var.get().strip() or None,
                    limit=300,
                )
            finally:
                index.close()

            self.clear_results()
            for row in rows:
                self.result_tree.insert(
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
                    ),
                    tags=(json.dumps({
                        "original_path": row.original_path,
                        "snippet": row.snippet,
                        "doi": row.doi,
                    }, ensure_ascii=False),),
                )

            self.log(f"[SEARCH] {len(rows)}건 조회됨")
        except Exception as exc:
            self.log(f"[ERROR] 검색 실패: {exc}")

    def clear_results(self) -> None:
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

    def clear_search(self) -> None:
        self.search_keyword_var.set("")
        self.search_author_var.set("")
        self.search_year_var.set("")
        self.search_field_var.set("")
        self.search_venue_var.set("")
        self.search_index()

    def on_result_double_click(self, _event=None) -> None:
        selected = self.result_tree.selection()
        if not selected:
            return

        item_id = selected[0]
        values = self.result_tree.item(item_id, "values")
        if not values:
            return

        stored_path = values[6]
        if stored_path:
            self.open_path(Path(stored_path))

    def open_path(self, path: Path) -> None:
        try:
            if not path.exists():
                messagebox.showwarning("경고", f"파일이 없습니다:\n{path}")
                return

            if os.name == "nt":
                os.startfile(str(path))
            elif os.name == "posix":
                subprocess.run(["xdg-open", str(path)], check=False)
            else:
                messagebox.showinfo("경로", str(path))
        except Exception as exc:
            messagebox.showerror("오류", f"열기 실패: {exc}")

    def open_output_folder(self) -> None:
        output_dir = self.output_dir_var.get().strip()
        if not output_dir:
            messagebox.showwarning("경고", "출력 폴더가 설정되지 않았습니다.")
            return
        self.open_path(Path(output_dir))

    def open_db_folder(self) -> None:
        db_path = self.get_db_path()
        if db_path is None:
            messagebox.showwarning("경고", "출력 폴더가 설정되지 않았습니다.")
            return
        self.open_path(db_path.parent)

    def on_close(self) -> None:
        try:
            self.stop_watch(close_message=False)
        finally:
            self.destroy()


def main() -> None:
    app = PaperOrganizerApp()
    app.mainloop()


if __name__ == "__main__":
    main()