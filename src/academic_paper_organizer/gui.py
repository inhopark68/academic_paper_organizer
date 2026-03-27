
from __future__ import annotations

import csv
import queue
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    from .professor_core import (
        ProfessorSourceRow,
        ProfessorResultRow,
        create_professor_source_registry_template,
        export_latest_yonsei_professors_csv,
        export_professor_achievements_csv,
        export_professor_results_csv,
        fetch_professors_from_registered_sources,
        load_professor_source_registry,
        save_professor_source_registry,
    )
except Exception:
    from professor_core import (
        ProfessorSourceRow,
        ProfessorResultRow,
        create_professor_source_registry_template,
        export_latest_yonsei_professors_csv,
        export_professor_achievements_csv,
        export_professor_results_csv,
        fetch_professors_from_registered_sources,
        load_professor_source_registry,
        save_professor_source_registry,
    )


GROUPS = ["전체", "기초의학", "임상의학", "인문의학"]
PAGE_TYPES = ["department_list", "professor_profile"]


@dataclass
class AppConfig:
    output_dir: str = ""
    registry_csv: str = ""
    professors_csv: str = ""
    achievements_email: str = ""
    per_professor_limit: str = "20"

    @classmethod
    def load(cls, path: Path) -> "AppConfig":
        if not path.exists():
            return cls()
        try:
            import json
            data = json.loads(path.read_text(encoding="utf-8"))
            return cls(
                output_dir=str(data.get("output_dir", "")).strip(),
                registry_csv=str(data.get("registry_csv", "")).strip(),
                professors_csv=str(data.get("professors_csv", "")).strip(),
                achievements_email=str(data.get("achievements_email", "")).strip(),
                per_professor_limit=str(data.get("per_professor_limit", "20")).strip() or "20",
            )
        except Exception:
            return cls()

    def save(self, path: Path) -> None:
        import json
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2), encoding="utf-8")


class ProfessorManagerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("교수성과 관리 시스템")
        self.root.geometry("1420x880")
        self.root.minsize(1200, 760)

        self.config_path = Path.home() / ".professor_manager_gui.json"
        self.config = AppConfig.load(self.config_path)

        self.output_dir_var = tk.StringVar(value=self.config.output_dir)
        self.registry_csv_var = tk.StringVar(value=self.config.registry_csv)
        self.professors_csv_var = tk.StringVar(value=self.config.professors_csv)
        self.collect_group_var = tk.StringVar(value="전체")
        self.ach_group_var = tk.StringVar(value="전체")
        self.email_var = tk.StringVar(value=self.config.achievements_email)
        self.limit_var = tk.StringVar(value=self.config.per_professor_limit)
        self.status_var = tk.StringVar(value="대기 중")

        self.src_id_var = tk.StringVar(value="")
        self.src_group_var = tk.StringVar(value="기초의학")
        self.src_dept_ko_var = tk.StringVar(value="")
        self.src_dept_en_var = tk.StringVar(value="")
        self.src_page_type_var = tk.StringVar(value="department_list")
        self.src_url_ko_var = tk.StringVar(value="")
        self.src_url_en_var = tk.StringVar(value="")
        self.src_active_var = tk.StringVar(value="Y")
        self.src_note_var = tk.StringVar(value="")

        self.registry_rows: list[ProfessorSourceRow] = []
        self.collected_rows: list[ProfessorResultRow] = []
        self.log_queue: queue.Queue[str] = queue.Queue()

        self._build_ui()
        self.root.after(120, self._drain_log_queue)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        if self.registry_csv_var.get():
            self._try_load_registry(self.registry_csv_var.get())
        self.append_log("[APP] 교수성과 관리 시스템 시작")

    def _build_ui(self) -> None:
        style = ttk.Style()
        if "clam" in style.theme_names():
            style.theme_use("clam")

        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill="both", expand=True)

        top = ttk.LabelFrame(outer, text="기본 경로", padding=10)
        top.pack(fill="x")
        top.columnconfigure(1, weight=1)
        top.columnconfigure(3, weight=1)

        ttk.Label(top, text="작업 폴더").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(top, textvariable=self.output_dir_var).grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Button(top, text="찾아보기", command=self.pick_output_dir).grid(row=0, column=2, padx=6, pady=4)
        ttk.Button(top, text="설정 저장", command=self.save_settings).grid(row=0, column=4, padx=6, pady=4)

        ttk.Label(top, text="등록 CSV").grid(row=1, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(top, textvariable=self.registry_csv_var).grid(row=1, column=1, sticky="ew", pady=4)
        ttk.Button(top, text="열기", command=self.load_registry_csv_dialog).grid(row=1, column=2, padx=6, pady=4)
        ttk.Button(top, text="템플릿 생성", command=self.create_registry_template).grid(row=1, column=3, padx=6, pady=4, sticky="w")

        ttk.Label(top, text="교수명단 CSV").grid(row=2, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Entry(top, textvariable=self.professors_csv_var).grid(row=2, column=1, sticky="ew", pady=4)
        ttk.Button(top, text="열기", command=self.pick_professors_csv).grid(row=2, column=2, padx=6, pady=4)

        ttk.Label(outer, textvariable=self.status_var).pack(fill="x", pady=(6, 6))

        notebook = ttk.Notebook(outer)
        notebook.pack(fill="both", expand=True)

        registry_tab = ttk.Frame(notebook, padding=10)
        collect_tab = ttk.Frame(notebook, padding=10)
        achievements_tab = ttk.Frame(notebook, padding=10)
        log_tab = ttk.Frame(notebook, padding=10)
        notebook.add(registry_tab, text="URL 등록")
        notebook.add(collect_tab, text="교수정보 수집")
        notebook.add(achievements_tab, text="교수성과")
        notebook.add(log_tab, text="로그")

        self._build_registry_tab(registry_tab)
        self._build_collect_tab(collect_tab)
        self._build_achievements_tab(achievements_tab)
        self._build_log_tab(log_tab)

    def _build_registry_tab(self, parent: ttk.Frame) -> None:
        form = ttk.LabelFrame(parent, text="교수정보 URL 등록", padding=10)
        form.pack(fill="x")
        form.columnconfigure(1, weight=1)
        form.columnconfigure(3, weight=1)

        ttk.Label(form, text="ID").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(form, textvariable=self.src_id_var, width=12).grid(row=0, column=1, sticky="w", pady=3)
        ttk.Label(form, text="구분").grid(row=0, column=2, sticky="w", padx=(12, 6), pady=3)
        ttk.Combobox(form, textvariable=self.src_group_var, values=GROUPS[1:], state="readonly").grid(row=0, column=3, sticky="ew", pady=3)

        ttk.Label(form, text="국문 교실명").grid(row=1, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(form, textvariable=self.src_dept_ko_var).grid(row=1, column=1, sticky="ew", pady=3)
        ttk.Label(form, text="영문 교실명").grid(row=1, column=2, sticky="w", padx=(12, 6), pady=3)
        ttk.Entry(form, textvariable=self.src_dept_en_var).grid(row=1, column=3, sticky="ew", pady=3)

        ttk.Label(form, text="페이지 유형").grid(row=2, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Combobox(form, textvariable=self.src_page_type_var, values=PAGE_TYPES, state="readonly").grid(row=2, column=1, sticky="ew", pady=3)
        ttk.Label(form, text="활성").grid(row=2, column=2, sticky="w", padx=(12, 6), pady=3)
        ttk.Combobox(form, textvariable=self.src_active_var, values=["Y", "N"], state="readonly", width=8).grid(row=2, column=3, sticky="w", pady=3)

        ttk.Label(form, text="국문 URL").grid(row=3, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(form, textvariable=self.src_url_ko_var).grid(row=3, column=1, columnspan=3, sticky="ew", pady=3)
        ttk.Label(form, text="영문 URL").grid(row=4, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(form, textvariable=self.src_url_en_var).grid(row=4, column=1, columnspan=3, sticky="ew", pady=3)
        ttk.Label(form, text="비고").grid(row=5, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(form, textvariable=self.src_note_var).grid(row=5, column=1, columnspan=3, sticky="ew", pady=3)

        btns = ttk.Frame(parent)
        btns.pack(fill="x", pady=(8, 8))
        ttk.Button(btns, text="입력 초기화", command=self.clear_source_form).pack(side="left", padx=4)
        ttk.Button(btns, text="현재 입력 추가", command=self.add_source_row).pack(side="left", padx=4)
        ttk.Button(btns, text="선택 행 수정", command=self.update_source_row).pack(side="left", padx=4)
        ttk.Button(btns, text="선택 행 삭제", command=self.delete_source_row).pack(side="left", padx=4)
        ttk.Button(btns, text="등록 CSV 저장", command=self.save_registry_csv_dialog).pack(side="left", padx=4)

        table_frame = ttk.Frame(parent)
        table_frame.pack(fill="both", expand=True)
        columns = ("id", "group", "department_ko", "department_en", "page_type", "url_ko", "url_en", "active", "note")
        self.registry_tree = ttk.Treeview(table_frame, columns=columns, show="headings", selectmode="browse")
        headings = {
            "id": "ID", "group": "구분", "department_ko": "국문 교실명", "department_en": "영문 교실명",
            "page_type": "유형", "url_ko": "국문 URL", "url_en": "영문 URL", "active": "활성", "note": "비고"
        }
        widths = {"id": 60, "group": 80, "department_ko": 160, "department_en": 200, "page_type": 110, "url_ko": 320, "url_en": 320, "active": 60, "note": 120}
        for col in columns:
            self.registry_tree.heading(col, text=headings[col])
            self.registry_tree.column(col, width=widths[col], anchor="w")
        self.registry_tree.pack(side="left", fill="both", expand=True)
        self.registry_tree.bind("<<TreeviewSelect>>", self.on_registry_select)
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.registry_tree.yview)
        self.registry_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

    def _build_collect_tab(self, parent: ttk.Frame) -> None:
        top = ttk.LabelFrame(parent, text="교수정보 수집", padding=10)
        top.pack(fill="x")
        ttk.Label(top, text="수집 구분").grid(row=0, column=0, sticky="w", padx=(0, 6))
        ttk.Combobox(top, textvariable=self.collect_group_var, values=GROUPS, state="readonly").grid(row=0, column=1, sticky="w")
        ttk.Button(top, text="등록 URL로 교수정보 수집", command=self.collect_from_registry).grid(row=0, column=2, padx=6)
        ttk.Button(top, text="최신 교수명단 등록", command=self.register_latest_professors).grid(row=0, column=3, padx=6)
        ttk.Button(top, text="교수명단 파일 열기/편집", command=self.open_professor_results_file).grid(row=0, column=4, padx=6)

        info = ttk.LabelFrame(parent, text="설명", padding=10)
        info.pack(fill="both", expand=True, pady=(10, 0))
        msg = (
            "1. URL 등록 탭에서 국문/영문 교실 URL을 등록합니다.\n"
            "2. 등록 URL로 교수정보 수집을 실행하면 등록된 URL만 대상으로 CSV용 교수명단을 생성합니다.\n"
            "3. 수집 결과 검토 창에서 선택 삭제, 전체 저장, 선택만 저장을 할 수 있습니다.\n"
            "4. 최신 교수명단 등록은 기존 자동 수집 기능을 별도 경로로 유지합니다."
        )
        ttk.Label(info, text=msg, justify="left").pack(anchor="w")

    def _build_achievements_tab(self, parent: ttk.Frame) -> None:
        top = ttk.LabelFrame(parent, text="교수성과 CSV 생성", padding=10)
        top.pack(fill="x")
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="교수명단 CSV").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(top, textvariable=self.professors_csv_var).grid(row=0, column=1, sticky="ew", pady=3)
        ttk.Button(top, text="찾아보기", command=self.pick_professors_csv).grid(row=0, column=2, padx=6, pady=3)

        ttk.Label(top, text="구분").grid(row=1, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Combobox(top, textvariable=self.ach_group_var, values=GROUPS, state="readonly").grid(row=1, column=1, sticky="w", pady=3)
        ttk.Label(top, text="이메일").grid(row=2, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(top, textvariable=self.email_var).grid(row=2, column=1, sticky="ew", pady=3)
        ttk.Label(top, text="교수당 최대 논문 수").grid(row=3, column=0, sticky="w", padx=(0, 6), pady=3)
        ttk.Entry(top, textvariable=self.limit_var, width=12).grid(row=3, column=1, sticky="w", pady=3)
        ttk.Button(top, text="교수성과 CSV 생성", command=self.export_achievements).grid(row=4, column=1, sticky="w", pady=(8, 0))

    def _build_log_tab(self, parent: ttk.Frame) -> None:
        self.log_text = tk.Text(parent, wrap="word")
        self.log_text.pack(fill="both", expand=True)

    def append_log(self, msg: str) -> None:
        self.log_queue.put(msg)

    def _drain_log_queue(self) -> None:
        while not self.log_queue.empty():
            msg = self.log_queue.get_nowait()
            timestamp = time.strftime("%H:%M:%S")
            self.log_text.insert("end", f"[{timestamp}] {msg}\n")
            self.log_text.see("end")
            self.status_var.set(msg)
        self.root.after(120, self._drain_log_queue)

    def set_status(self, msg: str) -> None:
        self.status_var.set(msg)
        self.append_log(msg)

    def pick_output_dir(self) -> None:
        path = filedialog.askdirectory(title="작업 폴더 선택", initialdir=self.output_dir_var.get() or str(Path.home()))
        if path:
            self.output_dir_var.set(path)

    def load_registry_csv_dialog(self) -> None:
        path = filedialog.askopenfilename(title="등록 CSV 열기", filetypes=[("CSV", "*.csv")], initialdir=self.output_dir_var.get() or str(Path.home()))
        if path:
            self.registry_csv_var.set(path)
            self._try_load_registry(path)

    def _try_load_registry(self, path: str) -> None:
        try:
            rows = load_professor_source_registry(path)
        except Exception as exc:
            messagebox.showerror("오류", f"등록 CSV를 불러오지 못했습니다.\n{exc}")
            return
        self.registry_rows = rows
        self.refresh_registry_tree()
        self.append_log(f"[REGISTRY] 로드 완료: {path} | {len(rows)}행")

    def create_registry_template(self) -> None:
        default_dir = self.output_dir_var.get().strip() or str(Path.home())
        path = filedialog.asksaveasfilename(title="등록 CSV 템플릿 저장", defaultextension=".csv", initialdir=default_dir, initialfile="professor_source_registry_template.csv", filetypes=[("CSV", "*.csv")])
        if not path:
            return
        try:
            create_professor_source_registry_template(path)
            self.registry_csv_var.set(path)
            self._try_load_registry(path)
        except Exception as exc:
            messagebox.showerror("오류", str(exc))

    def save_registry_csv_dialog(self) -> None:
        default_dir = self.output_dir_var.get().strip() or str(Path.home())
        path = filedialog.asksaveasfilename(title="등록 CSV 저장", defaultextension=".csv", initialdir=default_dir, initialfile=Path(self.registry_csv_var.get() or 'professor_source_registry.csv').name, filetypes=[("CSV", "*.csv")])
        if not path:
            return
        try:
            save_professor_source_registry(path, self.registry_rows)
            self.registry_csv_var.set(path)
            self.append_log(f"[REGISTRY] 저장 완료: {path} | {len(self.registry_rows)}행")
        except Exception as exc:
            messagebox.showerror("오류", str(exc))

    def refresh_registry_tree(self) -> None:
        for item in self.registry_tree.get_children():
            self.registry_tree.delete(item)
        for row in self.registry_rows:
            self.registry_tree.insert("", "end", values=(row.id, row.group, row.department_ko, row.department_en, row.page_type, row.url_ko, row.url_en, row.active, row.note))

    def clear_source_form(self) -> None:
        self.src_id_var.set("")
        self.src_group_var.set("기초의학")
        self.src_dept_ko_var.set("")
        self.src_dept_en_var.set("")
        self.src_page_type_var.set("department_list")
        self.src_url_ko_var.set("")
        self.src_url_en_var.set("")
        self.src_active_var.set("Y")
        self.src_note_var.set("")

    def _build_source_row_from_form(self) -> ProfessorSourceRow:
        row_id = self.src_id_var.get().strip() or str(len(self.registry_rows) + 1)
        return ProfessorSourceRow(
            id=row_id,
            group=self.src_group_var.get().strip(),
            department_ko=self.src_dept_ko_var.get().strip(),
            department_en=self.src_dept_en_var.get().strip(),
            page_type=self.src_page_type_var.get().strip(),
            url_ko=self.src_url_ko_var.get().strip(),
            url_en=self.src_url_en_var.get().strip(),
            active=self.src_active_var.get().strip() or "Y",
            note=self.src_note_var.get().strip(),
        )

    def add_source_row(self) -> None:
        row = self._build_source_row_from_form()
        if not row.department_ko or not row.url_ko:
            messagebox.showwarning("입력 필요", "국문 교실명과 국문 URL은 필수입니다.")
            return
        self.registry_rows.append(row)
        self.refresh_registry_tree()
        self.append_log(f"[REGISTRY] 추가: {row.group} | {row.department_ko}")
        self.clear_source_form()

    def on_registry_select(self, _event=None) -> None:
        sel = self.registry_tree.selection()
        if not sel:
            return
        values = self.registry_tree.item(sel[0], "values")
        if not values:
            return
        self.src_id_var.set(values[0])
        self.src_group_var.set(values[1])
        self.src_dept_ko_var.set(values[2])
        self.src_dept_en_var.set(values[3])
        self.src_page_type_var.set(values[4])
        self.src_url_ko_var.set(values[5])
        self.src_url_en_var.set(values[6])
        self.src_active_var.set(values[7])
        self.src_note_var.set(values[8])

    def update_source_row(self) -> None:
        sel = self.registry_tree.selection()
        if not sel:
            messagebox.showwarning("선택 필요", "수정할 행을 선택하세요.")
            return
        idx = self.registry_tree.index(sel[0])
        self.registry_rows[idx] = self._build_source_row_from_form()
        self.refresh_registry_tree()
        self.append_log("[REGISTRY] 선택 행 수정 완료")

    def delete_source_row(self) -> None:
        sel = self.registry_tree.selection()
        if not sel:
            messagebox.showwarning("선택 필요", "삭제할 행을 선택하세요.")
            return
        idx = self.registry_tree.index(sel[0])
        deleted = self.registry_rows.pop(idx)
        self.refresh_registry_tree()
        self.append_log(f"[REGISTRY] 삭제: {deleted.department_ko}")

    def collect_from_registry(self) -> None:
        registry_csv = self.registry_csv_var.get().strip()
        if not registry_csv:
            messagebox.showwarning("등록 CSV 필요", "먼저 등록 CSV를 불러오거나 저장하세요.")
            return
        group_filter = self.collect_group_var.get().strip() or "전체"
        self.append_log(f"[REGISTRY] 등록 URL 수집 시작 | 구분={group_filter}")

        def worker() -> None:
            try:
                rows = fetch_professors_from_registered_sources(registry_csv, group_filter=group_filter, logger=self.append_log)
                self.collected_rows = rows
                self.root.after(0, lambda: self._open_results_editor(rows, source_label=f"등록 URL 수집 결과 | {group_filter}"))
                self.root.after(0, lambda: self.status_var.set(f"등록 URL 수집 완료: {len(rows)}명"))
            except Exception as exc:
                self.append_log(f"[REGISTRY-ERROR] {exc}")
                self.root.after(0, lambda: messagebox.showerror("오류", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def register_latest_professors(self) -> None:
        group = self.collect_group_var.get().strip() or "전체"
        default_dir = Path(self.output_dir_var.get().strip() or str(Path.home()))
        slug = {"전체": "all", "기초의학": "basic", "임상의학": "clinical", "인문의학": "humanities"}.get(group, "all")
        out = filedialog.asksaveasfilename(title="최신 교수명단 등록 저장", defaultextension=".csv", initialdir=str(default_dir), initialfile=f"yonsei_professors_latest_{slug}_{time.strftime('%Y%m%d_%H%M%S')}.csv", filetypes=[("CSV", "*.csv")])
        if not out:
            return
        self.append_log(f"[YONSEI] 최신 교수명단 등록 시작 | 구분={group}")

        def worker() -> None:
            try:
                export_latest_yonsei_professors_csv(out, group_filter=group, logger=self.append_log)
                self.professors_csv_var.set(out)
                self.root.after(0, lambda: self._open_professors_csv_editor(out, source_label=f"최신 교수명단 | {group}"))
                self.root.after(0, lambda: self.status_var.set(f"최신 교수명단 등록 완료: {Path(out).name}"))
            except Exception as exc:
                self.append_log(f"[YONSEI-ERROR] {exc}")
                self.root.after(0, lambda: messagebox.showerror("오류", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def pick_professors_csv(self) -> None:
        path = filedialog.askopenfilename(title="교수명단 CSV 선택", filetypes=[("CSV", "*.csv")], initialdir=self.output_dir_var.get() or str(Path.home()))
        if path:
            self.professors_csv_var.set(path)

    def open_professor_results_file(self) -> None:
        path = self.professors_csv_var.get().strip()
        if not path:
            path = filedialog.askopenfilename(title="교수명단 CSV 열기", filetypes=[("CSV", "*.csv")], initialdir=self.output_dir_var.get() or str(Path.home()))
            if not path:
                return
            self.professors_csv_var.set(path)
        self._open_professors_csv_editor(path, source_label=Path(path).name)

    def _load_professor_results_from_csv(self, path: str) -> list[ProfessorResultRow]:
        rows: list[ProfessorResultRow] = []
        with Path(path).open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(ProfessorResultRow(
                    group=str(row.get("group", "")).strip(),
                    department=str(row.get("department", "")).strip(),
                    name=str(row.get("name", "")).strip(),
                    query=str(row.get("query", "")).strip(),
                    title=str(row.get("title", "")).strip(),
                    email=str(row.get("email", "")).strip(),
                    affiliation=str(row.get("affiliation", "")).strip(),
                    source_url=str(row.get("source_url", "")).strip(),
                    source_url_en=str(row.get("source_url_en", "")).strip(),
                    match_status=str(row.get("match_status", "")).strip(),
                    review_status=str(row.get("review_status", "pending")).strip() or "pending",
                ))
        return rows

    def _open_professors_csv_editor(self, path: str, *, source_label: str) -> None:
        try:
            rows = self._load_professor_results_from_csv(path)
        except Exception as exc:
            messagebox.showerror("오류", f"교수명단 CSV를 열지 못했습니다.\n{exc}")
            return
        self._open_results_editor(rows, source_label=source_label, source_file=path)

    def _open_results_editor(self, rows: list[ProfessorResultRow], *, source_label: str = "수집 결과", source_file: str | None = None) -> None:
        win = tk.Toplevel(self.root)
        win.title(source_label)
        win.geometry("1460x820")

        current_rows = list(rows)
        visible_rows = list(rows)
        current_file = {"path": source_file or ""}
        item_to_row: dict[str, ProfessorResultRow] = {}

        top = ttk.Frame(win, padding=8)
        top.pack(fill="x")
        ttk.Label(top, text=source_label).pack(side="left")

        search_bar = ttk.Frame(win, padding=(8, 4, 8, 0))
        search_bar.pack(fill="x")
        ttk.Label(search_bar, text="교수명 검색").pack(side="left")
        search_var = tk.StringVar(value="")
        search_entry = ttk.Entry(search_bar, textvariable=search_var, width=24)
        search_entry.pack(side="left", padx=(6, 8))

        btns = ttk.Frame(win, padding=(8,0))
        btns.pack(fill="x")
        selection_bar = ttk.Frame(win, padding=(8, 4, 8, 0))
        selection_bar.pack(fill="x")
        tree_frame = ttk.Frame(win, padding=8)
        tree_frame.pack(fill="both", expand=True)

        columns = ("group", "department", "name", "query", "title", "email", "match_status", "review_status")
        tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings", selectmode="extended")
        tree.heading("#0", text="선택")
        tree.column("#0", width=70, minwidth=60, anchor="center", stretch=False)
        headings = {"group": "구분", "department": "교실", "name": "교수명", "query": "영문이름(Query)", "title": "직위", "email": "이메일", "match_status": "매칭", "review_status": "검토"}
        widths = {"group": 80, "department": 180, "name": 100, "query": 220, "title": 120, "email": 180, "match_status": 100, "review_status": 90}
        for c in columns:
            tree.heading(c, text=headings[c])
            tree.column(c, width=widths[c], anchor="w")
        tree.pack(side="left", fill="both", expand=True)
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

        selection_state_var = tk.StringVar(value="선택된 항목: 0 / 0")

        def get_filtered_rows() -> list[ProfessorResultRow]:
            keyword = search_var.get().strip().casefold()
            if not keyword:
                return list(current_rows)
            return [
                row for row in current_rows
                if keyword in str(row.name).casefold()
                or keyword in str(row.query).casefold()
                or keyword in str(row.department).casefold()
                or keyword in str(row.email).casefold()
            ]

        def update_selection_summary() -> None:
            selected = len(tree.selection())
            selection_state_var.set(f"선택된 항목: {selected} / 현재목록 {len(visible_rows)} / 전체 {len(current_rows)}")

        def sync_selected_markers(_event=None) -> None:
            selected = set(tree.selection())
            for item in tree.get_children():
                tree.item(item, text="☑" if item in selected else "☐")
            update_selection_summary()

        def refresh() -> None:
            nonlocal visible_rows
            previously_selected_rows = {item_to_row[item] for item in tree.selection() if item in item_to_row}
            visible_rows = get_filtered_rows()
            item_to_row.clear()
            for item in tree.get_children():
                tree.delete(item)
            for row in visible_rows:
                item_id = tree.insert(
                    "",
                    "end",
                    text="☐",
                    values=(row.group, row.department, row.name, row.query, row.title, row.email, row.match_status, row.review_status),
                )
                item_to_row[item_id] = row
                if row in previously_selected_rows:
                    tree.selection_add(item_id)
            sync_selected_markers()

        def selected_rows() -> list[ProfessorResultRow]:
            return [item_to_row[item] for item in tree.selection() if item in item_to_row]

        def select_all() -> None:
            items = tree.get_children()
            if items:
                tree.selection_set(items)
            sync_selected_markers()

        def clear_selection() -> None:
            tree.selection_remove(tree.selection())
            sync_selected_markers()

        def apply_search(_event=None) -> None:
            refresh()

        def clear_search() -> None:
            search_var.set("")
            refresh()

        def delete_selected() -> None:
            rows_to_remove = set(selected_rows())
            if not rows_to_remove:
                messagebox.showwarning("선택 필요", "삭제할 행을 선택하세요.")
                return
            current_rows[:] = [row for row in current_rows if row not in rows_to_remove]
            refresh()

        def save_all() -> None:
            path = filedialog.asksaveasfilename(title="교수명단 CSV 저장", defaultextension=".csv", initialdir=self.output_dir_var.get() or str(Path.home()), initialfile=Path(current_file['path'] or 'professors_reviewed.csv').name, filetypes=[("CSV", "*.csv")])
            if not path:
                return
            export_professor_results_csv(path, current_rows)
            current_file['path'] = path
            self.professors_csv_var.set(path)
            self.append_log(f"[YONSEI-EDIT] 저장 완료: {path} | {len(current_rows)}명")

        def save_selected() -> None:
            rows_to_save = selected_rows()
            if not rows_to_save:
                messagebox.showwarning("선택 필요", "저장할 행을 선택하세요.")
                return
            path = filedialog.asksaveasfilename(title="선택 교수명단 CSV 저장", defaultextension=".csv", initialdir=self.output_dir_var.get() or str(Path.home()), initialfile="professors_selected.csv", filetypes=[("CSV", "*.csv")])
            if not path:
                return
            export_professor_results_csv(path, rows_to_save)
            self.append_log(f"[YONSEI-EDIT] 선택 저장 완료: {path} | {len(rows_to_save)}명")

        def run_achievements_for_rows(target_rows: list[ProfessorResultRow], mode_label: str) -> None:
            if not target_rows:
                messagebox.showwarning("대상 필요", f"성과 분석할 {mode_label} 교수를 찾지 못했습니다.")
                return
            try:
                per_limit = int(self.limit_var.get().strip() or "20")
            except ValueError:
                messagebox.showwarning("입력 오류", "교수당 최대 논문 수는 숫자여야 합니다.")
                return
            default_dir = Path(self.output_dir_var.get().strip() or str(Path.home()))
            filename_suffix = f"{mode_label}_{time.strftime('%Y%m%d_%H%M%S')}"
            out = filedialog.asksaveasfilename(
                title=f"{mode_label} 교수성과 CSV 저장",
                defaultextension=".csv",
                initialdir=str(default_dir),
                initialfile=f"professor_achievements_{filename_suffix}.csv",
                filetypes=[("CSV", "*.csv")],
            )
            if not out:
                return

            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv", encoding="utf-8-sig", newline="") as tmp:
                tmp_path = tmp.name
            export_professor_results_csv(tmp_path, target_rows)
            self.append_log(f"[PROF] 성과 조회 시작 | 모드={mode_label} | 대상 교수 {len(target_rows)}명")

            def worker() -> None:
                try:
                    result = export_professor_achievements_csv(
                        tmp_path,
                        out,
                        email=self.email_var.get().strip(),
                        per_professor_limit=per_limit,
                        group_filter="전체",
                        logger=self.append_log,
                    )
                    self.root.after(0, lambda: self.status_var.set(f"교수성과 CSV 생성 완료: {result.get('papers', 0)}건"))
                    PLACEHOLDER
                except Exception as exc:
                    self.append_log(f"[PROF-ERROR] {exc}")
                    self.root.after(0, lambda: messagebox.showerror("오류", str(exc)))
                finally:
                    try:
                        Path(tmp_path).unlink(missing_ok=True)
                    except Exception:
                        pass

            threading.Thread(target=worker, daemon=True).start()

        def analyze_selected() -> None:
            run_achievements_for_rows(selected_rows(), "선택")

        def analyze_searched() -> None:
            run_achievements_for_rows(list(visible_rows), "검색")

        def reload_current() -> None:
            path = current_file['path']
            if not path:
                messagebox.showwarning("파일 필요", "현재 다시 불러올 파일이 없습니다.")
                return
            try:
                loaded = self._load_professor_results_from_csv(path)
            except Exception as exc:
                messagebox.showerror("오류", str(exc))
                return
            current_rows.clear()
            current_rows.extend(loaded)
            refresh()
            self.append_log(f"[YONSEI-EDIT] 교수명단 다시 불러오기 완료: {path} | {len(loaded)}명")

        def open_other() -> None:
            path = filedialog.askopenfilename(title="다른 교수명단 CSV 열기", filetypes=[("CSV", "*.csv")], initialdir=self.output_dir_var.get() or str(Path.home()))
            if not path:
                return
            try:
                loaded = self._load_professor_results_from_csv(path)
            except Exception as exc:
                messagebox.showerror("오류", str(exc))
                return
            current_file['path'] = path
            self.professors_csv_var.set(path)
            current_rows.clear()
            current_rows.extend(loaded)
            refresh()
            self.append_log(f"[YONSEI-EDIT] 교수명단 열기 완료: {path} | {len(loaded)}명")

        ttk.Button(search_bar, text="검색", command=apply_search).pack(side="left", padx=4)
        ttk.Button(search_bar, text="검색 초기화", command=clear_search).pack(side="left", padx=4)
        ttk.Button(btns, text="전체 선택", command=select_all).pack(side="left", padx=4)
        ttk.Button(btns, text="전체 선택 해제", command=clear_selection).pack(side="left", padx=4)
        ttk.Button(btns, text="선택 삭제", command=delete_selected).pack(side="left", padx=4)
        ttk.Button(btns, text="선택 교수 성과 수집", command=analyze_selected).pack(side="left", padx=4)
        ttk.Button(btns, text="검색 결과 성과 분석", command=analyze_searched).pack(side="left", padx=4)
        ttk.Button(btns, text="현재 목록 저장", command=save_all).pack(side="left", padx=4)
        ttk.Button(btns, text="선택만 저장", command=save_selected).pack(side="left", padx=4)
        ttk.Button(btns, text="현재 파일 다시 불러오기", command=reload_current).pack(side="left", padx=4)
        ttk.Button(btns, text="다른 파일 열기", command=open_other).pack(side="left", padx=4)
        ttk.Label(selection_bar, textvariable=selection_state_var).pack(side="left")
        tree.bind("<<TreeviewSelect>>", sync_selected_markers)
        search_entry.bind("<Return>", apply_search)
        refresh()

    def export_achievements(self) -> None:
        professors_file = self.professors_csv_var.get().strip()
        if not professors_file:
            messagebox.showwarning("교수 목록 필요", "교수명단 CSV를 선택하세요.")
            return
        try:
            per_limit = int(self.limit_var.get().strip() or "20")
        except ValueError:
            messagebox.showwarning("입력 오류", "교수당 최대 논문 수는 숫자여야 합니다.")
            return
        default_dir = Path(self.output_dir_var.get().strip() or str(Path.home()))
        group = self.ach_group_var.get().strip() or "전체"
        out = filedialog.asksaveasfilename(title="교수성과 CSV 저장", defaultextension=".csv", initialdir=str(default_dir), initialfile=f"professor_achievements_{time.strftime('%Y%m%d_%H%M%S')}.csv", filetypes=[("CSV", "*.csv")])
        if not out:
            return

        self.append_log(f"[PROF] 성과 조회 시작 | 구분={group} | 파일={professors_file}")
        def worker() -> None:
            try:
                result = export_professor_achievements_csv(professors_file, out, email=self.email_var.get().strip(), per_professor_limit=per_limit, group_filter=group, logger=self.append_log)
                self.root.after(0, lambda: self.status_var.set(f"교수성과 CSV 생성 완료: {result.get('papers', 0)}건"))
                self.root.after(0, lambda: messagebox.showinfo("완료", f"교수성과 CSV 생성 완료\n{out}"))
            except Exception as exc:
                self.append_log(f"[PROF-ERROR] {exc}")
                self.root.after(0, lambda: messagebox.showerror("오류", str(exc)))
        threading.Thread(target=worker, daemon=True).start()

    def save_settings(self) -> None:
        self.config.output_dir = self.output_dir_var.get().strip()
        self.config.registry_csv = self.registry_csv_var.get().strip()
        self.config.professors_csv = self.professors_csv_var.get().strip()
        self.config.achievements_email = self.email_var.get().strip()
        self.config.per_professor_limit = self.limit_var.get().strip() or "20"
        self.config.save(self.config_path)
        self.append_log("[CONFIG] 설정 저장 완료")

    def on_close(self) -> None:
        self.save_settings()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    ProfessorManagerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
