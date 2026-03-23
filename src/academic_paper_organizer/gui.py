from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from watchdog.observers import Observer

from .core import PDFCreatedHandler, PaperIndex, PaperOrganizer, scan_existing_pdfs, run_reindex


class GuiLogHandler:
    def __init__(self, callback):
        self.callback = callback

    def write(self, message: str) -> None:
        if message.strip():
            self.callback(message.rstrip())

    def flush(self) -> None:
        return


class OrganizerGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title('Academic Paper Organizer GUI')
        self.root.geometry('1180x760')

        self.watch_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.keyword_var = tk.StringVar()
        self.author_var = tk.StringVar()
        self.year_var = tk.StringVar()
        self.field_var = tk.StringVar()
        self.venue_var = tk.StringVar()
        self.limit_var = tk.StringVar(value='50')
        self.status_var = tk.StringVar(value='대기 중')

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.observer: Observer | None = None
        self.organizer: PaperOrganizer | None = None
        self.worker_thread: threading.Thread | None = None

        self._build_ui()
        self.root.protocol('WM_DELETE_WINDOW', self.on_close)
        self.root.after(150, self._drain_log_queue)

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill='both', expand=True)

        top = ttk.LabelFrame(outer, text='기본 설정', padding=10)
        top.pack(fill='x')
        top.columnconfigure(1, weight=1)
        top.columnconfigure(4, weight=1)

        ttk.Label(top, text='감시 폴더').grid(row=0, column=0, sticky='w', padx=(0, 8), pady=4)
        ttk.Entry(top, textvariable=self.watch_var).grid(row=0, column=1, sticky='ew', pady=4)
        ttk.Button(top, text='찾아보기', command=self.pick_watch_dir).grid(row=0, column=2, padx=6, pady=4)

        ttk.Label(top, text='출력 폴더').grid(row=1, column=0, sticky='w', padx=(0, 8), pady=4)
        ttk.Entry(top, textvariable=self.output_var).grid(row=1, column=1, sticky='ew', pady=4)
        ttk.Button(top, text='찾아보기', command=self.pick_output_dir).grid(row=1, column=2, padx=6, pady=4)

        controls = ttk.Frame(top)
        controls.grid(row=0, column=4, rowspan=2, sticky='e')
        ttk.Button(controls, text='1회 처리', command=self.run_once).pack(side='left', padx=4)
        ttk.Button(controls, text='감시 시작', command=self.start_watch).pack(side='left', padx=4)
        ttk.Button(controls, text='감시 중지', command=self.stop_watch).pack(side='left', padx=4)
        ttk.Button(controls, text='재인덱싱', command=self.reindex).pack(side='left', padx=4)

        notebook = ttk.Notebook(outer)
        notebook.pack(fill='both', expand=True, pady=(10, 0))

        search_tab = ttk.Frame(notebook, padding=10)
        log_tab = ttk.Frame(notebook, padding=10)
        notebook.add(search_tab, text='검색')
        notebook.add(log_tab, text='로그')

        self._build_search_tab(search_tab)
        self._build_log_tab(log_tab)

        status_bar = ttk.Label(outer, textvariable=self.status_var, anchor='w')
        status_bar.pack(fill='x', pady=(8, 0))

    def _build_search_tab(self, parent: ttk.Frame) -> None:
        filters = ttk.LabelFrame(parent, text='검색 조건', padding=10)
        filters.pack(fill='x')
        for i in range(6):
            filters.columnconfigure(i, weight=1 if i in (1, 3, 5) else 0)

        ttk.Label(filters, text='키워드').grid(row=0, column=0, sticky='w', padx=(0, 6), pady=4)
        ttk.Entry(filters, textvariable=self.keyword_var).grid(row=0, column=1, sticky='ew', pady=4)
        ttk.Label(filters, text='저자').grid(row=0, column=2, sticky='w', padx=(16, 6), pady=4)
        ttk.Entry(filters, textvariable=self.author_var).grid(row=0, column=3, sticky='ew', pady=4)
        ttk.Label(filters, text='연도').grid(row=0, column=4, sticky='w', padx=(16, 6), pady=4)
        ttk.Entry(filters, textvariable=self.year_var, width=10).grid(row=0, column=5, sticky='ew', pady=4)

        ttk.Label(filters, text='분야').grid(row=1, column=0, sticky='w', padx=(0, 6), pady=4)
        ttk.Entry(filters, textvariable=self.field_var).grid(row=1, column=1, sticky='ew', pady=4)
        ttk.Label(filters, text='저널/학회').grid(row=1, column=2, sticky='w', padx=(16, 6), pady=4)
        ttk.Entry(filters, textvariable=self.venue_var).grid(row=1, column=3, sticky='ew', pady=4)
        ttk.Label(filters, text='최대 건수').grid(row=1, column=4, sticky='w', padx=(16, 6), pady=4)
        ttk.Entry(filters, textvariable=self.limit_var, width=10).grid(row=1, column=5, sticky='ew', pady=4)

        action_row = ttk.Frame(parent)
        action_row.pack(fill='x', pady=(8, 8))
        ttk.Button(action_row, text='검색', command=self.search).pack(side='left')
        ttk.Button(action_row, text='필터 초기화', command=self.clear_filters).pack(side='left', padx=6)
        ttk.Button(action_row, text='파일 열기', command=self.open_selected_file).pack(side='left', padx=(24, 6))
        ttk.Button(action_row, text='폴더 열기', command=self.open_selected_folder).pack(side='left', padx=6)

        columns = ('field', 'year', 'author', 'venue', 'title', 'doi', 'path')
        self.tree = ttk.Treeview(parent, columns=columns, show='headings', height=18)
        headings = {
            'field': '분야', 'year': '연도', 'author': '저자', 'venue': '저널/학회',
            'title': '제목', 'doi': 'DOI', 'path': '경로'
        }
        widths = {'field': 70, 'year': 70, 'author': 100, 'venue': 170, 'title': 320, 'doi': 180, 'path': 300}
        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=widths[col], anchor='w')
        self.tree.pack(fill='both', expand=True)
        self.tree.bind('<Double-1>', lambda event: self.open_selected_file())

        scrollbar = ttk.Scrollbar(parent, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.place(relx=1.0, rely=0.33, relheight=0.62, anchor='ne')

        snippet_box = ttk.LabelFrame(parent, text='미리보기', padding=8)
        snippet_box.pack(fill='both', expand=False, pady=(8, 0))
        self.snippet_text = tk.Text(snippet_box, height=8, wrap='word')
        self.snippet_text.pack(fill='both', expand=True)
        self.tree.bind('<<TreeviewSelect>>', self._on_tree_select)

    def _build_log_tab(self, parent: ttk.Frame) -> None:
        self.log_text = tk.Text(parent, wrap='word')
        self.log_text.pack(fill='both', expand=True)

    def pick_watch_dir(self) -> None:
        path = filedialog.askdirectory(title='감시 폴더 선택')
        if path:
            self.watch_var.set(path)

    def pick_output_dir(self) -> None:
        path = filedialog.askdirectory(title='출력 폴더 선택')
        if path:
            self.output_var.set(path)

    def append_log(self, message: str) -> None:
        self.log_queue.put(message)

    def _drain_log_queue(self) -> None:
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_text.insert('end', message + '\n')
            self.log_text.see('end')
        self.root.after(150, self._drain_log_queue)

    def _validate_paths(self) -> tuple[Path, Path] | None:
        watch = self.watch_var.get().strip()
        output = self.output_var.get().strip()
        if not watch or not output:
            messagebox.showwarning('경고', '감시 폴더와 출력 폴더를 모두 지정해 주세요.')
            return None
        return Path(watch).expanduser().resolve(), Path(output).expanduser().resolve()

    def _run_in_thread(self, target, status_text: str) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo('안내', '이미 작업이 실행 중입니다.')
            return
        self.status_var.set(status_text)
        self.worker_thread = threading.Thread(target=target, daemon=True)
        self.worker_thread.start()

    def run_once(self) -> None:
        paths = self._validate_paths()
        if not paths:
            return
        watch_dir, output_dir = paths

        def task():
            organizer = PaperOrganizer(watch_dir=watch_dir, output_dir=output_dir)
            try:
                self.append_log(f'[GUI] 1회 처리 시작: {watch_dir}')
                scan_existing_pdfs(organizer, watch_dir)
                self.append_log('[GUI] 1회 처리 완료')
            finally:
                organizer.close()
                self.status_var.set('1회 처리 완료')

        self._run_in_thread(task, '1회 처리 중')

    def start_watch(self) -> None:
        if self.observer and self.observer.is_alive():
            messagebox.showinfo('안내', '이미 감시 중입니다.')
            return
        paths = self._validate_paths()
        if not paths:
            return
        watch_dir, output_dir = paths
        self.organizer = PaperOrganizer(watch_dir=watch_dir, output_dir=output_dir)
        handler = PDFCreatedHandler(self.organizer)
        self.observer = Observer()
        self.observer.schedule(handler, str(watch_dir), recursive=False)
        self.observer.start()
        self.status_var.set(f'감시 중: {watch_dir}')
        self.append_log(f'[GUI] 감시 시작: {watch_dir}')

    def stop_watch(self) -> None:
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=3)
            self.observer = None
        if self.organizer:
            self.organizer.close()
            self.organizer = None
        self.status_var.set('감시 중지')
        self.append_log('[GUI] 감시 중지')

    def reindex(self) -> None:
        output = self.output_var.get().strip()
        if not output:
            messagebox.showwarning('경고', '출력 폴더를 지정해 주세요.')
            return
        output_dir = Path(output).expanduser().resolve()

        def task():
            class Args:
                pass
            args = Args()
            args.output = str(output_dir)
            self.append_log(f'[GUI] 재인덱싱 시작: {output_dir}')
            stdout_backup = sys.stdout
            sys.stdout = GuiLogHandler(self.append_log)
            try:
                run_reindex(args)
            finally:
                sys.stdout = stdout_backup
                self.status_var.set('재인덱싱 완료')

        self._run_in_thread(task, '재인덱싱 중')

    def clear_filters(self) -> None:
        self.keyword_var.set('')
        self.author_var.set('')
        self.year_var.set('')
        self.field_var.set('')
        self.venue_var.set('')
        self.limit_var.set('50')
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.snippet_text.delete('1.0', 'end')

    def search(self) -> None:
        output = self.output_var.get().strip()
        if not output:
            messagebox.showwarning('경고', '출력 폴더를 지정해 주세요.')
            return
        db_path = Path(output).expanduser().resolve() / 'LOG' / 'paper_index.sqlite3'
        if not db_path.exists():
            messagebox.showwarning('경고', '검색 인덱스가 없습니다. 먼저 PDF를 처리하거나 재인덱싱해 주세요.')
            return
        try:
            limit = int(self.limit_var.get().strip() or '50')
        except ValueError:
            messagebox.showwarning('경고', '최대 건수는 숫자로 입력해 주세요.')
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
        finally:
            index.close()

        for item in self.tree.get_children():
            self.tree.delete(item)

        for row in rows:
            self.tree.insert('', 'end', values=(row.field_code, row.year, row.first_author, row.venue, row.title, row.doi, row.path), tags=(row.snippet,))

        self.status_var.set(f'검색 완료: {len(rows)}건')
        self.append_log(f'[GUI] 검색 완료: {len(rows)}건')
        if rows:
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._on_tree_select(None)

    def _on_tree_select(self, event) -> None:
        selected = self.tree.selection()
        if not selected:
            return
        item = selected[0]
        tags = self.tree.item(item, 'tags')
        snippet = tags[0] if tags else ''
        self.snippet_text.delete('1.0', 'end')
        self.snippet_text.insert('1.0', snippet)

    def _selected_path(self) -> Path | None:
        selected = self.tree.selection()
        if not selected:
            return None
        values = self.tree.item(selected[0], 'values')
        if not values:
            return None
        return Path(values[6])

    def open_selected_file(self) -> None:
        path = self._selected_path()
        if not path:
            messagebox.showinfo('안내', '먼저 검색 결과에서 파일을 선택해 주세요.')
            return
        if not path.exists():
            messagebox.showwarning('경고', f'파일이 존재하지 않습니다.')
            return
        self._open_path(path)

    def open_selected_folder(self) -> None:
        path = self._selected_path()
        if not path:
            messagebox.showinfo('안내', '먼저 검색 결과에서 파일을 선택해 주세요.')
            return
        target = path.parent
        if not target.exists():
            messagebox.showwarning('경고', f'폴더가 존재하지 않습니다.{target}')
            return
        self._open_path(target)

    def _open_path(self, path: Path) -> None:
        try:
            if sys.platform.startswith('win'):
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == 'darwin':
                subprocess.Popen(['open', str(path)])
            else:
                subprocess.Popen(['xdg-open', str(path)])
        except Exception as exc:
            messagebox.showerror('오류', f'열기에 실패했습니다.{exc}')

    def on_close(self) -> None:
        self.stop_watch()
        self.root.destroy()


def main() -> int:
    root = tk.Tk()
    ttk.Style().theme_use('clam') if 'clam' in ttk.Style().theme_names() else None
    OrganizerGUI(root)
    root.mainloop()
    return 0
