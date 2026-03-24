from __future__ import annotations

import json
import re
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from watchdog.events import FileSystemEventHandler


FIELD_CODES: dict[str, str] = {
    "AI": "인공지능 일반",
    "ML": "머신러닝",
    "DL": "딥러닝",
    "NLP": "자연어처리",
    "CV": "컴퓨터비전",
    "SP": "음성처리",
    "RS": "추천시스템",
    "DM": "데이터마이닝",
    "IR": "정보검색",
    "RL": "강화학습",
    "ROB": "로보틱스",
    "HCI": "인간-컴퓨터 상호작용",
    "SYS": "시스템",
    "NET": "네트워크",
    "SEC": "보안",
    "DB": "데이터베이스",
    "BIO": "바이오인포매틱스",
    "MED": "의료AI",
    "EDU": "교육/교육AI",
    "MATH": "수학",
    "STAT": "통계",
    "PHY": "물리",
    "CHEM": "화학",
    "ECO": "경제/계량",
    "ETC": "기타",
}


@dataclass
class PaperRow:
    field_code: str
    year: str
    first_author: str
    venue: str
    title: str
    doi: str
    path: str
    original_path: str
    snippet: str


def _current_year() -> int:
    return datetime.now().year


def normalize_year(
    year_value: str | int | None,
    *,
    allow_historic: bool = False,
) -> str:
    """
    연도 문자열/숫자를 안전하게 정규화한다.

    기본 정책:
    - 숫자 4자리만 인정
    - 현대 논문 기본 범위: 1950 ~ 현재연도+1
    - allow_historic=True면 1800년 이후 허용
    - 나머지는 UnknownYear
    """
    if year_value is None:
        return "UnknownYear"

    text = str(year_value).strip()
    if not text:
        return "UnknownYear"

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 4:
        return "UnknownYear"

    year = int(digits)
    current_year = _current_year()

    min_year = 1800 if allow_historic else 1950
    max_year = current_year + 1

    if min_year <= year <= max_year:
        return str(year)

    return "UnknownYear"


def extract_year_candidates(text: str) -> list[str]:
    if not text:
        return []
    return re.findall(r"\b(18\d{2}|19\d{2}|20\d{2})\b", text)


def choose_best_year(
    *candidates: str | int | None,
    allow_historic: bool = False,
) -> str:
    normalized: list[str] = []
    for value in candidates:
        y = normalize_year(value, allow_historic=allow_historic)
        if y != "UnknownYear":
            normalized.append(y)

    if not normalized:
        return "UnknownYear"

    counts: dict[str, int] = {}
    for y in normalized:
        counts[y] = counts.get(y, 0) + 1

    best = sorted(
        counts.items(),
        key=lambda item: (item[1], int(item[0])),
        reverse=True,
    )[0][0]
    return best


def unique_path(path: Path) -> Path:
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


def copy_pdf_preserve_original(src_pdf: Path, dst_pdf: Path) -> Path:
    dst_pdf.parent.mkdir(parents=True, exist_ok=True)
    final_dst = unique_path(dst_pdf)
    shutil.copy2(src_pdf, final_dst)
    return final_dst


def write_pdf_sidecar_metadata(
    meta_path: Path,
    *,
    title: str,
    doi: str,
    original_path: Path,
    stored_path: Path,
    field_code: str,
    year: str,
    author: str,
    venue: str,
) -> None:
    payload = {
        "title": title,
        "doi": doi,
        "original_path": str(original_path),
        "stored_path": str(stored_path),
        "field_code": field_code,
        "year": normalize_year(year),
        "author": author,
        "venue": venue,
    }
    meta_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class PaperIndex:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                field_code TEXT,
                year TEXT,
                first_author TEXT,
                venue TEXT,
                title TEXT,
                doi TEXT,
                path TEXT,
                original_path TEXT,
                snippet TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.commit()
        self._ensure_original_path_column()
        self._ensure_crossref_cache_table()

    def _ensure_original_path_column(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(papers)")
        columns = [row[1] for row in cur.fetchall()]
        if "original_path" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN original_path TEXT")
            self.conn.commit()

    def _ensure_crossref_cache_table(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crossref_cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def clear_crossref_cache(self) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM crossref_cache")
        self.conn.commit()

    def add_paper(
        self,
        *,
        field_code: str,
        year: str,
        first_author: str,
        venue: str,
        title: str,
        doi: str,
        path: str,
        original_path: str,
        snippet: str,
    ) -> None:
        safe_year = normalize_year(year)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO papers (
                field_code, year, first_author, venue, title, doi, path, original_path, snippet
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                field_code,
                safe_year,
                first_author,
                venue,
                title,
                doi,
                path,
                original_path,
                snippet,
            ),
        )
        self.conn.commit()

    def delete_by_original_path(self, original_path: str) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM papers WHERE original_path = ?", (original_path,))
        self.conn.commit()

    def search(
        self,
        *,
        keyword: str = "",
        author: str | None = None,
        year: str | None = None,
        field_code: str | None = None,
        venue: str | None = None,
        limit: int = 50,
    ) -> list[PaperRow]:
        sql = """
            SELECT
                field_code,
                year,
                first_author,
                venue,
                title,
                doi,
                path,
                COALESCE(original_path, '') AS original_path,
                snippet
            FROM papers
            WHERE 1=1
        """
        params: list[str | int] = []

        if keyword:
            like = f"%{keyword}%"
            sql += " AND (title LIKE ? OR snippet LIKE ? OR doi LIKE ?)"
            params.extend([like, like, like])

        if author:
            sql += " AND first_author LIKE ?"
            params.append(f"%{author}%")

        if year:
            sql += " AND year = ?"
            params.append(normalize_year(year))

        if field_code:
            sql += " AND field_code LIKE ?"
            params.append(f"%{field_code}%")

        if venue:
            sql += " AND venue LIKE ?"
            params.append(f"%{venue}%")

        sql += " ORDER BY year DESC, title ASC LIMIT ?"
        params.append(limit)

        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

        return [
            PaperRow(
                field_code=row["field_code"] or "",
                year=row["year"] or "",
                first_author=row["first_author"] or "",
                venue=row["venue"] or "",
                title=row["title"] or "",
                doi=row["doi"] or "",
                path=row["path"] or "",
                original_path=row["original_path"] or "",
                snippet=row["snippet"] or "",
            )
            for row in rows
        ]


def infer_field_code_from_text(text: str) -> str:
    t = text.lower()

    rules: list[tuple[str, list[str]]] = [
        ("NLP", ["nlp", "language model", "llm", "transformer", "bert", "gpt", "token", "prompt"]),
        ("CV", ["cv", "vision", "image", "video", "object detection", "segmentation", "recognition"]),
        ("RL", ["reinforcement learning", "rl", "policy gradient", "q-learning", "mdp"]),
        ("RS", ["recommendation", "recommender system", "collaborative filtering"]),
        ("IR", ["information retrieval", "retrieval", "search engine", "ranking"]),
        ("SP", ["speech", "asr", "tts", "voice", "speaker recognition"]),
        ("ROB", ["robot", "robotics", "manipulation", "navigation"]),
        ("SEC", ["security", "privacy", "attack", "defense", "malware", "cryptography"]),
        ("DB", ["database", "sql", "query optimization", "transaction"]),
        ("BIO", ["bioinformatics", "genomics", "protein", "sequence", "molecule"]),
        ("MED", ["medical", "clinical", "healthcare", "diagnosis", "radiology"]),
        ("STAT", ["statistics", "bayesian", "causal inference", "probability"]),
        ("MATH", ["algebra", "geometry", "theorem", "proof"]),
        ("NET", ["network", "routing", "wireless", "tcp", "5g"]),
        ("SYS", ["system", "distributed", "operating system", "compiler", "storage"]),
        ("HCI", ["human-computer interaction", "hci", "user study", "usability"]),
        ("DM", ["data mining", "clustering", "association rule"]),
        ("DL", ["deep learning", "neural network", "cnn", "rnn", "autoencoder"]),
        ("ML", ["machine learning", "ml", "xgboost", "lightgbm", "svm"]),
        ("AI", ["artificial intelligence", "ai", "planning", "reasoning"]),
        ("PHY", ["physics", "quantum", "particle"]),
        ("CHEM", ["chemistry", "chemical", "reaction"]),
        ("ECO", ["economics", "econometrics", "market"]),
        ("EDU", ["education", "learning analytics", "intelligent tutoring"]),
    ]

    for code, keywords in rules:
        if any(keyword in t for keyword in keywords):
            return code

    return "ETC"


def extract_paper_metadata(pdf_path: Path) -> dict[str, str]:
    """
    현재는 파일명 기반 메타 추출.
    연도는 파일명에서 4자리 후보를 찾되,
    normalize_year()를 통과한 값만 사용한다.
    """
    name = pdf_path.stem

    year_candidates = extract_year_candidates(name)
    year = choose_best_year(*year_candidates)

    field_code = infer_field_code_from_text(name)

    return {
        "field_code": field_code,
        "year": year,
        "first_author": "",
        "venue": "",
        "title": name,
        "doi": "",
        "snippet": "",
    }


def build_output_pdf_path(output_dir: Path, field_code: str, year: str, src_pdf: Path) -> Path:
    safe_field = field_code.strip() or "ETC"
    safe_year = normalize_year(year)
    return output_dir / safe_field / safe_year / src_pdf.name


class PaperOrganizer:
    def __init__(
        self,
        watch_dir: Path,
        output_dir: Path,
        log_fn: Callable[[str], None] | None = None,
        cancel_event: threading.Event | None = None,
        crossref_mailto: str = "your-email@example.com",
        crossref_cache_days: int = 180,
    ):
        self.watch_dir = Path(watch_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.log_fn = log_fn or (lambda msg: None)
        self.cancel_event = cancel_event or threading.Event()

        # gui.py 호환용
        self.crossref_mailto = crossref_mailto
        self.crossref_cache_days = crossref_cache_days

        log_dir = self.output_dir / "LOG"
        log_dir.mkdir(parents=True, exist_ok=True)

        self.index = PaperIndex(log_dir / "paper_index.sqlite3")

    def close(self) -> None:
        self.index.close()

    def log(self, message: str) -> None:
        self.log_fn(message)

    def request_cancel(self) -> None:
        self.cancel_event.set()

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def process_pdf(self, pdf_path: Path) -> None:
        if self.is_cancelled():
            self.log("[CANCEL] PDF 처리 취소됨")
            return

        pdf_path = Path(pdf_path).resolve()

        if not pdf_path.exists() or not pdf_path.is_file():
            self.log(f"[WARN] 파일이 존재하지 않음: {pdf_path}")
            return

        if pdf_path.suffix.lower() != ".pdf":
            return

        try:
            pdf_path.relative_to(self.output_dir)
            self.log(f"[SKIP] 출력 폴더 내부 파일은 건너뜀: {pdf_path}")
            return
        except ValueError:
            pass

        try:
            if self.is_cancelled():
                self.log(f"[CANCEL] 메타 추출 전 취소: {pdf_path}")
                return

            meta = extract_paper_metadata(pdf_path)

            if self.is_cancelled():
                self.log(f"[CANCEL] 메타 추출 후 취소: {pdf_path}")
                return

            field_code = meta.get("field_code", "").strip() or "ETC"
            year = normalize_year(meta.get("year", "").strip() or "UnknownYear")
            first_author = meta.get("first_author", "").strip()
            venue = meta.get("venue", "").strip()
            title = meta.get("title", "").strip() or pdf_path.stem
            doi = meta.get("doi", "").strip()
            snippet = meta.get("snippet", "").strip()

            dst_pdf = build_output_pdf_path(self.output_dir, field_code, year, pdf_path)

            if self.is_cancelled():
                self.log(f"[CANCEL] 복사 전 취소: {pdf_path}")
                return

            stored_pdf = copy_pdf_preserve_original(pdf_path, dst_pdf)

            if self.is_cancelled():
                self.log(f"[CANCEL] 복사 후 취소: {pdf_path}")
                return

            self.index.delete_by_original_path(str(pdf_path))
            self.index.add_paper(
                field_code=field_code,
                year=year,
                first_author=first_author,
                venue=venue,
                title=title,
                doi=doi,
                path=str(stored_pdf),
                original_path=str(pdf_path),
                snippet=snippet,
            )

            if self.is_cancelled():
                self.log(f"[CANCEL] 인덱싱 후 취소: {pdf_path}")
                return

            write_pdf_sidecar_metadata(
                stored_pdf.with_suffix(".json"),
                title=title,
                doi=doi,
                original_path=pdf_path,
                stored_path=stored_pdf,
                field_code=field_code,
                year=year,
                author=first_author,
                venue=venue,
            )

            self.log(f"[COPY] 원본 유지: {pdf_path}")
            self.log(f"[COPY] 정리본 저장: {stored_pdf}")

        except Exception as exc:
            self.log(f"[ERROR] PDF 처리 실패: {pdf_path} | {exc}")


class PDFCreatedHandler(FileSystemEventHandler):
    def __init__(self, organizer: PaperOrganizer):
        super().__init__()
        self.organizer = organizer

    def on_created(self, event) -> None:
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() == ".pdf":
            time.sleep(0.5)
            self.organizer.process_pdf(path)

    def on_moved(self, event) -> None:
        if event.is_directory:
            return
        path = Path(event.dest_path)
        if path.suffix.lower() == ".pdf":
            time.sleep(0.5)
            self.organizer.process_pdf(path)

    def on_modified(self, event) -> None:
        return


def scan_existing_pdfs(
    organizer: PaperOrganizer,
    root_dir: Path,
    log_fn: Callable[[str], None] | None = None,
) -> None:
    root_dir = Path(root_dir).resolve()
    logger = log_fn or (lambda msg: None)

    count = 0
    for pdf_path in root_dir.rglob("*.pdf"):
        if organizer.is_cancelled():
            logger("[CANCEL] 기존 PDF 스캔 취소됨")
            break

        if not pdf_path.is_file():
            continue

        try:
            organizer.process_pdf(pdf_path)
            count += 1
        except Exception as exc:
            logger(f"[ERROR] 기존 PDF 처리 실패: {pdf_path} | {exc}")

    logger(f"[SCAN] 기존 PDF 처리 완료: {count}건")


def run_reindex(
    args,
    log_fn: Callable[[str], None] | None = None,
    cancel_event: threading.Event | None = None,
    crossref_cache_days: int = 180,
) -> None:
    logger = log_fn or (lambda msg: None)
    output_dir = Path(args.output).resolve()
    log_dir = output_dir / "LOG"
    db_path = log_dir / "paper_index.sqlite3"

    _ = crossref_cache_days  # gui.py 호환용

    index = PaperIndex(db_path)
    try:
        cur = index.conn.cursor()
        cur.execute("DELETE FROM papers")
        index.conn.commit()

        restored = 0

        for meta_file in output_dir.rglob("*.json"):
            if cancel_event is not None and cancel_event.is_set():
                logger("[CANCEL] 재인덱싱 취소됨")
                break

            if meta_file.parent.name == "LOG":
                continue

            try:
                data = json.loads(meta_file.read_text(encoding="utf-8"))
                stored_path = data.get("stored_path", "")
                original_path = data.get("original_path", "")

                if not stored_path:
                    continue

                stored_pdf = Path(stored_path)
                if not stored_pdf.exists():
                    continue

                field_code = str(data.get("field_code", "ETC")).strip() or "ETC"
                year = normalize_year(data.get("year", "UnknownYear"))
                first_author = str(data.get("author", ""))
                venue = str(data.get("venue", ""))
                title = str(data.get("title", stored_pdf.stem))
                doi = str(data.get("doi", ""))

                index.add_paper(
                    field_code=field_code,
                    year=year,
                    first_author=first_author,
                    venue=venue,
                    title=title,
                    doi=doi,
                    path=str(stored_pdf),
                    original_path=str(original_path),
                    snippet="",
                )
                restored += 1
            except Exception as exc:
                logger(f"[WARN] 재인덱싱 메타 복원 실패: {meta_file} | {exc}")

        logger(f"[REINDEX] 완료: {restored}건")
    finally:
        index.close()