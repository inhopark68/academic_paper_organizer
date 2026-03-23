from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, List, Callable

import fitz  # PyMuPDF
import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

DOI_REGEX = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.IGNORECASE)
INVALID_FILENAME_CHARS = r'<>:"/\\|?*'
MAX_SHORT_TITLE_WORDS = 5
MAX_FILENAME_LENGTH = 160
FILE_STABLE_CHECKS = 3
FILE_STABLE_INTERVAL = 1.0
MAX_INDEX_TEXT_CHARS = 12000

CROSSREF_API_URL = "https://api.crossref.org/works/"
CROSSREF_USER_AGENT = "academic-paper-organizer/0.2.0 (mailto:your-email@example.com)"

FIELD_RULES = {
    "AI": [
        "artificial intelligence", "machine learning", "deep learning",
        "neural network", "neural networks", "transformer", "large language model",
        "llm", "reinforcement learning", "computer vision", "nlp",
        "natural language processing", "diffusion model"
    ],
    "CS": [
        "algorithm", "algorithms", "distributed system", "operating system",
        "database", "compiler", "computer architecture", "network protocol",
        "software engineering", "information retrieval", "cryptography"
    ],
    "DS": [
        "data science", "data mining", "big data", "analytics",
        "statistical learning", "forecasting", "time series"
    ],
    "BIO": [
        "genome", "genomics", "protein", "proteomics", "cell",
        "molecular biology", "gene", "genes", "gene expression",
        "bioinformatics", "microbiome"
    ],
    "MED": [
        "clinical", "patient", "patients", "therapy", "treatment",
        "diagnosis", "diagnostic", "disease", "medical", "medicine",
        "randomized trial", "epidemiology", "surgery"
    ],
    "CHEM": [
        "chemistry", "chemical", "molecule", "molecular", "synthesis",
        "catalyst", "catalysis", "polymer", "spectroscopy"
    ],
    "PHY": [
        "physics", "quantum", "relativity", "particle", "thermodynamics",
        "optics", "condensed matter", "astrophysics"
    ],
    "SOC": [
        "sociology", "social science", "behavior", "behaviour",
        "survey", "education", "psychology", "political science"
    ],
    "ECO": [
        "economics", "economic", "finance", "financial", "market",
        "inflation", "labor", "labour", "productivity", "macroeconomics"
    ],
}

LogFn = Callable[[str], None]


@dataclass
class PaperMetadata:
    doi: Optional[str] = None
    title: Optional[str] = None
    first_author: Optional[str] = None
    year: Optional[str] = None
    venue: Optional[str] = None
    abstract: Optional[str] = None
    field_code: str = "ETC"


@dataclass
class SearchRow:
    path: str
    field_code: str
    year: str
    first_author: str
    venue: str
    title: str
    doi: str
    snippet: str


def safe_print(*args: Any) -> None:
    try:
        print(*args)
    except UnicodeEncodeError:
        print(*(str(a).encode("utf-8", errors="replace").decode("utf-8") for a in args))


def _default_log(message: str) -> None:
    safe_print(message)


def _get_logger(log_fn: Optional[LogFn]) -> LogFn:
    return log_fn if log_fn is not None else _default_log


def sanitize_filename(text: str) -> str:
    text = text.strip()
    for ch in INVALID_FILENAME_CHARS:
        text = text.replace(ch, " ")
    text = re.sub(r"\s+", " ", text)
    text = text.replace("&", "and")
    text = re.sub(r"[^\w\s.-]", "", text, flags=re.UNICODE)
    text = text.strip(" ._-")
    return text


def slugify_title(title: str, max_words: int = MAX_SHORT_TITLE_WORDS) -> str:
    title = re.sub(r"\s+", " ", title).strip()
    words = re.findall(r"[A-Za-z0-9가-힣]+", title)
    if not words:
        return "Untitled"
    short_words = words[:max_words]
    return "".join(word[:1].upper() + word[1:] for word in short_words)


def normalize_author_name(name: str) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        family = parts[0]
        return sanitize_filename(family) or "UnknownAuthor"
    parts = name.split()
    return sanitize_filename(parts[-1]) if parts else "UnknownAuthor"


def guess_year_from_text(text: str) -> Optional[str]:
    current_year = time.localtime().tm_year
    years = re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", text)
    valid = [y for y in years if 1900 <= int(y) <= current_year + 1]
    if not valid:
        return None
    counts: Dict[str, int] = {}
    for y in valid[:100]:
        counts[y] = counts.get(y, 0) + 1
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[0][0]


def compute_file_hash(file_path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with file_path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def truncate_filename(filename: str, max_length: int = MAX_FILENAME_LENGTH) -> str:
    if len(filename) <= max_length:
        return filename
    stem, ext = os.path.splitext(filename)
    allowed_stem = max(10, max_length - len(ext))
    return stem[:allowed_stem].rstrip(" ._-") + ext


def wait_for_file_stable(file_path: Path) -> bool:
    last_size = -1
    stable_count = 0
    for _ in range(30):
        if not file_path.exists():
            return False
        try:
            size = file_path.stat().st_size
        except OSError:
            time.sleep(FILE_STABLE_INTERVAL)
            continue

        if size > 0 and size == last_size:
            stable_count += 1
            if stable_count >= FILE_STABLE_CHECKS:
                return True
        else:
            stable_count = 0
            last_size = size

        time.sleep(FILE_STABLE_INTERVAL)
    return False


def extract_text_from_pdf(file_path: Path, max_pages: int = 5) -> str:
    texts: List[str] = []
    with fitz.open(file_path) as doc:
        for i, page in enumerate(doc):
            if i >= max_pages:
                break
            try:
                texts.append(page.get_text("text"))
            except Exception:
                continue
    return "\n".join(texts)


def extract_pdf_metadata(file_path: Path) -> Dict[str, Any]:
    try:
        with fitz.open(file_path) as doc:
            meta = doc.metadata or {}
            return meta
    except Exception:
        return {}


def extract_doi(text: str, pdf_meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
    candidates: List[str] = []
    if pdf_meta:
        for key in ("subject", "title", "keywords"):
            value = pdf_meta.get(key)
            if value:
                candidates.extend(DOI_REGEX.findall(str(value)))

    candidates.extend(DOI_REGEX.findall(text))

    if not candidates:
        return None

    cleaned = []
    for c in candidates:
        doi = c.strip().rstrip(".,);]")
        cleaned.append(doi)

    cleaned = sorted(set(cleaned), key=len)
    return cleaned[0] if cleaned else None


def fetch_crossref_metadata(doi: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
    url = CROSSREF_API_URL + requests.utils.quote(doi)
    headers = {
        "User-Agent": CROSSREF_USER_AGENT,
        "Accept": "application/json",
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code != 200:
            return None
        data = response.json()
        if "message" not in data:
            return None
        return data["message"]
    except Exception:
        return None


def parse_crossref(message: Dict[str, Any]) -> PaperMetadata:
    title_list = message.get("title") or []
    title = title_list[0] if title_list else None

    authors = message.get("author") or []
    first_author = None
    if authors:
        author0 = authors[0]
        family = author0.get("family")
        given = author0.get("given")
        if family:
            first_author = family
        elif given:
            first_author = given

    year = None
    for key in ("published-print", "published-online", "issued", "created"):
        part = message.get(key)
        if part and isinstance(part, dict):
            date_parts = part.get("date-parts")
            if date_parts and date_parts[0]:
                year = str(date_parts[0][0])
                break

    venue = None
    container_title = message.get("container-title") or []
    if container_title:
        venue = container_title[0]
    elif message.get("publisher"):
        venue = message.get("publisher")

    abstract = message.get("abstract")
    doi = message.get("DOI")

    return PaperMetadata(
        doi=doi,
        title=title,
        first_author=first_author,
        year=year,
        venue=venue,
        abstract=abstract,
    )


def classify_field(text: str, title: Optional[str], abstract: Optional[str], venue: Optional[str]) -> str:
    corpus = " ".join([
        title or "",
        abstract or "",
        venue or "",
        text[:10000],
    ]).lower()

    scores: Dict[str, int] = {}
    for code, keywords in FIELD_RULES.items():
        score = 0
        for kw in keywords:
            score += corpus.count(kw.lower())
        if score > 0:
            scores[code] = score

    if not scores:
        return "ETC"

    return sorted(scores.items(), key=lambda x: (-x[1], x[0]))[0][0]


def build_filename(meta: PaperMetadata, original_suffix: str = ".pdf") -> str:
    field_code = meta.field_code or "ETC"
    first_author = sanitize_filename(meta.first_author or "UnknownAuthor")
    year = sanitize_filename(meta.year or "UnknownYear")
    venue = sanitize_filename(meta.venue or "UnknownVenue")
    short_title = slugify_title(meta.title or "Untitled")

    filename = f"{field_code}_{first_author}_{year}_{venue}_{short_title}{original_suffix}"
    return truncate_filename(filename)


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    idx = 2
    while True:
        candidate = parent / f"{stem}_v{idx}{suffix}"
        if not candidate.exists():
            return candidate
        idx += 1


class DOIIndex:
    def __init__(self, index_path: Path):
        self.index_path = index_path
        self.data: Dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not self.index_path.exists():
            self.data = {}
            return
        try:
            self.data = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:
            self.data = {}

    def save(self) -> None:
        ensure_dir(self.index_path.parent)
        self.index_path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def get(self, doi: str) -> Optional[str]:
        return self.data.get(doi.lower())

    def set(self, doi: str, path: str) -> None:
        self.data[doi.lower()] = path
        self.save()


class CSVLogger:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        ensure_dir(csv_path.parent)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if self.csv_path.exists():
            return
        with self.csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "status",
                "original_path",
                "result_path",
                "doi",
                "field_code",
                "title",
                "first_author",
                "year",
                "venue",
                "message",
                "file_hash",
            ])

    def log(
        self,
        status: str,
        original_path: Path,
        result_path: Optional[Path],
        meta: PaperMetadata,
        message: str,
        file_hash: Optional[str] = None,
    ) -> None:
        with self.csv_path.open("a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                status,
                str(original_path),
                str(result_path) if result_path else "",
                meta.doi or "",
                meta.field_code,
                meta.title or "",
                meta.first_author or "",
                meta.year or "",
                meta.venue or "",
                message,
                file_hash or "",
            ])


class PaperIndex:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        ensure_dir(db_path.parent)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS papers (
                path TEXT PRIMARY KEY,
                file_hash TEXT,
                doi TEXT,
                field_code TEXT,
                title TEXT,
                first_author TEXT,
                year TEXT,
                venue TEXT,
                abstract TEXT,
                text_excerpt TEXT,
                search_blob TEXT,
                indexed_at TEXT
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_papers_field ON papers(field_code)")
        self.conn.commit()

    def upsert(self, path: Path, file_hash: Optional[str], meta: PaperMetadata, text_excerpt: str) -> None:
        abstract = clean_search_text(meta.abstract or "")
        excerpt = clean_search_text(text_excerpt)
        blob = " ".join([
            meta.doi or "",
            meta.field_code or "",
            meta.title or "",
            meta.first_author or "",
            meta.year or "",
            meta.venue or "",
            abstract,
            excerpt,
        ]).lower()
        self.conn.execute(
            """
            INSERT INTO papers (
                path, file_hash, doi, field_code, title, first_author, year, venue,
                abstract, text_excerpt, search_blob, indexed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                file_hash=excluded.file_hash,
                doi=excluded.doi,
                field_code=excluded.field_code,
                title=excluded.title,
                first_author=excluded.first_author,
                year=excluded.year,
                venue=excluded.venue,
                abstract=excluded.abstract,
                text_excerpt=excluded.text_excerpt,
                search_blob=excluded.search_blob,
                indexed_at=excluded.indexed_at
            """,
            (
                str(path),
                file_hash or "",
                meta.doi or "",
                meta.field_code,
                meta.title or "",
                meta.first_author or "",
                meta.year or "",
                meta.venue or "",
                abstract,
                excerpt,
                blob,
                time.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        self.conn.commit()

    def search(
        self,
        keyword: str = "",
        author: Optional[str] = None,
        year: Optional[str] = None,
        field_code: Optional[str] = None,
        venue: Optional[str] = None,
        limit: int = 20,
    ) -> List[SearchRow]:
        clauses = ["1=1"]
        params: List[str | int] = []

        if keyword:
            clauses.append("search_blob LIKE ?")
            params.append(f"%{keyword.lower()}%")
        if author:
            clauses.append("lower(first_author) LIKE ?")
            params.append(f"%{author.lower()}%")
        if year:
            clauses.append("year = ?")
            params.append(year)
        if field_code:
            clauses.append("upper(field_code) = ?")
            params.append(field_code.upper())
        if venue:
            clauses.append("lower(venue) LIKE ?")
            params.append(f"%{venue.lower()}%")

        sql = f"""
            SELECT path, field_code, year, first_author, venue, title, doi, text_excerpt
            FROM papers
            WHERE {' AND '.join(clauses)}
            ORDER BY year DESC, first_author ASC, title ASC
            LIMIT ?
        """
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        results: List[SearchRow] = []
        for row in rows:
            results.append(
                SearchRow(
                    path=row["path"],
                    field_code=row["field_code"] or "",
                    year=row["year"] or "",
                    first_author=row["first_author"] or "",
                    venue=row["venue"] or "",
                    title=row["title"] or "",
                    doi=row["doi"] or "",
                    snippet=make_snippet(row["text_excerpt"] or "", keyword),
                )
            )
        return results

    def remove_missing_paths(self) -> int:
        rows = self.conn.execute("SELECT path FROM papers").fetchall()
        removed = 0
        for row in rows:
            if not Path(row["path"]).exists():
                self.conn.execute("DELETE FROM papers WHERE path = ?", (row["path"],))
                removed += 1
        self.conn.commit()
        return removed

    def close(self) -> None:
        self.conn.close()


def clean_search_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()[:MAX_INDEX_TEXT_CHARS]


def make_snippet(text: str, keyword: str, width: int = 180) -> str:
    cleaned = clean_search_text(text)
    if not cleaned:
        return ""
    if not keyword:
        return cleaned[:width]
    lowered = cleaned.lower()
    needle = keyword.lower()
    idx = lowered.find(needle)
    if idx == -1:
        return cleaned[:width]
    start = max(0, idx - width // 3)
    end = min(len(cleaned), idx + width)
    snippet = cleaned[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(cleaned):
        snippet = snippet + "..."
    return snippet


def build_metadata_for_pdf(file_path: Path, organizer: "PaperOrganizer") -> tuple[str, str, PaperMetadata]:
    file_hash = compute_file_hash(file_path)
    pdf_meta = extract_pdf_metadata(file_path)
    text = extract_text_from_pdf(file_path, max_pages=5)
    doi = extract_doi(text, pdf_meta)

    crossref_data = fetch_crossref_metadata(doi) if doi else None
    if crossref_data:
        meta = parse_crossref(crossref_data)
    else:
        meta = PaperMetadata(
            doi=doi,
            title=organizer._guess_title(text, pdf_meta),
            first_author=organizer._guess_author(pdf_meta),
            year=guess_year_from_text(text),
            venue=organizer._guess_venue(text, pdf_meta),
        )

    meta.field_code = classify_field(
        text=text,
        title=meta.title,
        abstract=meta.abstract,
        venue=meta.venue,
    )
    return file_hash, text, meta


def index_pdf_record(
    organizer: "PaperOrganizer",
    file_path: Path,
    file_hash: Optional[str],
    meta: PaperMetadata,
    text: str,
) -> None:
    organizer.paper_index.upsert(file_path, file_hash, meta, text)
    if meta.doi:
        organizer.doi_index.set(meta.doi, str(file_path))


class PaperOrganizer:
    def __init__(
        self,
        watch_dir: Path,
        output_dir: Path,
        log_fn: Optional[LogFn] = None,
    ):
        self.watch_dir = watch_dir
        self.output_dir = output_dir
        self.log = _get_logger(log_fn)

        self.review_dir = output_dir / "REVIEW"
        self.duplicate_dir = output_dir / "DUPLICATE"
        self.log_dir = output_dir / "LOG"
        self.logger = CSVLogger(self.log_dir / "paper_organizer_log.csv")
        self.doi_index = DOIIndex(self.log_dir / "doi_index.json")
        self.paper_index = PaperIndex(self.log_dir / "paper_index.sqlite3")

        ensure_dir(self.watch_dir)
        ensure_dir(self.output_dir)
        ensure_dir(self.review_dir)
        ensure_dir(self.duplicate_dir)
        ensure_dir(self.log_dir)

    def close(self) -> None:
        self.paper_index.close()

    def process_pdf(self, file_path: Path) -> None:
        original_path = file_path
        meta = PaperMetadata()
        result_path: Optional[Path] = None
        file_hash: Optional[str] = None
        text = ""

        try:
            if not file_path.exists() or file_path.suffix.lower() != ".pdf":
                return

            if file_path.name.endswith(".crdownload") or file_path.name.endswith(".part"):
                return

            self.log(f"[INFO] 처리 시작: {file_path}")

            stable = wait_for_file_stable(file_path)
            if not stable:
                meta.field_code = "ETC"
                moved_path = self._move_to_review(file_path, reason="file_not_stable")
                index_pdf_record(self, moved_path, file_hash, meta, text)
                self.logger.log(
                    status="REVIEW",
                    original_path=original_path,
                    result_path=moved_path,
                    meta=meta,
                    message="File not stable",
                    file_hash=file_hash,
                )
                self.log(f"[REVIEW] 파일 안정화 실패: {file_path}")
                return

            file_hash, text, meta = build_metadata_for_pdf(file_path, self)

            if meta.doi:
                existing = self.doi_index.get(meta.doi)
                if existing:
                    duplicate_target = unique_path(self.duplicate_dir / file_path.name)
                    shutil.move(str(file_path), str(duplicate_target))
                    index_pdf_record(self, duplicate_target, file_hash, meta, text)
                    self.logger.log(
                        status="DUPLICATE",
                        original_path=original_path,
                        result_path=duplicate_target,
                        meta=meta,
                        message=f"Duplicate DOI found. Existing file: {existing}",
                        file_hash=file_hash,
                    )
                    self.log(f"[DUPLICATE] {file_path} -> {duplicate_target}")
                    return

            if not meta.title or not meta.year:
                result_path = self._move_to_review(file_path, reason="metadata_incomplete")
                index_pdf_record(self, result_path, file_hash, meta, text)
                self.logger.log(
                    status="REVIEW",
                    original_path=original_path,
                    result_path=result_path,
                    meta=meta,
                    message="Missing essential metadata",
                    file_hash=file_hash,
                )
                self.log(f"[REVIEW] 필수 메타데이터 부족: {file_path}")
                return

            filename = build_filename(meta, original_suffix=".pdf")
            target_dir = self.output_dir / meta.field_code / meta.year
            ensure_dir(target_dir)
            result_path = unique_path(target_dir / filename)
            shutil.move(str(file_path), str(result_path))

            index_pdf_record(self, result_path, file_hash, meta, text)
            self.logger.log(
                status="SUCCESS",
                original_path=original_path,
                result_path=result_path,
                meta=meta,
                message="Processed successfully",
                file_hash=file_hash,
            )
            self.log(f"[OK] {original_path.name} -> {result_path}")

        except Exception as e:
            self.log(f"[ERROR] {file_path}: {e}")
            try:
                if file_path.exists():
                    result_path = self._move_to_review(file_path, reason="exception")
                    index_pdf_record(self, result_path, file_hash, meta, text)
            except Exception:
                result_path = None

            self.logger.log(
                status="ERROR",
                original_path=original_path,
                result_path=result_path,
                meta=meta,
                message=str(e),
                file_hash=file_hash,
            )

    def _move_to_review(self, file_path: Path, reason: str) -> Path:
        suffix = file_path.suffix if file_path.suffix else ".pdf"
        review_name = sanitize_filename(file_path.stem) or "unknown"
        target = unique_path(self.review_dir / f"REVIEW_{reason}_{review_name}{suffix}")
        shutil.move(str(file_path), str(target))
        return target

    def _guess_title(self, text: str, pdf_meta: Dict[str, Any]) -> Optional[str]:
        meta_title = pdf_meta.get("title")
        if meta_title and len(str(meta_title).strip()) > 5:
            return str(meta_title).strip()

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        lines = [ln for ln in lines if 10 < len(ln) < 250]

        bad_starts = ("abstract", "introduction", "keywords", "doi", "www.", "http")
        for line in lines[:20]:
            if line.lower().startswith(bad_starts):
                continue
            if re.search(r"\b(received|accepted|published)\b", line.lower()):
                continue
            return line
        return None

    def _guess_author(self, pdf_meta: Dict[str, Any]) -> Optional[str]:
        author = pdf_meta.get("author")
        if author:
            parts = re.split(r"[;,]| and ", str(author))
            if parts:
                return normalize_author_name(parts[0])
        return None

    def _guess_venue(self, text: str, pdf_meta: Dict[str, Any]) -> Optional[str]:
        subject = pdf_meta.get("subject")
        if subject and len(str(subject).strip()) > 2:
            return str(subject).strip()

        patterns = [
            r"\bProceedings of [^\n]{5,100}",
            r"\bInternational Conference on [^\n]{5,100}",
            r"\bJournal of [^\n]{3,100}",
            r"\bIEEE [^\n]{3,100}",
            r"\bACM [^\n]{3,100}",
            r"\bNature\b[^\n]{0,50}",
            r"\bScience\b[^\n]{0,50}",
            r"\bCell\b[^\n]{0,50}",
        ]
        for pat in patterns:
            match = re.search(pat, text, flags=re.IGNORECASE)
            if match:
                return match.group(0).strip()
        return None


class PDFCreatedHandler(FileSystemEventHandler):
    def __init__(self, organizer: PaperOrganizer):
        self.organizer = organizer
        super().__init__()

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() == ".pdf":
            self.organizer.process_pdf(path)

    def on_moved(self, event):
        if event.is_directory:
            return
        path = Path(event.dest_path)
        if path.suffix.lower() == ".pdf":
            self.organizer.process_pdf(path)


def scan_existing_pdfs(
    organizer: PaperOrganizer,
    directory: Path,
    log_fn: Optional[LogFn] = None,
) -> None:
    log = _get_logger(log_fn)
    for file_path in sorted(directory.iterdir()):
        if file_path.is_file() and file_path.suffix.lower() == ".pdf":
            log(f"[INFO] 스캔 중: {file_path}")
            organizer.process_pdf(file_path)


def index_existing_library(output_dir: Path, log_fn: Optional[LogFn] = None) -> int:
    log = _get_logger(log_fn)
    organizer = PaperOrganizer(output_dir, output_dir, log_fn=log)
    indexed = 0
    try:
        removed = organizer.paper_index.remove_missing_paths()
        if removed:
            log(f"[INFO] 누락 파일 인덱스 정리: {removed}건")

        for file_path in output_dir.rglob("*.pdf"):
            if any(part in {"LOG"} for part in file_path.parts):
                continue
            if file_path.is_file():
                organizer.process_pdf(file_path)
                indexed += 1
    finally:
        organizer.close()
    return indexed


def run_watch(args: argparse.Namespace, log_fn: Optional[LogFn] = None) -> int:
    log = _get_logger(log_fn)

    watch_dir = Path(args.watch).expanduser().resolve()
    output_dir = Path(args.output).expanduser().resolve()

    organizer = PaperOrganizer(watch_dir=watch_dir, output_dir=output_dir, log_fn=log)

    try:
        if args.once:
            log("[INFO] 1회 스캔 시작")
            scan_existing_pdfs(organizer, watch_dir, log_fn=log)
            log("[INFO] 완료")
            return 0

        observer = Observer()
        handler = PDFCreatedHandler(organizer)
        observer.schedule(handler, str(watch_dir), recursive=False)

        def shutdown(signum=None, frame=None):
            log("\n[INFO] 종료 중...")
            observer.stop()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        log(f"[INFO] 감시 시작: {watch_dir}")
        log(f"[INFO] 출력 폴더: {output_dir}")
        log("[INFO] 기존 PDF 1회 스캔 중...")
        scan_existing_pdfs(organizer, watch_dir, log_fn=log)

        observer.start()
        try:
            while observer.is_alive():
                observer.join(timeout=1)
        finally:
            observer.stop()
            observer.join()
        return 0
    finally:
        organizer.close()


def run_search(args: argparse.Namespace, log_fn: Optional[LogFn] = None) -> int:
    log = _get_logger(log_fn)

    output_dir = Path(args.output).expanduser().resolve()
    db_path = output_dir / "LOG" / "paper_index.sqlite3"
    if not db_path.exists():
        log("[INFO] 검색 인덱스가 없습니다. 먼저 논문을 처리하거나 reindex를 실행하세요.")
        return 1

    index = PaperIndex(db_path)
    try:
        rows = index.search(
            keyword=args.keyword or "",
            author=args.author,
            year=args.year,
            field_code=args.field,
            venue=args.venue,
            limit=args.limit,
        )
    finally:
        index.close()

    if not rows:
        log("검색 결과가 없습니다.")
        return 0

    for i, row in enumerate(rows, start=1):
        log(f"[{i}] {row.title or '(제목없음)'}")
        log(f"    field={row.field_code} | year={row.year} | author={row.first_author} | venue={row.venue}")
        if row.doi:
            log(f"    doi={row.doi}")
        if row.snippet:
            log(f"    snippet={row.snippet}")
        log(f"    path={row.path}")
        log("")
    return 0


def run_reindex(args: argparse.Namespace, log_fn: Optional[LogFn] = None) -> int:
    log = _get_logger(log_fn)

    output_dir = Path(args.output).expanduser().resolve()
    organizer = PaperOrganizer(output_dir, output_dir, log_fn=log)
    indexed = 0
    try:
        removed = organizer.paper_index.remove_missing_paths()
        if removed:
            log(f"[INFO] 누락 파일 인덱스 정리: {removed}건")

        for file_path in output_dir.rglob("*.pdf"):
            if any(part in {"LOG"} for part in file_path.parts):
                continue
            try:
                file_hash, text, meta = build_metadata_for_pdf(file_path, organizer)
                index_pdf_record(organizer, file_path, file_hash, meta, text)
                indexed += 1
            except Exception as e:
                log(f"[WARN] 인덱싱 실패: {file_path} | {e}")
    finally:
        organizer.close()

    log(f"[INFO] 인덱싱 완료: {indexed}건")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="학술논문 PDF 자동 정리 및 검색")
    subparsers = parser.add_subparsers(dest="command")

    watch_parser = subparsers.add_parser("watch", help="다운로드 폴더를 감시하며 논문 PDF를 자동 정리")
    watch_parser.add_argument("--watch", required=True, help="감시할 다운로드 폴더")
    watch_parser.add_argument("--output", required=True, help="정리 결과 저장 폴더")
    watch_parser.add_argument("--once", action="store_true", help="감시 없이 현재 폴더 내 PDF만 1회 처리")
    watch_parser.set_defaults(func=run_watch)

    search_parser = subparsers.add_parser("search", help="분류된 논문을 키워드/저자/연도/분야/저널로 검색")
    search_parser.add_argument("keyword", nargs="?", default="", help="본문/제목/초록/메타데이터 검색어")
    search_parser.add_argument("--output", required=True, help="정리 결과 저장 폴더")
    search_parser.add_argument("--author", help="첫 저자 기준 검색")
    search_parser.add_argument("--year", help="연도 검색")
    search_parser.add_argument("--field", help="분야 코드 검색, 예: AI, BIO")
    search_parser.add_argument("--venue", help="저널/학회명 검색")
    search_parser.add_argument("--limit", type=int, default=20, help="최대 결과 수")
    search_parser.set_defaults(func=run_search)

    reindex_parser = subparsers.add_parser("reindex", help="기존 라이브러리를 다시 인덱싱")
    reindex_parser.add_argument("--output", required=True, help="정리 결과 저장 폴더")
    reindex_parser.set_defaults(func=run_reindex)

    return parser


def main() -> int:
    parser = build_parser()

    if len(sys.argv) > 1 and sys.argv[1].startswith("--"):
        args = parser.parse_args(["watch", *sys.argv[1:]])
    else:
        args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())