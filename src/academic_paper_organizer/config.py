from __future__ import annotations

import hashlib
import html
import json
import re
import shutil
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
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
        "year": year,
        "author": author,
        "venue": venue,
    }
    meta_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def make_crossref_cache_key(*parts: str) -> str:
    normalized = "||".join((part or "").strip().lower() for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class PaperIndex:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
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

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crossref_cache (
                cache_key TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT,
                first_author TEXT,
                venue TEXT,
                year TEXT,
                doi TEXT,
                snippet TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self.conn.commit()
        self._ensure_original_path_column()

    def _ensure_original_path_column(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(papers)")
        columns = [row[1] for row in cur.fetchall()]
        if "original_path" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN original_path TEXT")
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()

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
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO papers (
                field_code, year, first_author, venue, title, doi, path, original_path, snippet
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                field_code,
                year,
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
        limit: int = 100,
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
            params.append(year)

        if field_code:
            sql += " AND field_code = ?"
            params.append(field_code)

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

    def get_crossref_cache(self, cache_key: str, max_age_days: int = 180) -> dict[str, str]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                source,
                title,
                first_author,
                venue,
                year,
                doi,
                snippet,
                fetched_at
            FROM crossref_cache
            WHERE cache_key = ?
              AND datetime(fetched_at) >= datetime('now', ?)
            """,
            (cache_key, f"-{max_age_days} days"),
        )
        row = cur.fetchone()
        if not row:
            return {}

        return {
            "source": row["source"] or "",
            "title": row["title"] or "",
            "first_author": row["first_author"] or "",
            "venue": row["venue"] or "",
            "year": row["year"] or "",
            "doi": row["doi"] or "",
            "snippet": row["snippet"] or "",
            "fetched_at": row["fetched_at"] or "",
        }

    def put_crossref_cache(self, cache_key: str, source: str, data: dict[str, str]) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO crossref_cache (
                cache_key, source, title, first_author, venue, year, doi, snippet, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cache_key) DO UPDATE SET
                source = excluded.source,
                title = excluded.title,
                first_author = excluded.first_author,
                venue = excluded.venue,
                year = excluded.year,
                doi = excluded.doi,
                snippet = excluded.snippet,
                fetched_at = CURRENT_TIMESTAMP
            """,
            (
                cache_key,
                source,
                data.get("title", ""),
                data.get("first_author", ""),
                data.get("venue", ""),
                data.get("year", ""),
                data.get("doi", ""),
                data.get("snippet", ""),
            ),
        )
        self.conn.commit()

    def clear_crossref_cache(self) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM crossref_cache")
        self.conn.commit()

    def purge_old_crossref_cache(self, max_age_days: int = 180) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            DELETE FROM crossref_cache
            WHERE datetime(fetched_at) < datetime('now', ?)
            """,
            (f"-{max_age_days} days",),
        )
        deleted = cur.rowcount
        self.conn.commit()
        return deleted


def normalize_text_lines(text: str) -> list[str]:
    lines = [line.strip() for line in text.splitlines()]
    lines = [re.sub(r"\s+", " ", line) for line in lines]
    return [line for line in lines if line]


def normalize_for_match(text: str) -> str:
    text = text.lower()
    text = html.unescape(text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def similarity_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0

    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens or not b_tokens:
        return 0.0

    intersection = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return intersection / union


def infer_field_code_from_text(text: str) -> str:
    t = text.lower()

    rules: list[tuple[str, list[str]]] = [
        ("NLP", ["nlp", "language model", "large language model", "llm", "transformer", "bert", "gpt", "token", "prompt"]),
        ("CV", ["computer vision", "vision", "image", "video", "object detection", "segmentation", "recognition"]),
        ("RL", ["reinforcement learning", "policy gradient", "q-learning", "markov decision process", "mdp"]),
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
        ("ML", ["machine learning", "xgboost", "lightgbm", "svm"]),
        ("AI", ["artificial intelligence", "ai", "planning", "reasoning"]),
        ("PHY", ["physics", "quantum", "particle"]),
        ("CHEM", ["chemistry", "chemical", "reaction"]),
        ("ECO", ["economics", "econometrics", "market"]),
        ("EDU", ["education", "learning analytics", "intelligent tutoring"]),
    ]

    for code, keywords in rules:
        if any(keyword in t for keyword in keywords):
            return code

    if re.search(r"\bml\b", t):
        return "ML"
    if re.search(r"\bai\b", t):
        return "AI"
    if re.search(r"\brl\b", t):
        return "RL"
    if re.search(r"\bnlp\b", t):
        return "NLP"
    if re.search(r"\bhci\b", t):
        return "HCI"
    if re.search(r"\basr\b|\btts\b", t):
        return "SP"

    return "ETC"


def extract_first_page_text(pdf_path: Path, max_chars: int = 6000) -> str:
    try:
        from pypdf import PdfReader

        reader = PdfReader(str(pdf_path))
        if not reader.pages:
            return ""

        text = reader.pages[0].extract_text() or ""
        text = text.replace("\x00", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text[:max_chars]
    except Exception:
        return ""


def extract_year_from_pdf_metadata(pdf_path: Path) -> str:
    try:
        from pypdf import PdfReader

        reader = PdfReader(str(pdf_path))
        meta = reader.metadata
        if not meta:
            return ""

        for key in ("/CreationDate", "/ModDate"):
            value = meta.get(key)
            if not value:
                continue

            match = re.search(r"(19|20)\d{2}", str(value))
            if match:
                return match.group(0)
    except Exception:
        pass

    return ""


def extract_year_from_file_stat(pdf_path: Path) -> str:
    try:
        stat = pdf_path.stat()
        year = datetime.fromtimestamp(stat.st_mtime).year
        if 1900 <= year <= 2100:
            return str(year)
    except Exception:
        pass
    return ""


def extract_doi_from_text(text: str) -> str:
    match = re.search(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", text, re.IGNORECASE)
    return match.group(0).rstrip(".,;)]}") if match else ""


def extract_title_from_text(text: str, fallback: str) -> str:
    lines = normalize_text_lines(text)
    if not lines:
        return fallback

    ignore_patterns = [
        r"^abstract$",
        r"^introduction$",
        r"^keywords?$",
        r"^arxiv\b",
        r"^submitted to\b",
        r"^proceedings of\b",
        r"^\d+$",
        r"^page \d+$",
    ]

    candidates: list[str] = []
    for line in lines[:15]:
        low = line.lower()

        if len(line) < 8 or len(line) > 300:
            continue
        if any(re.search(pat, low) for pat in ignore_patterns):
            continue
        if re.match(r"^(doi|https?:|www\.)", low):
            continue
        if "@" in line:
            continue

        candidates.append(line)

    if not candidates:
        return fallback

    def score(line: str) -> tuple[int, int]:
        word_count = len(line.split())
        has_many_letters = sum(ch.isalpha() for ch in line) >= 10
        penalty = 0
        if re.search(r"\b(university|department|school|laboratory|lab)\b", line, re.IGNORECASE):
            penalty += 3
        if re.search(r"\b(abstract|introduction|keywords)\b", line, re.IGNORECASE):
            penalty += 5
        bonus = 0
        if 4 <= word_count <= 20:
            bonus += 3
        if has_many_letters:
            bonus += 2
        return (bonus - penalty, -abs(12 - word_count))

    best = max(candidates, key=score)
    return best or fallback


def extract_first_author_from_text(text: str, title: str) -> str:
    lines = normalize_text_lines(text)
    if not lines:
        return ""

    title_index = -1
    for i, line in enumerate(lines[:20]):
        if line == title:
            title_index = i
            break

    search_range = lines[title_index + 1:title_index + 8] if title_index >= 0 else lines[:10]

    def looks_like_author_line(line: str) -> bool:
        if len(line) > 200 or len(line) < 3:
            return False
        if "@" in line:
            return False
        if re.search(r"\b(abstract|introduction|keywords|university|department|school|laboratory|lab)\b", line, re.IGNORECASE):
            return False
        if re.search(r"\b(doi|arxiv|proceedings)\b", line, re.IGNORECASE):
            return False

        name_like = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z'\-]+){0,2}\b", line)
        return len(name_like) >= 1

    for line in search_range:
        if not looks_like_author_line(line):
            continue

        cleaned = re.sub(r"[*†‡0-9,]", " ", line)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        parts = re.split(r"\band\b|,|;", cleaned, flags=re.IGNORECASE)
        for part in parts:
            part = part.strip()
            if re.fullmatch(r"[A-Z][a-z]+(?:\s+[A-Z][a-z'\-]+){0,2}", part):
                return part

        match = re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z'\-]+){0,2}\b", cleaned)
        if match:
            return match.group(0)

    return ""


def extract_venue_from_text(text: str) -> str:
    lines = normalize_text_lines(text)
    joined = "\n".join(lines[:40])

    patterns = [
        r"\bProceedings of the ([^\n.]+)",
        r"\bIn ([A-Z][A-Za-z0-9&,\-:() ]{5,120})",
        r"\bPublished in ([A-Z][A-Za-z0-9&,\-:() ]{5,120})",
        r"\bAppeared in ([A-Z][A-Za-z0-9&,\-:() ]{5,120})",
        r"\bJournal of ([A-Z][A-Za-z0-9&,\-:() ]{3,120})",
        r"\b([A-Z][A-Za-z& ]+ Conference)\b",
        r"\b([A-Z][A-Za-z& ]+ Symposium)\b",
        r"\b([A-Z][A-Za-z& ]+ Workshop)\b",
    ]

    for pat in patterns:
        match = re.search(pat, joined, re.IGNORECASE)
        if match:
            venue = match.group(1).strip()
            venue = re.sub(r"\s+", " ", venue)
            return venue.rstrip(".,;:")

    known_venues = [
        "NeurIPS", "ICML", "ICLR", "ACL", "EMNLP", "NAACL", "COLING",
        "CVPR", "ICCV", "ECCV", "AAAI", "IJCAI", "KDD", "WWW", "SIGIR",
        "CHI", "UAI", "AISTATS", "Interspeech", "ICASSP", "MICCAI",
        "Nature", "Science",
    ]

    found = [v for v in known_venues if re.search(rf"\b{re.escape(v)}\b", joined, re.IGNORECASE)]
    if found:
        return Counter(found).most_common(1)[0][0]

    return ""


def parse_crossref_work_message(message: dict) -> dict[str, str]:
    if not isinstance(message, dict):
        return {}

    title_list = message.get("title") or []
    title = title_list[0].strip() if title_list and isinstance(title_list[0], str) else ""

    author_list = message.get("author") or []
    first_author = ""
    if author_list and isinstance(author_list[0], dict):
        given = str(author_list[0].get("given", "")).strip()
        family = str(author_list[0].get("family", "")).strip()
        first_author = " ".join(part for part in [given, family] if part).strip()

    venue = ""
    container_title = message.get("container-title") or []
    if container_title and isinstance(container_title[0], str):
        venue = container_title[0].strip()

    year = ""
    for key in ("published-print", "published-online", "issued", "created"):
        date_part = message.get(key, {})
        date_parts = date_part.get("date-parts") if isinstance(date_part, dict) else None
        if date_parts and isinstance(date_parts, list) and date_parts[0]:
            first = date_parts[0]
            if isinstance(first, list) and first:
                maybe_year = str(first[0]).strip()
                if re.fullmatch(r"(19|20)\d{2}", maybe_year):
                    year = maybe_year
                    break

    doi_value = str(message.get("DOI", "")).strip()

    abstract = str(message.get("abstract", "") or "").strip()
    abstract = html.unescape(abstract)
    abstract = re.sub(r"<[^>]+>", " ", abstract)
    abstract = re.sub(r"\s+", " ", abstract).strip()

    score = message.get("score")
    score_text = str(score).strip() if score is not None else ""

    return {
        "title": title,
        "first_author": first_author,
        "venue": venue,
        "year": year,
        "doi": doi_value,
        "snippet": abstract[:500] if abstract else "",
        "score": score_text,
    }


def fetch_crossref_metadata_by_doi(
    doi: str,
    timeout: int = 10,
    mailto: str = "your-email@example.com",
) -> dict[str, str]:
    if not doi:
        return {}

    try:
        encoded_doi = urllib.parse.quote(doi, safe="")
        url = f"https://api.crossref.org/works/{encoded_doi}?mailto={urllib.parse.quote(mailto)}"

        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": f"academic-paper-organizer/1.0 (mailto:{mailto})",
                "Accept": "application/json",
            },
        )

        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return {}

            payload = json.loads(resp.read().decode("utf-8"))
            message = payload.get("message", {}) if isinstance(payload, dict) else {}

        parsed = parse_crossref_work_message(message)
        if not parsed.get("doi"):
            parsed["doi"] = doi
        return parsed

    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        return {}
    except Exception:
        return {}


def fetch_crossref_metadata_by_title(
    title: str,
    first_author: str = "",
    year: str = "",
    timeout: int = 10,
    mailto: str = "your-email@example.com",
) -> dict[str, str]:
    clean_title = re.sub(r"\s+", " ", title).strip()
    if not clean_title:
        return {}

    try:
        bibliographic_parts = [clean_title]
        if year and re.fullmatch(r"(19|20)\d{2}", year):
            bibliographic_parts.append(year)

        params = {
            "query.bibliographic": " ".join(bibliographic_parts),
            "rows": "5",
            "select": "DOI,title,author,container-title,issued,published-print,published-online,created,abstract,score",
            "mailto": mailto,
        }

        if first_author.strip():
            params["query.author"] = first_author.strip()

        url = "https://api.crossref.org/works?" + urllib.parse.urlencode(params)

        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": f"academic-paper-organizer/1.0 (mailto:{mailto})",
                "Accept": "application/json",
            },
        )

        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return {}

            payload = json.loads(resp.read().decode("utf-8"))
            message = payload.get("message", {}) if isinstance(payload, dict) else {}
            items = message.get("items", []) if isinstance(message, dict) else []

        if not items:
            return {}

        best_item = None
        best_score = -1.0

        norm_input_title = normalize_for_match(clean_title)
        norm_input_author = normalize_for_match(first_author)

        for item in items:
            parsed = parse_crossref_work_message(item)
            candidate_title = parsed.get("title", "")
            candidate_author = parsed.get("first_author", "")
            candidate_year = parsed.get("year", "")
            candidate_score = float(parsed.get("score") or 0.0)

            title_similarity = similarity_ratio(norm_input_title, normalize_for_match(candidate_title))
            author_similarity = similarity_ratio(norm_input_author, normalize_for_match(candidate_author)) if norm_input_author else 0.0
            year_bonus = 0.08 if year and candidate_year == year else 0.0

            combined_score = (
                (title_similarity * 0.75)
                + (author_similarity * 0.15)
                + year_bonus
                + min(candidate_score / 100.0, 0.10)
            )

            if combined_score > best_score:
                best_score = combined_score
                best_item = parsed

        if not best_item:
            return {}

        final_title_similarity = similarity_ratio(
            norm_input_title,
            normalize_for_match(best_item.get("title", "")),
        )
        if final_title_similarity < 0.55:
            return {}

        return best_item

    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        return {}
    except Exception:
        return {}


def enrich_metadata_with_crossref_cache(
    index: PaperIndex,
    *,
    title: str,
    first_author: str,
    year: str,
    doi: str,
    mailto: str = "your-email@example.com",
    cache_days: int = 180,
    log_fn: Callable[[str], None] | None = None,
) -> dict[str, str]:
    logger = log_fn or (lambda msg: None)

    if doi:
        doi_key = make_crossref_cache_key("doi", doi)
        cached = index.get_crossref_cache(doi_key, max_age_days=cache_days)
        if cached:
            logger(f"[CACHE HIT] Crossref DOI: {doi}")
            return cached

        fetched = fetch_crossref_metadata_by_doi(doi=doi, mailto=mailto)
        if fetched:
            index.put_crossref_cache(doi_key, "doi", fetched)
            logger(f"[CACHE SAVE] Crossref DOI: {doi}")
            return fetched

    title_key = make_crossref_cache_key("title", title, first_author, year)
    cached = index.get_crossref_cache(title_key, max_age_days=cache_days)
    if cached:
        logger(f"[CACHE HIT] Crossref TITLE: {title}")
        return cached

    fetched = fetch_crossref_metadata_by_title(
        title=title,
        first_author=first_author,
        year=year if year != "UnknownYear" else "",
        mailto=mailto,
    )
    if fetched:
        index.put_crossref_cache(title_key, "title", fetched)
        logger(f"[CACHE SAVE] Crossref TITLE: {title}")

        fetched_doi = fetched.get("doi", "").strip()
        if fetched_doi:
            doi_key = make_crossref_cache_key("doi", fetched_doi)
            index.put_crossref_cache(doi_key, "doi", fetched)

        return fetched

    return {}


def extract_paper_metadata(pdf_path: Path) -> dict[str, str]:
    name = pdf_path.stem

    year_match = re.search(r"(19|20)\d{2}", name)
    year = year_match.group(0) if year_match else ""

    if not year:
        year = extract_year_from_pdf_metadata(pdf_path)

    if not year:
        year = extract_year_from_file_stat(pdf_path)

    first_page_text = extract_first_page_text(pdf_path)
    combined_text = f"{name}\n{first_page_text}".strip()

    field_code = infer_field_code_from_text(combined_text)
    doi = extract_doi_from_text(first_page_text)
    title = extract_title_from_text(first_page_text, fallback=name)
    first_author = extract_first_author_from_text(first_page_text, title=title)
    venue = extract_venue_from_text(first_page_text)
    snippet = re.sub(r"\s+", " ", first_page_text)[:500] if first_page_text else ""

    return {
        "field_code": field_code or "ETC",
        "year": year or "UnknownYear",
        "first_author": first_author,
        "venue": venue,
        "title": title,
        "doi": doi,
        "snippet": snippet,
    }


def build_output_pdf_path(output_dir: Path, field_code: str, year: str, src_pdf: Path) -> Path:
    safe_field = field_code.strip() or "ETC"
    safe_year = year.strip() or "UnknownYear"
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
        self.crossref_mailto = crossref_mailto
        self.crossref_cache_days = max(1, int(crossref_cache_days))

        log_dir = self.output_dir / "LOG"
        log_dir.mkdir(parents=True, exist_ok=True)

        self.index = PaperIndex(log_dir / "paper_index.sqlite3")
        self.index.purge_old_crossref_cache(self.crossref_cache_days)

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

            crossref = enrich_metadata_with_crossref_cache(
                self.index,
                title=meta.get("title", "") or pdf_path.stem,
                first_author=meta.get("first_author", ""),
                year=meta.get("year", ""),
                doi=meta.get("doi", ""),
                mailto=self.crossref_mailto,
                cache_days=self.crossref_cache_days,
                log_fn=self.log,
            )

            if crossref:
                meta["title"] = crossref.get("title") or meta.get("title", "")
                meta["first_author"] = crossref.get("first_author") or meta.get("first_author", "")
                meta["venue"] = crossref.get("venue") or meta.get("venue", "")
                meta["year"] = crossref.get("year") or meta.get("year", "")
                meta["doi"] = crossref.get("doi") or meta.get("doi", "")
                meta["snippet"] = crossref.get("snippet") or meta.get("snippet", "")

                enrich_text = " ".join(
                    part for part in [
                        meta.get("title", ""),
                        meta.get("first_author", ""),
                        meta.get("venue", ""),
                        meta.get("snippet", ""),
                        pdf_path.stem,
                    ] if part
                )
                meta["field_code"] = infer_field_code_from_text(enrich_text)

            if self.is_cancelled():
                self.log(f"[CANCEL] 메타 추출 후 취소: {pdf_path}")
                return

            field_code = meta.get("field_code", "").strip() or "ETC"
            year = meta.get("year", "").strip() or "UnknownYear"
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

    index = PaperIndex(db_path)
    try:
        index.purge_old_crossref_cache(crossref_cache_days)

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

                index.add_paper(
                    field_code=str(data.get("field_code", "ETC")),
                    year=str(data.get("year", "UnknownYear")),
                    first_author=str(data.get("author", "")),
                    venue=str(data.get("venue", "")),
                    title=str(data.get("title", stored_pdf.stem)),
                    doi=str(data.get("doi", "")),
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