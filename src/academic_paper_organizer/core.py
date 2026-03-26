from __future__ import annotations

import csv
import json
import re
import shutil
import sqlite3
import threading
import time
import requests
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
from urllib.parse import quote_plus

from pypdf import PdfReader
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
    authors_json: str
    venue: str
    title: str
    doi: str
    path: str
    original_path: str
    snippet: str
    doc_type: str
    doc_score: str
    doc_reasons_json: str
    scie: str = ""
    impact_factor: str = ""


def load_journal_metrics(csv_path: str | Path = "journal_metrics.csv") -> dict[str, tuple[str, str]]:
    data: dict[str, tuple[str, str]] = {}
    try:
        path = Path(csv_path)
        if not path.is_absolute():
            path = Path(__file__).resolve().parent / path
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = str(row.get("Journal Name", "")).strip()
                if not name:
                    continue
                data[name.casefold()] = (
                    str(row.get("SCIE", "")).strip(),
                    str(row.get("Impact Factor", "")).strip(),
                )
    except Exception:
        return {}
    return data


JOURNAL_DB = load_journal_metrics()


def fetch_openalex_journal_metrics(venue: str) -> tuple[str, str]:
    venue = str(venue or "").strip()
    if not venue:
        return "", ""

    try:
        res = requests.get(
            "https://api.openalex.org/sources",
            params={"search": venue, "per-page": 1},
            timeout=8,
        )
        res.raise_for_status()
        payload = res.json() or {}
        results = payload.get("results") or []
        if not results:
            return "", ""

        item = results[0] or {}
        is_in_doaj = bool(item.get("is_in_doaj"))
        is_oa = bool(item.get("is_oa"))
        summary_stats = item.get("summary_stats") or {}
        cited_half_life = summary_stats.get("2yr_mean_citedness")
        works_count = item.get("works_count") or 0
        cited_by_count = item.get("cited_by_count") or 0

        scie = "SCIE" if (works_count or cited_by_count or is_in_doaj or is_oa) else "Unknown"
        if cited_half_life not in (None, ""):
            impact = f"OA:{float(cited_half_life):.2f}"
        elif cited_by_count:
            impact = f"OA-CITES:{round(float(cited_by_count) / 1000.0, 2):.2f}"
        else:
            impact = ""

        return scie, impact
    except Exception:
        return "", ""


def fetch_journal_metrics(venue: str) -> tuple[str, str]:
    venue = str(venue or "").strip()
    if not venue:
        return "", ""

    key = venue.casefold()
    if key in JOURNAL_DB:
        return JOURNAL_DB[key]

    return fetch_openalex_journal_metrics(venue)


def _current_year() -> int:
    return datetime.now().year


def normalize_year(
    year_value: str | int | None,
    *,
    allow_historic: bool = False,
) -> str:
    """
    연도를 안전하게 정규화한다.

    기본 정책:
    - 숫자 4자리만 인정
    - 일반 논문 기준 1950 ~ 현재연도+1
    - allow_historic=True면 1800년 이후 허용
    - 1917 같은 오인식은 2017로 자동 보정
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

    # OCR / 추출 오인식 보정: 1917 -> 2017
    if not allow_historic and 1900 <= year <= 1930:
        corrected = year + 100
        if 1950 <= corrected <= max_year:
            return str(corrected)

    return "UnknownYear"


def extract_year_candidates(text: str) -> list[str]:
    if not text:
        return []
    return re.findall(r"(18\d{2}|19\d{2}|20\d{2})", text)


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
    authors: list[str],
    venue: str,
    doc_type: str,
    doc_score: str,
    doc_reasons_json: str,
) -> None:
    payload = {
        "title": title,
        "doi": doi,
        "original_path": str(original_path),
        "stored_path": str(stored_path),
        "field_code": field_code,
        "year": normalize_year(year),
        "author": author,
        "authors": authors,
        "venue": venue,
        "doc_type": doc_type,
        "doc_score": doc_score,
        "doc_reasons_json": doc_reasons_json,
    }
    meta_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def extract_pdf_first_page_text(pdf_path: Path) -> str:
    """
    PDF 첫 페이지 텍스트를 추출한다.
    실패하면 빈 문자열 반환.
    """
    try:
        reader = PdfReader(str(pdf_path))
        if not reader.pages:
            return ""
        text = reader.pages[0].extract_text() or ""
        return text.strip()
    except Exception:
        return ""


def extract_pdf_document_info(pdf_path: Path) -> dict[str, str]:
    """
    PDF 메타데이터(Title/Author/Subject 등)를 읽는다.
    실패하면 빈 dict 반환.
    """
    try:
        reader = PdfReader(str(pdf_path))
        meta = reader.metadata or {}

        def _clean(value) -> str:
            if value is None:
                return ""
            return str(value).strip()

        return {
            "title": _clean(meta.get("/Title")),
            "author": _clean(meta.get("/Author")),
            "subject": _clean(meta.get("/Subject")),
            "keywords": _clean(meta.get("/Keywords")),
            "creator": _clean(meta.get("/Creator")),
            "producer": _clean(meta.get("/Producer")),
        }
    except Exception:
        return {}


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
                authors_json TEXT,
                venue TEXT,
                title TEXT,
                doi TEXT,
                path TEXT,
                original_path TEXT,
                snippet TEXT,
                doc_type TEXT,
                doc_score TEXT,
                doc_reasons_json TEXT,
                scie TEXT,
                impact_factor TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.commit()
        self._ensure_original_path_column()
        self._ensure_authors_json_column()
        self._ensure_doc_type_columns()
        self._ensure_journal_metric_columns()
        self._ensure_crossref_cache_table()

    def _ensure_original_path_column(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(papers)")
        columns = [row[1] for row in cur.fetchall()]
        if "original_path" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN original_path TEXT")
            self.conn.commit()

    def _ensure_authors_json_column(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(papers)")
        columns = [row[1] for row in cur.fetchall()]
        if "authors_json" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN authors_json TEXT")
            self.conn.commit()

    def _ensure_doc_type_columns(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(papers)")
        columns = [row[1] for row in cur.fetchall()]

        if "doc_type" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN doc_type TEXT")
        if "doc_score" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN doc_score TEXT")
        if "doc_reasons_json" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN doc_reasons_json TEXT")

        self.conn.commit()

    def _ensure_journal_metric_columns(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(papers)")
        columns = [row[1] for row in cur.fetchall()]

        if "scie" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN scie TEXT")
        if "impact_factor" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN impact_factor TEXT")

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

    def get_crossref_cache(self, key: str, max_age_days: int = 180) -> str | None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT value
            FROM crossref_cache
            WHERE key = ?
              AND datetime(created_at) >= datetime('now', ?)
            """,
            (key, f"-{max_age_days} days"),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0]

    def set_crossref_cache(self, key: str, value: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO crossref_cache (key, value, created_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                created_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )
        self.conn.commit()

    def add_paper(
        self,
        *,
        field_code: str,
        year: str,
        first_author: str,
        authors_json: str,
        venue: str,
        title: str,
        doi: str,
        path: str,
        original_path: str,
        snippet: str,
        doc_type: str,
        doc_score: str,
        doc_reasons_json: str,
        scie: str = "",
        impact_factor: str = "",
    ) -> None:
        safe_year = normalize_year(year)
        if not scie and not impact_factor:
            scie, impact_factor = fetch_journal_metrics(venue)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO papers (
                field_code, year, first_author, authors_json, venue, title, doi,
                path, original_path, snippet, doc_type, doc_score, doc_reasons_json,
                scie, impact_factor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                field_code,
                safe_year,
                first_author,
                authors_json,
                venue,
                title,
                doi,
                path,
                original_path,
                snippet,
                doc_type,
                doc_score,
                doc_reasons_json,
                scie,
                impact_factor,
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
        doc_type: str | None = None,
        file_path: str | None = None,
        limit: int = 50,
    ) -> list[PaperRow]:
        sql = """
            SELECT
                field_code,
                year,
                first_author,
                COALESCE(authors_json, '[]') AS authors_json,
                venue,
                title,
                doi,
                path,
                COALESCE(original_path, '') AS original_path,
                snippet,
                COALESCE(doc_type, 'unknown') AS doc_type,
                COALESCE(doc_score, '') AS doc_score,
                COALESCE(doc_reasons_json, '[]') AS doc_reasons_json,
                COALESCE(scie, '') AS scie,
                COALESCE(impact_factor, '') AS impact_factor
            FROM papers
            WHERE 1=1
        """
        params: list[str | int] = []

        if keyword:
            like = f"%{keyword}%"
            sql += " AND (title LIKE ? OR snippet LIKE ? OR doi LIKE ?)"
            params.extend([like, like, like])

        if author:
            like_author = f"%{author}%"
            sql += " AND (first_author LIKE ? OR authors_json LIKE ?)"
            params.extend([like_author, like_author])

        if year:
            sql += " AND year = ?"
            params.append(normalize_year(year))

        if field_code:
            sql += " AND field_code LIKE ?"
            params.append(f"%{field_code}%")

        if venue:
            sql += " AND venue LIKE ?"
            params.append(f"%{venue}%")

        if doc_type:
            sql += " AND COALESCE(doc_type, 'unknown') = ?"
            params.append(doc_type)

        if file_path:
            normalized_path = str(Path(file_path))
            file_name = Path(normalized_path).name
            sql += (
                " AND (path = ? OR original_path = ? OR path LIKE ? OR original_path LIKE ?)"
            )
            params.extend([
                normalized_path,
                normalized_path,
                f"%{file_name}%",
                f"%{file_name}%",
            ])

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
                authors_json=row["authors_json"] or "[]",
                venue=row["venue"] or "",
                title=row["title"] or "",
                doi=row["doi"] or "",
                path=row["path"] or "",
                original_path=row["original_path"] or "",
                snippet=row["snippet"] or "",
                doc_type=row["doc_type"] or "unknown",
                doc_score=row["doc_score"] or "",
                doc_reasons_json=row["doc_reasons_json"] or "[]",
                scie=row["scie"] or "",
                impact_factor=row["impact_factor"] or "",
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


def classify_document_type(
    *,
    first_page_text: str,
    pdf_info: dict[str, str],
    title: str,
    venue: str,
    doi: str,
) -> tuple[str, float, list[str]]:
    text = " ".join(
        part for part in [
            first_page_text or "",
            pdf_info.get("title", ""),
            pdf_info.get("subject", ""),
            pdf_info.get("keywords", ""),
            title or "",
            venue or "",
            doi or "",
        ]
        if part
    )

    t = text.lower()
    score = 0.0
    reasons: list[str] = []

    positive_rules = [
        ("doi", 2.5),
        ("abstract", 1.5),
        ("introduction", 1.0),
        ("references", 1.5),
        ("keywords", 0.8),
        ("proceedings", 1.5),
        ("journal", 1.0),
        ("volume", 0.7),
        ("issue", 0.5),
        ("et al", 0.8),
        ("citation", 0.5),
    ]

    for token, weight in positive_rules:
        if token in t:
            score += weight
            reasons.append(f"+ {token}")

    if doi:
        score += 2.0
        reasons.append("+ doi field")

    if venue:
        score += 1.5
        reasons.append("+ venue detected")

    academic_venues = [
        "frontiers in",
        "ieee",
        "acm",
        "springer",
        "elsevier",
        "neurips",
        "icml",
        "iclr",
        "acl",
        "emnlp",
        "cvpr",
        "iccv",
        "eccv",
        "aaai",
        "ijcai",
        "kdd",
        "www",
        "nature",
        "science",
        "cell",
        "lancet",
    ]
    if any(v in t for v in academic_venues):
        score += 2.0
        reasons.append("+ academic venue")

    negative_rules = [
        ("invoice", -3.0),
        ("quotation", -2.5),
        ("resume", -2.5),
        ("curriculum vitae", -2.5),
        ("contract", -2.0),
        ("proposal", -1.5),
        ("brochure", -2.0),
        ("manual", -1.5),
        ("installation guide", -1.5),
        ("meeting notes", -2.0),
        ("minutes", -2.0),
        ("price", -1.0),
        ("policy", -1.0),
        ("press release", -2.0),
        ("statement", -1.0),
    ]
    for token, weight in negative_rules:
        if token in t:
            score += weight
            reasons.append(f"- {token}")

    if "references" not in t and "abstract" not in t and not doi and not venue:
        score -= 1.5
        reasons.append("- no scholarly markers")

    if score >= 3.0:
        return "academic", score, reasons
    if score <= 0.0:
        return "non_academic", score, reasons
    return "unknown", score, reasons




def _normalize_compare_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip().casefold()
    text = re.sub(r"[^\w가-힣 ]+", "", text)
    return text


def _is_similar_text(a: str, b: str) -> bool:
    na = _normalize_compare_text(a)
    nb = _normalize_compare_text(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def clean_front_matter(text: str) -> str:
    """
    첫 페이지 상단의 편집 정보 / citation / correspondence 같은 잡음을 제거한다.
    """
    if not text:
        return ""

    lines = [line.rstrip() for line in text.splitlines()]
    cleaned: list[str] = []

    skip_citation_block = False

    for line in lines:
        stripped = line.strip()
        lowered = stripped.lower()

        if not stripped:
            continue

        if lowered.startswith("citation:"):
            skip_citation_block = True
            continue

        if skip_citation_block:
            if (
                10 <= len(stripped) <= 200
                and not lowered.startswith("doi:")
                and not lowered.startswith("frontiers in ")
                and "doi:" not in lowered
                and not re.match(r"^[A-Z\s]+$", stripped)
            ):
                skip_citation_block = False
            else:
                continue

        if any(
            lowered.startswith(prefix)
            for prefix in [
                "review",
                "published:",
                "edited by:",
                "reviewed by:",
                "*correspondence:",
                "specialty section:",
                "received:",
                "accepted:",
                "published:",
            ]
        ):
            continue

        if lowered in {"reviewed by:", "edited by:", "specialty section:"}:
            continue

        if "@" in lowered and ("correspondence" in lowered or lowered.endswith('.edu') or lowered.endswith('.org') or lowered.endswith('.com')):
            continue

        cleaned.append(stripped)

    return "\n".join(cleaned)


def looks_like_author_line(line: str) -> bool:
    if not line:
        return False

    s = re.sub(r"\s+", " ", line).strip()
    lower = s.lower()

    if len(s) < 3 or len(s) > 160:
        return False

    if "@" in lower:
        return False

    if any(token in lower for token in [
        "abstract", "introduction", "keywords", "university", "department",
        "institute", "laboratory", "college", "school", "faculty", "proceedings",
        "conference", "journal", "doi.org", "arxiv", "submitted", "accepted"
    ]):
        return False

    if re.search(r"\b(and|&)\b", s, flags=re.IGNORECASE) or ";" in s:
        return True

    tokens = re.findall(r"[A-Za-z가-힣][A-Za-z가-힣.'-]*", s)
    if 2 <= len(tokens) <= 12:
        short_tokens = sum(1 for t in tokens if len(t) <= 2)
        long_tokens = sum(1 for t in tokens if len(t) >= 3)
        if long_tokens >= 2 and short_tokens <= len(tokens) // 2 + 1:
            return True

    return False


def infer_header_title_lines(first_page_text: str, pdf_info: dict[str, str]) -> list[str]:
    lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
    if not lines:
        return []

    meta_title = pdf_info.get("title", "").strip()
    collected: list[str] = []

    for line in lines[:8]:
        lower = line.lower()

        if any(token in lower for token in ["abstract", "introduction", "keywords"]):
            break

        if looks_like_author_line(line):
            break

        if "@" in lower:
            break

        if any(token in lower for token in ["university", "department", "institute", "college", "school", "faculty", "laboratory", "lab"]):
            break

        if 5 <= len(line) <= 220:
            collected.append(line)
            if meta_title and _is_similar_text(" ".join(collected), meta_title):
                break

    return collected
def infer_author_block(text: str, known_title: str = "") -> str:
    """
    첫 페이지 상단에서 저자 블록 후보를 추출한다.
    제목과 동일하거나 제목의 연장선처럼 보이는 줄은 제외한다.
    """
    if not text:
        return ""

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return ""

    block: list[str] = []

    for line in lines[1:10]:
        lowered = line.lower()

        if any(token in lowered for token in ["abstract", "introduction", "keywords"]):
            break

        if len(line) > 180:
            continue

        if "@" in lowered:
            continue

        if known_title and _is_similar_text(line, known_title):
            continue

        if any(
            token in lowered
            for token in [
                "university",
                "department",
                "school",
                "institute",
                "laboratory",
                "lab",
                "college",
                "faculty",
            ]
        ):
            continue

        if looks_like_author_line(line):
            block.append(line)

    return " ; ".join(block).strip()


def parse_authors(author_text: str) -> list[str]:
    """
    저자 문자열을 분리/정규화한다.
    """
    if not author_text:
        return []

    text = author_text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()

    text = re.sub(r"\s+(and|&)\s+", "; ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=\w)[0-9*†‡]+", "", text)

    text = re.sub(r"\([^)]*@[^)]*\)", "", text)
    text = re.sub(
        r"\([^)]*(University|Department|Institute|School|College|Laboratory|Lab|Faculty)[^)]*\)",
        "",
        text,
        flags=re.IGNORECASE,
    )

    parts: list[str]

    if ";" in text:
        parts = [p.strip() for p in text.split(";") if p.strip()]
    else:
        comma_parts = [p.strip() for p in text.split(",") if p.strip()]
        if len(comma_parts) >= 4 and len(comma_parts) % 2 == 0:
            parts = [
                f"{comma_parts[i]}, {comma_parts[i + 1]}"
                for i in range(0, len(comma_parts), 2)
            ]
        else:
            parts = comma_parts

    cleaned: list[str] = []

    for name in parts:
        name = re.sub(r"\s+", " ", name).strip()
        name = re.sub(r"(?<=\w)[0-9*†‡]+$", "", name).strip()

        if re.search(
            r"\b(University|Department|Institute|School|College|Faculty|Laboratory|Lab|@)\b",
            name,
            flags=re.IGNORECASE,
        ):
            continue

        if "," in name:
            sub = [x.strip() for x in name.split(",") if x.strip()]
            if len(sub) == 2:
                family, given = sub
                name = f"{given} {family}".strip()

        if len(name) < 2 or len(name) > 80:
            continue

        cleaned.append(name)

    deduped: list[str] = []
    seen: set[str] = set()
    for name in cleaned:
        key = name.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(name)

    return deduped


def infer_first_author(text: str) -> str:
    """
    호환용 함수.
    """
    author_block = infer_author_block(text)
    authors = parse_authors(author_block)
    return authors[0] if authors else ""


def infer_title(file_stem: str, first_page_text: str, pdf_info: dict[str, str]) -> str:
    meta_title = pdf_info.get("title", "").strip()
    meta_author = pdf_info.get("author", "").strip()

    if meta_title and not _is_similar_text(meta_title, meta_author):
        return meta_title

    title_lines = infer_header_title_lines(first_page_text, pdf_info)
    if title_lines:
        candidate = " ".join(title_lines).strip()
        if 5 <= len(candidate) <= 300:
            return candidate

    lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
    for line in lines[:5]:
        if looks_like_author_line(line):
            continue
        if 5 <= len(line) <= 300:
            return line

    return file_stem


def infer_venue(text: str) -> str:
    if not text:
        return ""

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    joined = "\n".join(lines[:40])

    patterns = [
        r"(Proceedings of [^\n]+)",
        r"(In Proceedings of [^\n]+)",
        r"(International Conference on [^\n]+)",
        r"(Conference on [^\n]+)",
        r"(Journal of [^\n]+)",
        r"(Transactions on [^\n]+)",
        r"(Frontiers in [A-Za-z ]+)",
        r"(Nature [A-Za-z ]+|Science|Cell|The Lancet [A-Za-z ]+|IEEE Transactions on [A-Za-z ,\-]+)",
        r"(Neural Information Processing Systems(?: \(NeurIPS\))?)",
        r"(International Conference on Machine Learning(?: \(ICML\))?)",
        r"(International Conference on Learning Representations(?: \(ICLR\))?)",
        r"(Annual Meeting of the Association for Computational Linguistics(?: \(ACL\))?)",
        r"(Conference on Empirical Methods in Natural Language Processing(?: \(EMNLP\))?)",
        r"(Conference on Computer Vision and Pattern Recognition(?: \(CVPR\))?)",
        r"(International Conference on Computer Vision(?: \(ICCV\))?)",
        r"(European Conference on Computer Vision(?: \(ECCV\))?)",
        r"(AAAI(?: Conference on Artificial Intelligence)?)",
        r"(International Joint Conference on Artificial Intelligence(?: \(IJCAI\))?)",
        r"(ACM SIGKDD[^\n]*)",
        r"(The Web Conference(?: \(WWW\))?)",
        r"(NeurIPS(?:\s+\d{4})?)",
        r"(ICML(?:\s+\d{4})?)",
        r"(ICLR(?:\s+\d{4})?)",
        r"(ACL(?:\s+\d{4})?)",
        r"(EMNLP(?:\s+\d{4})?)",
        r"(CVPR(?:\s+\d{4})?)",
        r"(ICCV(?:\s+\d{4})?)",
        r"(ECCV(?:\s+\d{4})?)",
        r"(AAAI(?:\s+\d{4})?)",
        r"(IJCAI(?:\s+\d{4})?)",
        r"(KDD(?:\s+\d{4})?)",
        r"(WWW(?:\s+\d{4})?)",
    ]

    for pattern in patterns:
        m = re.search(pattern, joined, flags=re.IGNORECASE)
        if m:
            venue = re.sub(r"\s+", " ", m.group(1)).strip(" .,;:")
            return venue

    return ""


def infer_doi(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\b(10\.\d{4,9}/[-._;()/:A-Z0-9]+)\b", text, flags=re.IGNORECASE)
    if not m:
        return ""
    return m.group(1).rstrip(".,);]")


def normalize_doi(doi: str) -> str:
    if not doi:
        return ""

    text = doi.strip()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    text = text.strip().rstrip(".,);]")
    return text.lower()


def build_snippet(text: str, max_length: int = 500) -> str:
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= max_length:
        return compact
    return compact[: max_length - 3].rstrip() + "..."

def sanitize_pubmed_title(title: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", title or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def build_pubmed_search_term(title: str) -> str:
    cleaned = sanitize_pubmed_title(title)
    if not cleaned:
        return ""
    return f'"{cleaned}"[Title]'


def build_pubmed_search_url(title: str) -> str:
    cleaned = sanitize_pubmed_title(title)
    return f"https://pubmed.ncbi.nlm.nih.gov/?term={quote_plus(cleaned)}&sort=jour&sort_order=asc"


def build_pubmed_article_url(pmid: str) -> str:
    return f"https://pubmed.ncbi.nlm.nih.gov/{str(pmid).strip()}/"

def sanitize_journal_title(title: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", title or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def build_yonsei_journal_service_urls() -> dict[str, str]:
    return {
        "landing": "https://ymlib.yonsei.ac.kr/research-support/research-achievement/journal-analysis-service/",
        "scie": "https://openlink.ymlproxy.yonsei.ac.kr/link.n2s?url=https%3A%2F%2Fs2journal.bwise.kr%2Fjrnl%2FjrnlList.do%3Fmenu%3D1",
        "impact_factor": "https://openlink.ymlproxy.yonsei.ac.kr/link.n2s?url=https%3A%2F%2Fs2journal.bwise.kr%2Fjcr%2FjcrCategoryRankingPage.do",
    }


def search_pubmed_by_title(
    title: str,
    *,
    email: str = "",
    tool: str = "academic_paper_organizer",
    timeout: int = 15,
) -> dict[str, str]:
    cleaned = sanitize_pubmed_title(title)
    if not cleaned:
        return {
            "title": "",
            "matched": "false",
            "pmid": "",
            "url": "",
            "search_url": "",
            "error": "empty_title",
        }

    search_url = build_pubmed_search_url(cleaned)
    params = {
        "db": "pubmed",
        "term": build_pubmed_search_term(cleaned),
        "retmode": "json",
        "retmax": 1,
        "sort": "relevance",
        "tool": tool,
    }
    if email:
        params["email"] = email

    try:
        response = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        idlist = (((payload or {}).get("esearchresult") or {}).get("idlist") or [])
        if idlist:
            pmid = str(idlist[0]).strip()
            return {
                "title": cleaned,
                "matched": "true",
                "pmid": pmid,
                "url": build_pubmed_article_url(pmid),
                "search_url": search_url,
                "error": "",
            }
        return {
            "title": cleaned,
            "matched": "false",
            "pmid": "",
            "url": search_url,
            "search_url": search_url,
            "error": "",
        }
    except Exception as exc:
        return {
            "title": cleaned,
            "matched": "false",
            "pmid": "",
            "url": search_url,
            "search_url": search_url,
            "error": str(exc),
        }


def extract_paper_metadata(pdf_path: Path) -> dict[str, str]:
    """
    파일명 + PDF 첫 페이지 텍스트 + PDF 메타데이터를 조합해 메타를 추출한다.
    Crossref 비의존 fallback용 전역 함수.
    """
    name = pdf_path.stem
    raw_first_page_text = extract_pdf_first_page_text(pdf_path)
    first_page_text = clean_front_matter(raw_first_page_text)
    pdf_info = extract_pdf_document_info(pdf_path)

    filename_year_candidates = extract_year_candidates(name)
    first_page_year_candidates = extract_year_candidates(first_page_text)

    year = choose_best_year(
        *filename_year_candidates,
        *first_page_year_candidates,
    )

    title = infer_title(name, first_page_text, pdf_info)
    doi = normalize_doi(infer_doi(first_page_text))

    author_block = infer_author_block(first_page_text, known_title=title)
    authors = parse_authors(author_block)
    if not authors:
        authors = parse_authors(pdf_info.get("author", "").strip())

    if authors and title:
        t = title.casefold().strip()
        a0 = authors[0].casefold().strip()
        if t == a0 or t in a0 or a0 in t:
            authors = []

    if not authors:
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        if title in lines:
            idx = lines.index(title)
            retry_block = " ".join(lines[idx + 1: idx + 4])
            authors = parse_authors(retry_block)

    first_author = authors[0] if authors else ""
    venue = infer_venue(first_page_text)
    snippet = build_snippet(first_page_text)

    field_source = " ".join(
        part for part in [
            name,
            title,
            first_page_text[:3000],
            venue,
            pdf_info.get("subject", ""),
            pdf_info.get("keywords", ""),
        ]
        if part
    )
    field_code = infer_field_code_from_text(field_source)

    doc_type, doc_score, doc_reasons = classify_document_type(
        first_page_text=first_page_text,
        pdf_info=pdf_info,
        title=title,
        venue=venue,
        doi=doi,
    )

    if doc_type != "academic":
        field_code = "ETC"
        if not doi:
            venue = ""
        if not authors:
            first_author = ""
        if doc_type == "non_academic":
            year = "UnknownYear"

    return {
        "field_code": field_code,
        "year": year,
        "first_author": first_author,
        "authors_json": json.dumps(authors, ensure_ascii=False),
        "venue": venue,
        "title": title,
        "doi": doi,
        "snippet": snippet,
        "doc_type": doc_type,
        "doc_score": f"{doc_score:.2f}",
        "doc_reasons_json": json.dumps(doc_reasons, ensure_ascii=False),
    }


def build_output_pdf_path(
    output_dir: Path,
    doc_type: str,
    field_code: str,
    year: str,
    src_pdf: Path,
) -> Path:
    safe_type = doc_type if doc_type in {"academic", "non_academic", "unknown"} else "unknown"
    safe_field = field_code.strip() or "ETC"
    safe_year = normalize_year(year)
    return output_dir / safe_type / safe_field / safe_year / src_pdf.name


class PaperOrganizer:
    def __init__(
        self,
        watch_dir: Path,
        output_dir: Path,
        log_fn: Callable[[str], None] | None = None,
        cancel_event: threading.Event | None = None,
        pause_event: threading.Event | None = None,
        crossref_mailto: str = "your-email@example.com",
        crossref_cache_days: int = 180,
    ):
        self.watch_dir = Path(watch_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.log_fn = log_fn or (lambda msg: None)
        self.cancel_event = cancel_event or threading.Event()
        self.pause_event = pause_event or threading.Event()

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

    def wait_if_paused(self) -> bool:
        while self.pause_event.is_set():
            if self.cancel_event.is_set():
                return False
            time.sleep(0.2)
        return True


    def fetch_crossref_record(self, doi: str) -> dict[str, object]:
        norm_doi = normalize_doi(doi)
        if not norm_doi:
            return {}

        cache_key = f"crossref:work:{norm_doi}"

        cached = self.index.get_crossref_cache(
            cache_key,
            max_age_days=self.crossref_cache_days,
        )
        if cached:
            try:
                data = json.loads(cached)
                if isinstance(data, dict):
                    return data
            except Exception:
                pass

        url = f"https://api.crossref.org/works/{norm_doi}"
        headers = {
            "User-Agent": f"AcademicPaperOrganizer/1.0 (mailto:{self.crossref_mailto})"
        }

        try:
            resp = requests.get(url, headers=headers, timeout=8)
            if resp.status_code != 200:
                return {}

            payload = resp.json()
            message = payload.get("message", {})

            authors_raw = message.get("author", [])
            authors: list[str] = []
            for author in authors_raw:
                if not isinstance(author, dict):
                    continue

                given = str(author.get("given", "")).strip()
                family = str(author.get("family", "")).strip()
                literal = str(author.get("literal", "")).strip()

                name = f"{given} {family}".strip() or literal
                if name:
                    authors.append(name)

            deduped_authors: list[str] = []
            seen: set[str] = set()
            for name in authors:
                key = name.casefold()
                if key not in seen:
                    seen.add(key)
                    deduped_authors.append(name)

            title_list = message.get("title", [])
            container_list = message.get("container-title", [])
            published = message.get("published-print") or message.get("published-online") or message.get("issued") or {}
            date_parts = published.get("date-parts", [])
            crossref_year = ""
            if date_parts and isinstance(date_parts, list) and date_parts[0]:
                try:
                    crossref_year = normalize_year(date_parts[0][0])
                except Exception:
                    crossref_year = ""

            record = {
                "authors": deduped_authors,
                "title": str(title_list[0]).strip() if isinstance(title_list, list) and title_list else "",
                "venue": str(container_list[0]).strip() if isinstance(container_list, list) and container_list else "",
                "year": crossref_year,
            }

            self.index.set_crossref_cache(
                cache_key,
                json.dumps(record, ensure_ascii=False),
            )

            time.sleep(0.2)
            return record

        except Exception:
            return {}

    def fetch_authors_from_crossref(self, doi: str) -> list[str]:
        record = self.fetch_crossref_record(doi)
        authors = record.get("authors", [])
        if isinstance(authors, list):
            return [str(x).strip() for x in authors if str(x).strip()]
        return []

    def extract_paper_metadata(self, pdf_path: Path) -> dict[str, str]:
        """
        파일명 + PDF 첫 페이지 텍스트 + PDF 메타데이터 + Crossref를 조합해 메타를 추출한다.
        저자/제목/저널 정보 우선순위:
        1) Crossref (DOI)
        2) PDF metadata
        3) 첫 페이지 텍스트 파싱
        """
        name = pdf_path.stem
        raw_first_page_text = extract_pdf_first_page_text(pdf_path)
        first_page_text = clean_front_matter(raw_first_page_text)
        pdf_info = extract_pdf_document_info(pdf_path)

        filename_year_candidates = extract_year_candidates(name)
        first_page_year_candidates = extract_year_candidates(first_page_text)

        doi = normalize_doi(infer_doi(first_page_text))
        crossref_record = self.fetch_crossref_record(doi)

        title = str(crossref_record.get("title", "")).strip() or infer_title(name, first_page_text, pdf_info)
        venue = str(crossref_record.get("venue", "")).strip() or infer_venue(first_page_text)

        year = choose_best_year(
            crossref_record.get("year", ""),
            *filename_year_candidates,
            *first_page_year_candidates,
        )

        authors = crossref_record.get("authors", [])
        if not isinstance(authors, list):
            authors = []

        if not authors:
            author_block = infer_author_block(first_page_text, known_title=title)
            authors = parse_authors(author_block)

        if not authors:
            authors = parse_authors(pdf_info.get("author", "").strip())

        if authors and title:
            t = title.casefold().strip()
            a0 = authors[0].casefold().strip()
            if t == a0 or t in a0 or a0 in t:
                authors = []

        if not authors:
            lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
            if title in lines:
                idx = lines.index(title)
                retry_block = " ".join(lines[idx + 1: idx + 4])
                authors = parse_authors(retry_block)

        first_author = authors[0] if authors else ""

        snippet = build_snippet(first_page_text)

        field_source = " ".join(
            part for part in [
                name,
                title,
                first_page_text[:3000],
                venue,
                pdf_info.get("subject", ""),
                pdf_info.get("keywords", ""),
            ]
            if part
        )
        field_code = infer_field_code_from_text(field_source)

        doc_type, doc_score, doc_reasons = classify_document_type(
            first_page_text=first_page_text,
            pdf_info=pdf_info,
            title=title,
            venue=venue,
            doi=doi,
        )

        if doc_type != "academic":
            field_code = "ETC"
            if not doi:
                venue = ""
            if not authors:
                first_author = ""
            if doc_type == "non_academic":
                year = "UnknownYear"

        return {
            "field_code": field_code,
            "year": year,
            "first_author": first_author,
            "authors_json": json.dumps(authors, ensure_ascii=False),
            "venue": venue,
            "title": title,
            "doi": doi,
            "snippet": snippet,
            "doc_type": doc_type,
            "doc_score": f"{doc_score:.2f}",
            "doc_reasons_json": json.dumps(doc_reasons, ensure_ascii=False),
        }

    def process_pdf(self, pdf_path: Path) -> None:
        if self.is_cancelled():
            self.log("[CANCEL] PDF 처리 취소됨")
            return

        if not self.wait_if_paused():
            self.log("[CANCEL] 일시정지 중 취소됨")
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
            if not self.wait_if_paused():
                self.log(f"[CANCEL] 일시정지 중 취소됨: {pdf_path}")
                return

            meta = self.extract_paper_metadata(pdf_path)

            if self.is_cancelled():
                self.log(f"[CANCEL] 메타 추출 후 취소: {pdf_path}")
                return

            field_code = meta.get("field_code", "").strip() or "ETC"
            year = normalize_year(meta.get("year", "").strip() or "UnknownYear")
            first_author = meta.get("first_author", "").strip()
            authors_json = meta.get("authors_json", "[]").strip() or "[]"
            venue = meta.get("venue", "").strip()
            title = meta.get("title", "").strip() or pdf_path.stem
            doi = meta.get("doi", "").strip()
            snippet = meta.get("snippet", "").strip()
            doc_type = meta.get("doc_type", "unknown").strip() or "unknown"
            doc_score = meta.get("doc_score", "").strip()
            doc_reasons_json = meta.get("doc_reasons_json", "[]").strip() or "[]"

            if not self.wait_if_paused():
                self.log(f"[CANCEL] 일시정지 중 취소됨: {pdf_path}")
                return

            dst_pdf = build_output_pdf_path(
                self.output_dir,
                doc_type,
                field_code,
                year,
                pdf_path,
            )

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
                authors_json=authors_json,
                venue=venue,
                title=title,
                doi=doi,
                path=str(stored_pdf),
                original_path=str(pdf_path),
                snippet=snippet,
                doc_type=doc_type,
                doc_score=doc_score,
                doc_reasons_json=doc_reasons_json,
            )

            if self.is_cancelled():
                self.log(f"[CANCEL] 인덱싱 후 취소: {pdf_path}")
                return

            try:
                authors = json.loads(authors_json)
                if not isinstance(authors, list):
                    authors = []
            except Exception:
                authors = []

            write_pdf_sidecar_metadata(
                stored_pdf.with_suffix(".json"),
                title=title,
                doi=doi,
                original_path=pdf_path,
                stored_path=stored_pdf,
                field_code=field_code,
                year=year,
                author=first_author,
                authors=authors,
                venue=venue,
                doc_type=doc_type,
                doc_score=doc_score,
                doc_reasons_json=doc_reasons_json,
            )

            self.log(f"[CLASSIFY] {pdf_path.name} -> {doc_type} (score={doc_score})")
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
    progress = progress_fn or (lambda msg: None)

    count = 0
    for pdf_path in root_dir.rglob("*.pdf"):
        if organizer.is_cancelled():
            logger("[CANCEL] 기존 PDF 스캔 취소됨")
            break
        if not organizer.wait_if_paused():
            logger("[CANCEL] 일시정지 중 스캔 취소됨")
            break

        if not pdf_path.is_file():
            continue

        try:
            organizer.process_pdf(pdf_path)
            count += 1
        except Exception as exc:
            logger(f"[ERROR] 기존 PDF 처리 실패: {pdf_path} | {exc}")

    logger(f"[SCAN] 기존 PDF 처리 완료: {count}건")


def repair_misplaced_year_folders(
    output_dir: Path,
    log_fn: Callable[[str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> int:
    logger = log_fn or (lambda msg: None)
    output_dir = Path(output_dir).resolve()

    moved_count = 0

    for pdf_path in output_dir.rglob("*.pdf"):
        if cancel_event is not None and cancel_event.is_set():
            logger("[CANCEL] 연도 폴더 보정 취소됨")
            break

        if not pdf_path.is_file():
            continue

        if "LOG" in pdf_path.parts:
            continue

        try:
            rel = pdf_path.relative_to(output_dir)
        except ValueError:
            continue

        parts = rel.parts
        if len(parts) < 3:
            continue

        current_field = parts[0]
        current_year = parts[1]

        meta = extract_paper_metadata(pdf_path)
        repaired_year = normalize_year(meta.get("year"))

        if repaired_year == "UnknownYear":
            logger(f"[REPAIR-SKIP] 연도 판별 불가: {pdf_path}")
            continue

        if repaired_year == current_year:
            continue

        current_doc_type = str(meta.get("doc_type", "unknown")).strip() or "unknown"
        target_pdf = build_output_pdf_path(
            output_dir,
            current_doc_type,
            current_field,
            repaired_year,
            pdf_path,
        )
        target_pdf.parent.mkdir(parents=True, exist_ok=True)
        target_pdf = unique_path(target_pdf)

        try:
            shutil.move(str(pdf_path), str(target_pdf))
            moved_count += 1
            logger(f"[REPAIR] PDF 이동: {pdf_path} -> {target_pdf}")
        except Exception as exc:
            logger(f"[ERROR] PDF 이동 실패: {pdf_path} | {exc}")
            continue

        old_meta = pdf_path.with_suffix(".json")
        new_meta = target_pdf.with_suffix(".json")

        if old_meta.exists():
            try:
                data = json.loads(old_meta.read_text(encoding="utf-8"))
            except Exception:
                data = {}

            data["stored_path"] = str(target_pdf)
            data["field_code"] = current_field
            data["year"] = repaired_year

            try:
                new_meta.write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                old_meta.unlink(missing_ok=True)
            except Exception as exc:
                logger(f"[WARN] 메타 갱신 실패: {old_meta} | {exc}")

    logger(f"[REPAIR] 연도 폴더 보정 완료: {moved_count}건")
    return moved_count


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

    _ = crossref_cache_days

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

                authors = data.get("authors", [])
                if not isinstance(authors, list):
                    authors = []
                authors_json = json.dumps(authors, ensure_ascii=False)

                venue = str(data.get("venue", ""))
                title = str(data.get("title", stored_pdf.stem))
                doi = str(data.get("doi", ""))
                doc_type = str(data.get("doc_type", "unknown")).strip() or "unknown"
                doc_score = str(data.get("doc_score", "")).strip()
                doc_reasons_json = str(data.get("doc_reasons_json", "[]")).strip() or "[]"

                index.add_paper(
                    field_code=field_code,
                    year=year,
                    first_author=first_author,
                    authors_json=authors_json,
                    venue=venue,
                    title=title,
                    doi=doi,
                    path=str(stored_pdf),
                    original_path=str(original_path),
                    snippet="",
                    doc_type=doc_type,
                    doc_score=doc_score,
                    doc_reasons_json=doc_reasons_json,
                )
                restored += 1
            except Exception as exc:
                logger(f"[WARN] 재인덱싱 메타 복원 실패: {meta_file} | {exc}")

        logger(f"[REINDEX] 완료: {restored}건")
    finally:
        index.close()


def repair_and_reindex(
    args,
    log_fn: Callable[[str], None] | None = None,
    cancel_event: threading.Event | None = None,
    crossref_cache_days: int = 180,
) -> None:
    logger = log_fn or (lambda msg: None)
    output_dir = Path(args.output).resolve()

    logger("[REPAIR] 연도 폴더 보정 시작")
    repair_misplaced_year_folders(
        output_dir=output_dir,
        log_fn=logger,
        cancel_event=cancel_event,
    )

    if cancel_event is not None and cancel_event.is_set():
        logger("[CANCEL] 보정 후 재인덱싱 취소됨")
        return

    logger("[REPAIR] 재인덱싱 시작")
    run_reindex(
        args,
        log_fn=logger,
        cancel_event=cancel_event,
        crossref_cache_days=crossref_cache_days,
    )

# ===== Fast reparse / incremental skip patch =====
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def _file_signature(path: Path) -> dict[str, int]:
    try:
        st = path.stat()
        return {
            "source_size": int(st.st_size),
            "source_mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
        }
    except Exception:
        return {"source_size": -1, "source_mtime_ns": -1}


def _safe_load_json(path: Path) -> dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _paperindex_init_fast(self, db_path: Path):
    self.db_path = Path(db_path)
    self.db_path.parent.mkdir(parents=True, exist_ok=True)
    self.conn = sqlite3.connect(str(self.db_path), timeout=30, check_same_thread=False)
    self.conn.row_factory = sqlite3.Row
    try:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=30000")
        self.conn.commit()
    except Exception:
        pass
    self._init_db()


def _paperindex_find_by_original_path(self, original_path: str) -> dict[str, str] | None:
    cur = self.conn.cursor()
    cur.execute(
        """
        SELECT path, original_path, title, doi, venue, year, field_code, doc_type
        FROM papers
        WHERE original_path = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (original_path,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "path": row["path"] or "",
        "original_path": row["original_path"] or "",
        "title": row["title"] or "",
        "doi": row["doi"] or "",
        "venue": row["venue"] or "",
        "year": row["year"] or "",
        "field_code": row["field_code"] or "",
        "doc_type": row["doc_type"] or "unknown",
    }


PaperIndex.__init__ = _paperindex_init_fast
PaperIndex.find_by_original_path = _paperindex_find_by_original_path

_old_process_pdf = PaperOrganizer.process_pdf


def _organizer_clone(self) -> "PaperOrganizer":
    return PaperOrganizer(
        watch_dir=self.watch_dir,
        output_dir=self.output_dir,
        log_fn=self.log_fn,
        cancel_event=self.cancel_event,
        crossref_mailto=self.crossref_mailto,
        crossref_cache_days=self.crossref_cache_days,
    )


def _organizer_should_skip_pdf(self, pdf_path: Path) -> bool:
    pdf_path = Path(pdf_path).resolve()
    row = self.index.find_by_original_path(str(pdf_path))
    if not row:
        return False

    stored_pdf = Path(row.get("path", ""))
    if not stored_pdf.exists():
        return False

    meta_path = stored_pdf.with_suffix(".json")
    if not meta_path.exists():
        return False

    payload = _safe_load_json(meta_path)
    current_sig = _file_signature(pdf_path)
    old_size = int(payload.get("source_size", -2))
    old_mtime = int(payload.get("source_mtime_ns", -2))

    if old_size == current_sig["source_size"] and old_mtime == current_sig["source_mtime_ns"]:
        return True
    return False


def _organizer_process_pdf_fast(self, pdf_path: Path, *, force_reparse: bool = False) -> None:
    pdf_path = Path(pdf_path).resolve()

    if not force_reparse and self.should_skip_pdf(pdf_path):
        self.log(f"[SKIP] 변경 없음: {pdf_path}")
        return

    _old_process_pdf(self, pdf_path)

    try:
        row = self.index.find_by_original_path(str(pdf_path))
        if not row:
            return
        stored_pdf = Path(row.get("path", ""))
        if not stored_pdf.exists():
            return
        meta_path = stored_pdf.with_suffix(".json")
        payload = _safe_load_json(meta_path)
        payload.update(_file_signature(pdf_path))
        payload["processed_at"] = datetime.now().isoformat(timespec="seconds")
        meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        self.log(f"[WARN] 시그니처 저장 실패: {pdf_path} | {exc}")


PaperOrganizer.clone = _organizer_clone
PaperOrganizer.should_skip_pdf = _organizer_should_skip_pdf
PaperOrganizer.process_pdf = _organizer_process_pdf_fast


def _iter_pdf_paths(root_dir: Path) -> list[Path]:
    return [p for p in Path(root_dir).resolve().rglob("*.pdf") if p.is_file()]


def scan_existing_pdfs(
    organizer: PaperOrganizer,
    root_dir: Path,
    log_fn: Callable[[str], None] | None = None,
    *,
    workers: int = 1,
    skip_unchanged: bool = True,
    force_reparse: bool = False,
) -> None:
    root_dir = Path(root_dir).resolve()
    logger = log_fn or (lambda msg: None)

    pdf_paths = _iter_pdf_paths(root_dir)
    total = len(pdf_paths)
    if total == 0:
        logger("[SCAN] 처리할 PDF가 없습니다")
        return

    workers = max(1, int(workers or 1))
    if workers == 1:
        processed = 0
        for idx, pdf_path in enumerate(pdf_paths, start=1):
            if organizer.is_cancelled():
                logger("[CANCEL] 기존 PDF 스캔 취소됨")
                break
            try:
                organizer.process_pdf(pdf_path, force_reparse=force_reparse)
                processed += 1
                if idx % 25 == 0 or idx == total:
                    logger(f"[SCAN] 진행: {idx}/{total}")
            except Exception as exc:
                logger(f"[ERROR] 기존 PDF 처리 실패: {pdf_path} | {exc}")
        logger(f"[SCAN] 기존 PDF 처리 완료: {processed}건")
        return

    logger(f"[SCAN] 병렬 처리 시작: 총 {total}건 | workers={workers}")

    def _task(pdf_path: Path) -> tuple[str, str]:
        if organizer.is_cancelled():
            return ("cancel", str(pdf_path))
        local_org = organizer.clone()
        try:
            if skip_unchanged and not force_reparse and local_org.should_skip_pdf(pdf_path):
                return ("skip", str(pdf_path))
            local_org.process_pdf(pdf_path, force_reparse=force_reparse)
            return ("ok", str(pdf_path))
        finally:
            try:
                local_org.close()
            except Exception:
                pass

    done = 0
    ok_count = 0
    skip_count = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_task, pdf): pdf for pdf in pdf_paths}
        for fut in as_completed(futures):
            done += 1
            pdf = futures[fut]
            try:
                status, _ = fut.result()
                if status == "ok":
                    ok_count += 1
                elif status == "skip":
                    skip_count += 1
                elif status == "cancel":
                    pass
            except Exception as exc:
                err_count += 1
                logger(f"[ERROR] 기존 PDF 처리 실패: {pdf} | {exc}")

            if done % 25 == 0 or done == total:
                logger(
                    f"[SCAN] 진행: {done}/{total} | 처리={ok_count} | 건너뜀={skip_count} | 오류={err_count}"
                )

            if organizer.is_cancelled():
                logger("[CANCEL] 기존 PDF 스캔 취소 요청 감지")
                break

    logger(
        f"[SCAN] 기존 PDF 처리 완료: 총 {total}건 | 처리={ok_count} | 건너뜀={skip_count} | 오류={err_count}"
    )
