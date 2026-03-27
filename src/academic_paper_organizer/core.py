from __future__ import annotations

import csv
import json
import re
import shutil
import sqlite3
import threading
import time
from difflib import SequenceMatcher
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
from urllib.parse import quote, quote_plus, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
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
    quartile: str = ""
    openalex_score: str = ""


def normalize_journal_name(name: str) -> str:
    text = str(name or "").strip().casefold()
    if not text:
        return ""

    text = text.replace("&", " and ")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\bthe\s+", "", text)
    text = re.sub(r"\bjournal of the\b", "journal of", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_journal_metrics(csv_path: str | Path = "journal_metrics.csv") -> dict[str, dict[str, str]]:
    data: dict[str, dict[str, str]] = {}
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

                payload = {
                    "scie": str(row.get("SCIE", "")).strip(),
                    "impact_factor": str(row.get("Impact Factor", "")).strip(),
                    "quartile": str(row.get("Quartile", "")).strip(),
                }

                aliases = [name]
                alias_text = str(row.get("Alias", "")).strip()
                if alias_text:
                    aliases.extend([part.strip() for part in alias_text.split("|") if part.strip()])

                for candidate in aliases:
                    norm = normalize_journal_name(candidate)
                    if norm:
                        data[norm] = payload.copy()
    except Exception:
        return {}
    return data


JOURNAL_DB = load_journal_metrics()


def classify_quartile(impact_factor: str | float | int | None) -> str:
    """
    Impact Factor 값을 기준으로 보수적으로 사분위 라벨을 추정한다.
    실제 JCR quartile 값이 있으면 그 값을 우선 사용해야 하며,
    이 함수는 fallback 용도다.
    """
    if impact_factor is None:
        return ""

    text = str(impact_factor).strip()
    if not text:
        return ""

    try:
        value = float(text)
    except Exception:
        return ""

    if value >= 10:
        return "Q1"
    if value >= 5:
        return "Q2"
    if value >= 2:
        return "Q3"
    if value > 0:
        return "Q4"
    return ""


def fetch_openalex_journal_metrics(venue: str) -> tuple[str, str, str, str]:
    venue = str(venue or "").strip()
    if not venue:
        return "", "", "", ""

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
            return "", "", "", ""

        item = results[0] or {}
        is_in_doaj = bool(item.get("is_in_doaj"))
        is_oa = bool(item.get("is_oa"))
        summary_stats = item.get("summary_stats") or {}
        cited_half_life = summary_stats.get("2yr_mean_citedness")
        works_count = item.get("works_count") or 0
        cited_by_count = item.get("cited_by_count") or 0

        scie = "SCIE" if (works_count or cited_by_count or is_in_doaj or is_oa) else "Unknown"
        impact_factor = ""
        quartile = ""
        if cited_half_life not in (None, ""):
            openalex_score = f"{float(cited_half_life):.2f}"
        elif cited_by_count:
            openalex_score = f"{round(float(cited_by_count) / 1000.0, 2):.2f}"
        else:
            openalex_score = ""

        return scie, impact_factor, quartile, openalex_score
    except Exception:
        return "", "", "", ""


def fetch_journal_metrics(venue: str) -> tuple[str, str, str, str]:
    venue = str(venue or "").strip()
    if not venue:
        return "", "", "", ""

    key = normalize_journal_name(venue)
    if key in JOURNAL_DB:
        payload = JOURNAL_DB[key]
        scie = payload.get("scie", "")
        impact_factor = payload.get("impact_factor", "")
        quartile = payload.get("quartile", "")
        return scie, impact_factor, quartile, ""

    scie, impact_factor, quartile, openalex_score = fetch_openalex_journal_metrics(venue)
    if not quartile and impact_factor:
        quartile = classify_quartile(impact_factor)
    return scie, impact_factor, quartile, openalex_score


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
                quartile TEXT,
                openalex_score TEXT,
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
        if "quartile" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN quartile TEXT")
        if "openalex_score" not in columns:
            cur.execute("ALTER TABLE papers ADD COLUMN openalex_score TEXT")

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
        quartile: str = "",
        openalex_score: str = "",
    ) -> None:
        safe_year = normalize_year(year)
        if not scie and not impact_factor and not openalex_score and not quartile:
            scie, impact_factor, quartile, openalex_score = fetch_journal_metrics(venue)
        if not quartile and impact_factor:
            quartile = classify_quartile(impact_factor)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO papers (
                field_code, year, first_author, authors_json, venue, title, doi,
                path, original_path, snippet, doc_type, doc_score, doc_reasons_json,
                scie, impact_factor, quartile, openalex_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                quartile,
                openalex_score,
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
        scie: str | None = None,
        min_impact_factor: str | None = None,
        quartile: str | None = None,
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
                COALESCE(impact_factor, '') AS impact_factor,
                COALESCE(quartile, '') AS quartile,
                COALESCE(openalex_score, '') AS openalex_score
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

        if scie:
            sql += " AND COALESCE(scie, '') = ?"
            params.append(scie)

        if quartile:
            sql += " AND COALESCE(quartile, '') = ?"
            params.append(quartile)

        if min_impact_factor:
            try:
                min_if_value = float(str(min_impact_factor).strip())
                sql += " AND CAST(COALESCE(NULLIF(impact_factor, ''), '0') AS REAL) >= ?"
                params.append(min_if_value)
            except Exception:
                pass

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
                quartile=row["quartile"] or "",
                openalex_score=row["openalex_score"] or "",
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


def scan_existing_pdfs_legacy(
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
        if len(parts) < 4:
            continue

        current_doc_type = parts[0]
        current_field = parts[1]
        current_year = parts[2]

        meta = extract_paper_metadata(pdf_path)
        repaired_year = normalize_year(meta.get("year"))

        if repaired_year == "UnknownYear":
            logger(f"[REPAIR-SKIP] 연도 판별 불가: {pdf_path}")
            continue

        if repaired_year == current_year:
            continue

        inferred_doc_type = str(meta.get("doc_type", "")).strip()
        target_doc_type = inferred_doc_type if inferred_doc_type in {"academic", "non_academic", "unknown"} else current_doc_type
        target_pdf = build_output_pdf_path(
            output_dir,
            target_doc_type,
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



def _safe_filename(text: str) -> str:
    text = re.sub(r'[\/:*?"<>|]+', '_', str(text or '').strip())
    text = re.sub(r'\s+', '_', text)
    return text[:120] or 'professors'


import re
import json
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


YONSEI_MEDICINE_PROFESSOR_INDEX_URLS = [
    "https://medicine.yonsei.ac.kr/medicine/research/basic/microbiology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/pathology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/Physiology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/biochemistry-molecular-biology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/pharmacology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/tropical-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/preventive.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/biomedical-science.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/systems-informatics.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/euyong.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/anatomy.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/family.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/internal-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/anesthesiology-pain-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/radiation-oncology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/urology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/obstetrics-gynecology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/plastic-reconstructive-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/juveniles.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/neurology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/neurosurgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/ophthalmology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/radiology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/emergency.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/otorhinolaryngology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/hospital-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/rehabilitation.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/psychiatry.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/orthopedic-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/laboratory.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/dermatology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/nuclearMedicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/chest-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/infectious-disease.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/endocrinology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/geriatrics.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/rheumatology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/gastroenterology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/nephrology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/cardiology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/allergy.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/oncology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/integrated-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/hematology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/pumonology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/hepatobiliary-pancreatic-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/thyroid-endocrine-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/colorectal-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/pediatric-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/gastrointestinal-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/breast-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/transplantation-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/trauma-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/forensic-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/medical-education.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/humanities-social.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/law-moral.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/medical-history.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/korea-unification-global-health.do"
]

YONSEI_MEDICINE_DEPARTMENT_URLS = [
    "https://medicine.yonsei.ac.kr/medicine/research/basic/microbiology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/pathology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/Physiology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/biochemistry-molecular-biology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/pharmacology.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/tropical-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/preventive.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/biomedical-science.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/systems-informatics.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/euyong.do",
    "https://medicine.yonsei.ac.kr/medicine/research/basic/anatomy.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/family.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/internal-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/anesthesiology-pain-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/radiation-oncology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/urology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/obstetrics-gynecology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/plastic-reconstructive-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/juveniles.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/neurology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/neurosurgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/ophthalmology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/radiology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/emergency.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/otorhinolaryngology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/hospital-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/rehabilitation.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/psychiatry.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/orthopedic-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/laboratory.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/dermatology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/nuclearMedicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/chest-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/infectious-disease.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/endocrinology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/geriatrics.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/rheumatology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/gastroenterology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/nephrology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/cardiology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/allergy.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/oncology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/integrated-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/hematology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/pumonology.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/hepatobiliary-pancreatic-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/thyroid-endocrine-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/colorectal-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/pediatric-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/gastrointestinal-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/breast-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/transplantation-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/trauma-surgery.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/forensic-medicine.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/medical-education.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/humanities-social.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/law-moral.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/medical-history.do",
    "https://medicine.yonsei.ac.kr/medicine/about/professor/humanities/korea-unification-global-health.do"
]

BAD_NAME_WORDS = {
    "검색", "닫기", "뉴스", "채용", "입찰", "소개", "학장단", "역대학장", "동아리",
    "대학", "대학생활", "의과대생활", "발달시험", "세미나", "강연", "공지", "공지사항",
    "학교소개", "학교현황", "오시는 길", "연구", "연구기관", "부설연구소", "센터",
    "클리닉", "교실", "교수소개", "전체교실", "입학", "입학전형", "입학공지",
    "후원", "언론보도", "장례식장", "암병원", "안과병원", "재활병원", "척추병원",
    "약학대학", "치과대학", "간호대학", "보건대학원", "주메뉴", "전체메뉴", "Home",
    "TOP", "KO", "EN", "YONSEI", "Image", "이미지",
}

BAD_NAME_SUBSTRINGS = [
    "yonsei", "severance", "medicine", "medical", "college", "school",
    "department", "faculty", "clinic", "center", "research", "download",
    "로그인", "회원가입", "교수의 책무", "학교안내", "학장소개",
]


EN_BAD_WORDS = {
    "professor", "staff", "department", "college", "medicine", "medical",
    "research", "education", "biochemistry", "molecular", "biology",
    "introduction", "contact", "laboratory", "lab", "home", "faculty",
}

KOREAN_SURNAME_TO_EN = {
    "김": {"kim", "gim"},
    "이": {"lee", "yi", "rhee"},
    "박": {"park", "bak"},
    "최": {"choi"},
    "정": {"jung", "jeong", "chung"},
    "조": {"cho", "jo"},
    "강": {"kang", "gang"},
    "윤": {"yoon", "yun"},
    "장": {"jang"},
    "임": {"lim", "im"},
    "한": {"han"},
    "오": {"oh", "o"},
    "서": {"seo", "suh"},
    "신": {"shin"},
    "유": {"yu", "yoo"},
    "송": {"song"},
    "홍": {"hong"},
    "문": {"moon", "mun"},
    "고": {"ko", "go"},
    "양": {"yang"},
    "손": {"son"},
    "배": {"bae"},
    "백": {"baek", "paik"},
    "허": {"heo", "huh"},
    "남": {"nam"},
    "노": {"noh", "roh"},
}

KOR_ROMA = {
    "가": ["ga", "ka"], "강": ["kang"], "건": ["geon", "gun"], "경": ["gyeong", "kyung"],
    "고": ["go", "ko"], "관": ["gwan"], "광": ["gwang"], "구": ["gu", "koo"],
    "규": ["gyu", "kyu"], "근": ["geun", "keun"], "기": ["gi", "ki"],
    "김": ["kim", "gim"], "나": ["na"], "남": ["nam"], "노": ["no", "roh", "noh"],
    "다": ["da"], "도": ["do"], "동": ["dong"], "두": ["doo", "du"],
    "라": ["ra", "la"], "류": ["ryu", "yu"], "리": ["lee", "ri", "yi"], "민": ["min"],
    "박": ["park", "bak"], "배": ["bae"], "백": ["baek", "paik"], "범": ["beom", "bum"],
    "병": ["byeong", "byung"], "상": ["sang"], "석": ["seok", "suk"], "선": ["sun", "seon"],
    "성": ["seong", "sung"], "소": ["so"], "수": ["su", "soo"], "순": ["sun", "soon"],
    "승": ["seung", "sung"], "시": ["si", "shi"], "신": ["shin"], "아": ["a"],
    "안": ["an"], "양": ["yang"], "엄": ["eom", "um"], "여": ["yeo"], "연": ["yeon"],
    "영": ["yeong", "young"], "예": ["ye"], "오": ["oh", "o"], "옥": ["ok"],
    "완": ["wan"], "용": ["yong"], "우": ["u", "woo"], "원": ["won"],
    "유": ["yu", "yoo"], "윤": ["yoon", "yun"], "은": ["eun"], "이": ["i", "lee", "yi"],
    "인": ["in"], "임": ["lim", "im"], "장": ["jang"], "재": ["jae"], "전": ["jeon", "jun"],
    "정": ["jeong", "jung", "chung"], "제": ["je"], "조": ["jo", "cho"], "종": ["jong"],
    "주": ["ju", "joo"], "준": ["jun"], "지": ["ji"], "진": ["jin"], "차": ["cha"],
    "창": ["chang"], "채": ["chae"], "철": ["cheol", "chul"], "최": ["choi"],
    "태": ["tae"], "하": ["ha"], "한": ["han"], "현": ["hyeon", "hyun"], "형": ["hyeong", "hyung"],
    "혜": ["hye"], "호": ["ho"], "홍": ["hong"], "화": ["hwa"], "환": ["hwan"], "훈": ["hoon", "hun"], "희": ["hee", "hui"],
}
TITLE_WORDS = [
    "임상조교수", "임상부교수", "임상교수", "중계교수", "중견교수",
    "조교수", "부교수", "명예교수", "교수",
    "Professor", "professor", "M.D.", "Ph.D.", "MD", "PhD",
]

BAD_LINK_TEXTS = {
    "콘텐츠 바로가기", "주메뉴 바로가기", "푸터 바로가기", "로그인", "회원가입",
    "전체메뉴", "전체메뉴 열기", "전체메뉴 닫기", "교육", "연세의료원 네트워크",
    "home", "top",
}

BAD_LINK_SUBSTRINGS = [
    "바로가기", "로그인", "회원가입", "전체메뉴", "사이트맵", "sitemap",
    "copyright", "개인정보처리방침", "이용약관",
]

BAD_LINK_HREF_SUBSTRINGS = [
    "#", "login", "join", "member", "logout", "sitemap", "javascript:", "mailto:",
]


def _strip_professor_role_words(text: str) -> str:
    value = str(text or "")
    for token in sorted(TITLE_WORDS, key=len, reverse=True):
        value = value.replace(token, " ")
    value = re.sub(r"\b(Professor|professor|M\.D\.|Ph\.D\.|MD|PhD)\b", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _normalize_professor_name(name: str) -> str:
    text = str(name or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = _strip_professor_role_words(text)
    text = re.sub(r"[|/·•,()\[\]<>]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_korean_name_candidates(text: str) -> list[str]:
    value = _normalize_professor_name(text)
    if not value:
        return []

    candidates: list[str] = []
    for match in re.finditer(r"(?<![가-힣])[가-힣]{2,4}(?![가-힣])", value):
        token = match.group(0).strip()
        if _looks_like_professor_name(token):
            candidates.append(token)

    deduped: list[str] = []
    seen: set[str] = set()
    for token in candidates:
        if token not in seen:
            seen.add(token)
            deduped.append(token)
    return deduped
def _normalize_english_professor_name(name: str) -> str:
    value = _strip_professor_role_words(str(name or ""))
    value = re.sub(r"[^A-Za-z\s.'\-]", " ", value)
    value = re.sub(r"\s+", " ", value).strip(" .,-")
    if not value:
        return ""

    tokens = []
    for token in value.split():
        if len(token) == 1:
            tokens.append(token.upper())
        else:
            tokens.append(token[0].upper() + token[1:])
    value = " ".join(tokens).strip()

    if re.fullmatch(r"[A-Z][A-Za-z'\-]+(?: [A-Z][A-Za-z'\-]+){1,3}", value):
        return value
    if re.fullmatch(r"[A-Z]\.\s?[A-Z][A-Za-z'\-]+", value):
        return value
    return ""


def _normalize_professor_name_for_query(name: str) -> str:
    name = _normalize_professor_name(name)
    if not name:
        return ""
    return f'"{name}" 연세대학교 의과대학'


def _extract_english_name_candidates(text: str) -> list[str]:
    value = _strip_professor_role_words(str(text or ""))
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return []

    patterns = [
        r"[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,3}",
        r"[A-Z]\.\s?[A-Z][A-Za-z'\-]+",
    ]
    candidates: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        for match in re.finditer(pattern, value):
            token = _normalize_english_professor_name(match.group(0))
            if token and token.casefold() not in seen:
                seen.add(token.casefold())
                candidates.append(token)
    return candidates


def _extract_korean_english_name_pairs(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    soup = _remove_layout_noise(soup)

    pairs: dict[str, str] = {}
    nodes = soup.select("strong, b, dt, dd, li, td, p, h3, h4, h5, span, a")
    for node in nodes:
        text = node.get_text(" ", strip=True)
        if not text or len(text) > 120:
            continue

        korean_names: list[str] = []
        english_names: list[str] = []
        parts = [part.strip() for part in re.split(r"[|/,\n]|\s{2,}", text) if part.strip()]
        for part in parts:
            korean_names.extend(_extract_korean_name_candidates(part))
            english_names.extend(_extract_english_name_candidates(part))

        if len(korean_names) == 1 and len(english_names) == 1:
            pairs.setdefault(korean_names[0], english_names[0])
            continue

        whole_ko = _extract_korean_name_candidates(text)
        whole_en = _extract_english_name_candidates(text)
        if len(whole_ko) == 1 and len(whole_en) == 1:
            pairs.setdefault(whole_ko[0], whole_en[0])

    return pairs
def _looks_like_professor_name(text: str) -> bool:
    value = _normalize_professor_name(text)
    if not value:
        return False

    if len(value) < 2 or len(value) > 20:
        return False

    if value in BAD_NAME_WORDS:
        return False

    lowered = value.casefold()
    if any(token.casefold() in lowered for token in BAD_NAME_SUBSTRINGS):
        return False

    if any(bad in value for bad in BAD_NAME_WORDS):
        return False

    if re.search(r"\d", value):
        return False

    if re.fullmatch(r"[가-힣]{2,4}", value):
        return True
    if re.fullmatch(r"[가-힣]{1,3}\s[가-힣]{1,3}", value):
        return True

    if re.fullmatch(r"[A-Z][a-zA-Z'\-]+(?: [A-Z][a-zA-Z'\-]+){1,3}", value):
        return True
    if re.fullmatch(r"[A-Z]\.\s?[A-Z][a-zA-Z'\-]+", value):
        return True

    return False


def _remove_layout_noise(soup: BeautifulSoup) -> BeautifulSoup:
    for selector in [
        "header", "nav", "footer", "aside", "script", "style", "noscript",
        ".gnb", ".lnb", ".breadcrumb", ".util", ".search", ".searchBox",
        ".quick", ".quick-menu", ".site-map", ".sitemap", ".footer",
        "#header", "#gnb", "#lnb", "#footer",
    ]:
        for node in soup.select(selector):
            node.decompose()
    return soup


def _extract_names_from_rendered_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    soup = _remove_layout_noise(soup)

    text_nodes = []
    for node in soup.select("strong, b, dt, dd, li, td, p, h3, h4, h5, span, a"):
        text = node.get_text(" ", strip=True)
        if not text:
            continue
        if len(text) > 80:
            continue
        text_nodes.append(text)

    candidates: list[str] = []
    for text in text_nodes:
        parts = [part.strip() for part in re.split(r"[|/,\n]|\s{2,}", text) if part.strip()]
        for part in parts:
            normalized = _normalize_professor_name(part)
            if _looks_like_professor_name(normalized):
                candidates.append(normalized)
                continue
            candidates.extend(_extract_korean_name_candidates(part))

    deduped: list[str] = []
    seen: set[str] = set()
    for name in candidates:
        key = name.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(name)

    return deduped




def _normalize_en_name(name: str) -> str:
    text = re.sub(r"[^A-Za-z\s\-]", " ", str(name or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _looks_like_english_name(text: str) -> bool:
    value = _normalize_en_name(text)
    if not value:
        return False

    parts = value.split()
    if len(parts) < 2 or len(parts) > 4:
        return False

    for p in parts:
        if len(p) < 2:
            return False
        if p.lower() in EN_BAD_WORDS:
            return False
        if not re.fullmatch(r"[A-Z][a-zA-Z\-]*", p):
            return False
    return True


def _extract_english_names_from_text(text: str) -> list[str]:
    if not text:
        return []

    candidates: list[str] = []
    pattern = re.compile(r"[A-Z][a-zA-Z\-]+(?:\s+[A-Z][a-zA-Z\-]+){1,3}")
    for m in pattern.finditer(text):
        name = _normalize_en_name(m.group(0))
        if _looks_like_english_name(name):
            candidates.append(name)

    deduped: list[str] = []
    seen: set[str] = set()
    for name in candidates:
        key = name.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(name)
    return deduped


def _extract_english_names_from_rendered_html(html: str) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    soup = _remove_layout_noise(soup)

    texts: list[str] = []
    for tag in soup.find_all(["li", "td", "div", "span", "p", "a", "strong"]):
        text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True))
        if text:
            texts.append(text)

    collected: list[str] = []
    for text in texts:
        collected.extend(_extract_english_names_from_text(text))

    deduped: list[str] = []
    seen: set[str] = set()
    for name in collected:
        key = name.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(name)
    return deduped


def ko_to_en_department_url(ko_url: str) -> str:
    url = str(ko_url or "").strip()
    if not url:
        return ""
    url = url.replace("/medicine/", "/medicine-en/")
    url = url.replace("/research/basic/", "/education/basic/")
    url = url.replace("/about/professor/clinic/", "/education/clinical/")
    url = url.replace("/about/professor/humanities/", "/education/humanistic/")
    return url


def _find_professor_staff_url(page, base_url: str) -> str:
    try:
        anchors = page.locator("a")
        count = min(anchors.count(), 300)
    except Exception:
        return base_url

    for i in range(count):
        try:
            a = anchors.nth(i)
            href = a.get_attribute("href") or ""
            text = re.sub(r"\s+", " ", a.inner_text() or "").strip().casefold()
            if "professor staff" in text or text == "professor":
                return urljoin(base_url, href)
        except Exception:
            continue
    return base_url


def _romanize_loose_korean_name(name: str) -> list[str]:
    name = str(name or "").strip()
    if not re.fullmatch(r"[가-힣]{2,4}", name):
        return []

    surname = name[0]
    given = name[1:]
    surname_opts = list(KOREAN_SURNAME_TO_EN.get(surname, set()) or KOR_ROMA.get(surname, []))
    if not surname_opts:
        surname_opts = [surname]

    given_opts = [""]
    for ch in given:
        roma = KOR_ROMA.get(ch, [ch])
        next_opts = []
        for prefix in given_opts:
            for r in roma:
                next_opts.append((prefix + r).strip())
                next_opts.append((prefix + " " + r).strip())
        given_opts = next_opts[:40]

    candidates: set[str] = set()
    for s in surname_opts:
        for g in given_opts:
            candidates.add(f"{g} {s}".strip().lower())
            candidates.add(f"{s} {g}".strip().lower())
            candidates.add(f"{g}{s}".strip().lower())
    return sorted(candidates)


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _match_korean_to_english_names(ko_names: list[str], en_names: list[str]) -> dict[str, str]:
    matched: dict[str, str] = {}
    used_en: set[int] = set()
    norm_en = [_normalize_en_name(x) for x in en_names]

    for i, ko_name in enumerate(ko_names):
        best_idx = -1
        best_score = -1
        ko_surname = ko_name[0] if ko_name else ""
        roma_candidates = _romanize_loose_korean_name(ko_name)

        for j, en_name in enumerate(norm_en):
            if j in used_en:
                continue
            parts = en_name.split()
            if len(parts) < 2:
                continue

            score = 0
            en_surname = parts[-1].lower()

            if abs(i - j) <= 1:
                score += 40
            if en_surname in KOREAN_SURNAME_TO_EN.get(ko_surname, set()):
                score += 35

            best_sim = 0.0
            for candidate in roma_candidates:
                best_sim = max(best_sim, _similarity(candidate, en_name.lower()))
            if best_sim >= 0.92:
                score += 30
            elif best_sim >= 0.85:
                score += 20
            elif best_sim >= 0.75:
                score += 10

            if score > best_score:
                best_score = score
                best_idx = j

        if best_idx >= 0 and best_score >= 60:
            matched[ko_name] = norm_en[best_idx]
            used_en.add(best_idx)
        else:
            matched[ko_name] = ""

    return matched


def _fetch_english_professor_names_for_department(page, ko_department_url: str, logger: Callable[[str], None] | None = None) -> list[str]:
    log = logger or (lambda _msg: None)
    en_url = ko_to_en_department_url(ko_department_url)
    if not en_url:
        return []

    try:
        page.goto(en_url, wait_until="domcontentloaded", timeout=20000)
        try:
            page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass

        professor_url = _find_professor_staff_url(page, en_url)
        if professor_url != en_url:
            page.goto(professor_url, wait_until="domcontentloaded", timeout=20000)
            try:
                page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:
                pass

        html = page.content()
        names = _extract_english_names_from_rendered_html(html)
        log(f"[YONSEI-EN] 영문 교수명 후보 {len(names)}명 | {professor_url}")
        return names
    except Exception as exc:
        log(f"[YONSEI-EN-WARN] 영문 페이지 수집 실패: {en_url} | {exc}")
        return []
def _slugify_debug_name(text: str) -> str:
    s = re.sub(r"[^\w가-힣\-\.]+", "_", str(text or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:80] or "page"


def _save_debug_page(page, debug_dir: Path, tag: str) -> None:
    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        slug = _slugify_debug_name(tag)
        (debug_dir / f"{slug}.html").write_text(page.content(), encoding="utf-8")
        page.screenshot(path=str(debug_dir / f"{slug}.png"), full_page=True)
    except Exception:
        pass


def _load_manual_department_urls() -> dict[str, str]:
    pages: dict[str, str] = {}

    for url in YONSEI_MEDICINE_DEPARTMENT_URLS:
        u = str(url or "").strip()
        if not u:
            continue
        dept = Path(u).stem
        pages.setdefault(u, dept)

    txt_path = Path(__file__).resolve().parent / "yonsei_department_urls.txt"
    if txt_path.exists():
        try:
            for line in txt_path.read_text(encoding="utf-8").splitlines():
                u = line.strip()
                if not u or u.startswith("#"):
                    continue
                dept = Path(u).stem
                pages.setdefault(u, dept)
        except Exception:
            pass

    return pages


def _clean_department_text(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    value = value.replace("교실소개", "").replace("교수소개", "").strip()
    return value


def _is_valid_department_link(index_url: str, href: str, text: str) -> tuple[bool, str]:
    raw_href = str(href or "").strip()
    raw_text = _clean_department_text(text)

    if not raw_href:
        return False, ""

    lowered_href = raw_href.lower()
    lowered_text = raw_text.casefold()

    if lowered_href.startswith("#"):
        return False, ""
    if any(token in lowered_href for token in BAD_LINK_HREF_SUBSTRINGS):
        return False, ""
    if not raw_text:
        return False, ""
    if len(raw_text) > 40:
        return False, ""
    if raw_text in BAD_LINK_TEXTS:
        return False, ""
    if any(token.casefold() in lowered_text for token in BAD_LINK_SUBSTRINGS):
        return False, ""
    if raw_text in BAD_NAME_WORDS:
        return False, ""

    full_url = urljoin(index_url, raw_href)
    parsed = urlparse(full_url)
    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    if parsed.netloc != "medicine.yonsei.ac.kr":
        return False, ""
    if not parsed.path.endswith(".do"):
        return False, ""

    allowed = (
        parsed.path.startswith("/medicine/research/basic/")
        or parsed.path.startswith("/medicine/about/professor/clinic/")
        or parsed.path.startswith("/medicine/about/professor/humanities/")
    )
    if not allowed:
        return False, ""

    banned_path_bits = [
        "/intro/", "/news/", "/board/", "/campus/", "/student/",
        "/admission/", "/support/",
    ]
    if any(bit in parsed.path for bit in banned_path_bits):
        # 실제 교수/교실 페이지 경로는 허용
        if not allowed:
            return False, ""

    return True, clean_url


def _guess_department_name_from_url(page_url: str) -> str:
    stem = Path(urlparse(page_url).path).stem
    stem = stem.replace("-", " ").replace("_", " ").strip()
    return stem or page_url


def _is_department_like_url(page_url: str) -> bool:
    path = urlparse(str(page_url or "")).path
    return (
        path.startswith("/medicine/research/basic/")
        or path.startswith("/medicine/about/professor/clinic/")
        or path.startswith("/medicine/about/professor/humanities/")
    ) and path.endswith('.do')


def _collect_department_links(page, index_url: str) -> dict[str, str]:
    links: dict[str, str] = {}

    anchors = page.locator("a[href]")
    count = anchors.count()

    for i in range(count):
        try:
            a = anchors.nth(i)
            href = a.get_attribute("href") or ""
            text = re.sub(r"\s+", " ", a.inner_text() or "").strip()
            ok, clean_url = _is_valid_department_link(index_url, href, text)
            if not ok:
                continue

            clean_text = _clean_department_text(text)
            if any(tok in clean_text for tok in ["전체교실", "검색", "닫기", "홈페이지"]):
                continue

            links.setdefault(clean_url, clean_text)
        except Exception:
            continue

    return links
def fetch_latest_yonsei_professors(
    *,
    logger: Callable[[str], None] | None = None,
    timeout_ms: int = 20000,
    debug_dir: str | Path | None = None,
    group_filter: str = "전체",
) -> list[dict[str, str]]:
    def _log(msg: str) -> None:
        if logger:
            logger(msg)

    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    department_pages: dict[str, str] = {}
    index_link_count = 0
    dept_name_count = 0

    group_urls = {
        "기초의학": [u for u in YONSEI_MEDICINE_PROFESSOR_INDEX_URLS if "/medicine/research/basic/" in u],
        "임상의학": [u for u in YONSEI_MEDICINE_PROFESSOR_INDEX_URLS if "/medicine/about/professor/clinic/" in u],
        "인문의학": [u for u in YONSEI_MEDICINE_PROFESSOR_INDEX_URLS if "/medicine/about/professor/humanities/" in u],
    }
    target_index_urls = group_urls.get(group_filter, YONSEI_MEDICINE_PROFESSOR_INDEX_URLS)

    if debug_dir is None:
        debug_path = Path(__file__).resolve().parent / "_debug_yonsei"
    else:
        debug_path = Path(debug_dir)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for idx, index_url in enumerate(target_index_urls, start=1):
            try:
                page.goto(index_url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=min(5000, timeout_ms))
                except Exception:
                    pass
                page.wait_for_timeout(1200)
                _save_debug_page(page, debug_path, f"index_{idx}")

                found = _collect_department_links(page, index_url)
                if not found and _is_department_like_url(index_url):
                    guessed_name = _guess_department_name_from_url(index_url)
                    found = {index_url: guessed_name}
                    _log(f"[YONSEI] 직접 교실 페이지로 처리: {guessed_name} | {index_url}")

                index_link_count += len(found)
                department_pages.update(found)

                _log(f"[YONSEI] 인덱스 수집: {index_url} -> {len(found)}개 교실")
                _log("[YONSEI] 유효 교실 링크만 필터링 완료")
                for u, d in found.items():
                    _log(f"[YONSEI] 교실 링크: {d} | {u}")

            except Exception as exc:
                _log(f"[YONSEI-WARN] 인덱스 조회 실패: {index_url} | {exc}")

        if not department_pages:
            manual_pages = _load_manual_department_urls()
            if group_filter == "기초의학":
                manual_pages = {u: d for u, d in manual_pages.items() if "/medicine/research/basic/" in u}
            elif group_filter == "임상의학":
                manual_pages = {u: d for u, d in manual_pages.items() if "/medicine/about/professor/clinic/" in u}
            elif group_filter == "인문의학":
                manual_pages = {u: d for u, d in manual_pages.items() if "/medicine/about/professor/humanities/" in u}
            if manual_pages:
                department_pages.update(manual_pages)
                _log(f"[YONSEI] 자동수집 0건 -> 수동 시드 사용: {len(manual_pages)}개")
                for u, d in manual_pages.items():
                    _log(f"[YONSEI] 수동 시드: {d} | {u}")

        _log(f"[YONSEI] 교수명단 스캔 범위 축소: 구분={group_filter} | 교실 {len(department_pages)}개")

        for n, (page_url, dept_name) in enumerate(department_pages.items(), start=1):
            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=min(5000, timeout_ms))
                except Exception:
                    pass
                page.wait_for_timeout(1200)
                _save_debug_page(page, debug_path, f"dept_{n}_{dept_name}")

                html = page.content()
                names = _extract_names_from_rendered_html(html)
                english_name_map = _extract_korean_english_name_pairs(html)
                en_names = _fetch_english_professor_names_for_department(page, page_url, logger=_log)
                matched_map = _match_korean_to_english_names(names, en_names)

                for ko_name, en_name in english_name_map.items():
                    normalized_en = _normalize_english_professor_name(en_name)
                    if normalized_en:
                        matched_map[ko_name] = normalized_en

                matched_count = sum(1 for v in matched_map.values() if v)
                _log(
                    f"[YONSEI] {dept_name}: 이름 후보 {len(names)}명"
                    f" | 영문매칭 {matched_count}명"
                )

                if not names:
                    continue

                dept_name_count += len(names)

                for name in names:
                    english_name = _normalize_english_professor_name(matched_map.get(name, ""))
                    query = english_name or _normalize_professor_name_for_query(name)
                    if not query:
                        continue

                    key = (dept_name.casefold(), name.casefold())
                    if key in seen:
                        continue
                    seen.add(key)

                    rows.append({
                        "name": name,
                        "query": query,
                        "department": dept_name,
                        "affiliation": "Yonsei OR Severance",
                        "source_url": page_url,
                        "orcid": "",
                    })

            except Exception as exc:
                _log(f"[YONSEI-WARN] 교수 페이지 조회 실패: {dept_name} | {page_url} | {exc}")

        browser.close()

    if not rows:
        summary = {
            "index_link_count": index_link_count,
            "department_page_count": len(department_pages),
            "name_candidate_count": dept_name_count,
            "debug_dir": str(debug_path),
        }
        try:
            debug_path.mkdir(parents=True, exist_ok=True)
            (debug_path / "summary.json").write_text(
                json.dumps(summary, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

        raise RuntimeError(
            "연세대 교수명단을 0건 가져왔습니다. "
            f"인덱스링크={index_link_count}, 교실페이지={len(department_pages)}, 이름후보={dept_name_count}. "
            f"디버그 파일 확인: {debug_path}"
        )

    rows.sort(key=lambda row: (row.get("department", ""), row.get("name", "")))
    return rows


def validate_exported_rows(rows: list[dict[str, str]]) -> None:
    sample_bad = {"검색", "닫기", "뉴스", "채용", "주메뉴", "전체메뉴", "동아리"}
    polluted = [r for r in rows if r.get("name", "") in sample_bad]
    if polluted:
        raise ValueError(f"교수 명단 오염 감지: bad_rows={len(polluted)}")




def categorize_yonsei_professor_group(source_url: str, department: str = "") -> str:
    url = str(source_url or "").lower()
    dept = str(department or "").strip()

    if "/medicine/research/basic/" in url:
        return "기초의학"
    if "/medicine/about/professor/clinic/" in url:
        return "임상의학"
    if "/medicine/about/professor/humanities/" in url:
        return "인문의학"

    basic_keywords = ["해부", "생리", "약리", "미생물", "병리", "예방", "생화학", "의생명", "시스템", "열대", "의용"]
    humanities_keywords = ["법", "윤리", "의학교육", "의사학", "인문", "사회", "통일", "global health", "forensic"]
    clinic_keywords = ["내과", "외과", "정형", "정신", "영상", "마취", "산부인과", "비뇨", "소아", "피부", "재활", "응급", "안과", "이비인후", "신경", "흉부", "진단", "방사선", "가정", "검사의학"]

    if any(k in dept for k in basic_keywords):
        return "기초의학"
    if any(k in dept for k in humanities_keywords):
        return "인문의학"
    if any(k in dept for k in clinic_keywords):
        return "임상의학"
    return "전체"


def export_latest_yonsei_professors_csv(
    output_csv: str | Path,
    *,
    group_filter: str = "전체",
    logger: Callable[[str], None] | None = None,
) -> dict[str, int | str]:
    output_csv = Path(output_csv)
    debug_dir = output_csv.parent / "_debug_yonsei"

    rows = fetch_latest_yonsei_professors(
        logger=logger,
        debug_dir=debug_dir,
        group_filter=group_filter,
    )
    validate_exported_rows(rows)

    normalized_group_filter = str(group_filter or "전체").strip() or "전체"
    valid_groups = {"전체", "기초의학", "임상의학", "인문의학"}
    if normalized_group_filter not in valid_groups:
        normalized_group_filter = "전체"

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    normalized_rows: list[dict[str, str]] = []
    for row in rows:
        professor_group = categorize_yonsei_professor_group(
            row.get("source_url", ""),
            row.get("department", ""),
        )
        normalized_row = {
            "group": professor_group,
            "name": row.get("name", ""),
            "query": row.get("query", ""),
            "department": row.get("department", ""),
            "affiliation": row.get("affiliation", ""),
            "source_url": row.get("source_url", ""),
            "orcid": row.get("orcid", ""),
        }
        if normalized_group_filter == "전체" or professor_group == normalized_group_filter:
            normalized_rows.append(normalized_row)

    df = pd.DataFrame(normalized_rows, columns=[
        "group",
        "name",
        "query",
        "department",
        "affiliation",
        "source_url",
        "orcid",
    ])
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    if logger:
        logger(
            f"[YONSEI] CSV 저장 완료: {output_csv} | 구분={normalized_group_filter} | {len(df)} rows"
        )
        logger(f"[YONSEI] 디버그 폴더: {debug_dir}")

    return {
        "rows": len(df),
        "professors": len(df),
        "group": normalized_group_filter,
        "output_csv": str(output_csv),
    }


def load_professors_file(file_path: str | Path) -> list[dict[str, str]]:
    path = Path(file_path)
    entries: list[dict[str, str]] = []

    if path.suffix.lower() == '.txt':
        for line in path.read_text(encoding='utf-8').splitlines():
            name = line.strip()
            if not name or name.startswith('#'):
                continue
            entries.append({
                'name': name,
                'query': name,
                'department': '',
                'affiliation': 'Yonsei OR Severance',
                'group': '',
                'orcid': '',
            })
        return entries

    with path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = str(
                row.get('name')
                or row.get('Name')
                or row.get('professor')
                or row.get('Professor')
                or row.get('교수명')
                or ''
            ).strip()
            query = str(
                row.get('query')
                or row.get('Query')
                or row.get('pubmed_query')
                or row.get('PubMed Query')
                or row.get('검색명')
                or name
            ).strip()
            department = str(
                row.get('department')
                or row.get('Department')
                or row.get('소속')
                or ''
            ).strip()
            affiliation = str(
                row.get('affiliation')
                or row.get('Affiliation')
                or row.get('기관필터')
                or 'Yonsei OR Severance'
            ).strip()
            group = str(
                row.get('group')
                or row.get('Group')
                or row.get('category')
                or row.get('Category')
                or row.get('division')
                or row.get('Division')
                or row.get('구분')
                or ''
            ).strip()
            orcid = str(
                row.get('orcid')
                or row.get('ORCID')
                or row.get('orcid_id')
                or row.get('ORCID ID')
                or row.get('오르시드')
                or ''
            ).strip()

            if not name and not query:
                continue

            entries.append({
                'name': name or query,
                'query': query or name,
                'department': department,
                'affiliation': affiliation or 'Yonsei OR Severance',
                'group': group,
                'orcid': normalize_orcid(orcid),
            })

    return entries


def _build_pubmed_author_term(author_query: str, affiliation: str = 'Yonsei OR Severance') -> str:
    author_query = str(author_query or '').strip()
    affiliation = str(affiliation or '').strip()

    if not author_query:
        return ''

    author_term = f'"{author_query}"[Author]'
    if not affiliation:
        return author_term

    tokens = [token.strip() for token in re.split(r'\s+OR\s+|,', affiliation, flags=re.IGNORECASE) if token.strip()]
    aff_term = ' OR '.join(f'"{token}"[Affiliation]' for token in tokens)
    return f'({author_term}) AND ({aff_term})' if aff_term else author_term


def search_pubmed_by_author(
    author_query: str,
    *,
    affiliation: str = 'Yonsei OR Severance',
    email: str = '',
    retmax: int = 20,
) -> list[str]:
    term = _build_pubmed_author_term(author_query, affiliation=affiliation)
    if not term:
        return []

    params = {
        'db': 'pubmed',
        'term': term,
        'retmode': 'json',
        'retmax': max(1, min(int(retmax or 20), 200)),
        'sort': 'pub date',
        'tool': 'academic-paper-organizer',
    }
    if email:
        params['email'] = email

    response = requests.get(
        'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi',
        params=params,
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json() or {}
    return list((payload.get('esearchresult') or {}).get('idlist') or [])


def fetch_pubmed_summaries(pmids: list[str], *, email: str = '') -> list[dict]:
    ids = [str(p).strip() for p in pmids if str(p).strip()]
    if not ids:
        return []

    params = {
        'db': 'pubmed',
        'id': ','.join(ids),
        'retmode': 'json',
        'version': '2.0',
        'tool': 'academic-paper-organizer',
    }
    if email:
        params['email'] = email

    response = requests.get(
        'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi',
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json() or {}
    result = payload.get('result') or {}

    rows: list[dict] = []
    for pmid in ids:
        item = result.get(pmid) or {}
        if item:
            rows.append(item)
    return rows


def normalize_orcid(value: str) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    text = re.sub(r'^https?://orcid\.org/', '', text, flags=re.IGNORECASE)
    text = text.upper()
    text = re.sub(r'[^0-9X]', '', text)
    if len(text) == 16:
        text = f"{text[:4]}-{text[4:8]}-{text[8:12]}-{text[12:]}"
    return text


def fetch_pubmed_author_details(pmids: list[str], *, email: str = '') -> dict[str, dict]:
    ids = [str(p).strip() for p in pmids if str(p).strip()]
    if not ids:
        return {}

    params = {
        'db': 'pubmed',
        'id': ','.join(ids),
        'retmode': 'xml',
        'tool': 'academic-paper-organizer',
    }
    if email:
        params['email'] = email

    response = requests.get(
        'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi',
        params=params,
        timeout=25,
    )
    response.raise_for_status()

    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.text)
    except Exception:
        return {}

    detail_map: dict[str, dict] = {}
    for article in root.findall('.//PubmedArticle'):
        pmid = ''.join(article.findtext('.//MedlineCitation/PMID') or '').strip()
        if not pmid:
            continue

        author_rows: list[dict[str, object]] = []
        for author in article.findall('.//AuthorList/Author'):
            collective = (author.findtext('CollectiveName') or '').strip()
            last = (author.findtext('LastName') or '').strip()
            fore = (author.findtext('ForeName') or '').strip()
            initials = (author.findtext('Initials') or '').strip()

            if collective:
                display_name = collective
            else:
                display_name = ' '.join(part for part in [fore, last] if part).strip() or ' '.join(part for part in [last, initials] if part).strip()

            affiliations = [
                ' '.join((aff.text or '').split())
                for aff in author.findall('.//AffiliationInfo/Affiliation')
                if (aff.text or '').strip()
            ]
            identifiers = {}
            for ident in author.findall('Identifier'):
                source = str(ident.attrib.get('Source') or '').strip().lower()
                value = str(ident.text or '').strip()
                if source and value:
                    identifiers[source] = value

            author_rows.append({
                'name': display_name,
                'lastname': last,
                'forename': fore,
                'initials': initials,
                'affiliations': affiliations,
                'orcid': normalize_orcid(identifiers.get('orcid', '')),
            })

        detail_map[pmid] = {'authors': author_rows}
    return detail_map


def _extract_pubmed_doi(item: dict) -> str:
    articleids = item.get('articleids') or []
    for article_id in articleids:
        if str(article_id.get('idtype', '')).lower() == 'doi':
            return normalize_doi(str(article_id.get('value', '')).strip())
    elocation = str(item.get('elocationid') or '').strip()
    if '10.' in elocation.lower():
        return normalize_doi(elocation)
    return ''


def _format_pubmed_authors(item: dict, max_authors: int = 8) -> str:
    authors = item.get('authors') or []
    names = []
    for author in authors[:max_authors]:
        name = str(author.get('name') or '').strip()
        if name:
            names.append(name)
    if len(authors) > max_authors:
        names.append('et al.')
    return '; '.join(names)

def _normalize_person_name(name: str) -> str:
    text = str(name or '').strip().casefold()
    if not text:
        return ''
    text = re.sub(r"<[^>]+>", ' ', text)
    text = re.sub(r"[^a-z0-9가-힣\s]", ' ', text)
    text = re.sub(r"\s+", ' ', text).strip()
    return text


def _author_name_variants(name: str) -> set[str]:
    base = _normalize_person_name(name)
    if not base:
        return set()

    variants = {base}
    parts = [p for p in base.split() if p]
    if not parts:
        return variants

    variants.add(' '.join(parts))
    variants.add(''.join(parts))

    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
        middle = parts[1:-1]
        initials = ''.join(p[0] for p in [first, *middle] if p)
        if initials:
            variants.add(f'{last} {initials}')
            variants.add(f'{last} {initials[0]}')
            variants.add(f'{first} {last}')
            variants.add(f'{last} {first}')
        variants.add(f'{last} {first[0]}')

    return {v.strip() for v in variants if v.strip()}


def _names_match(candidate: str, target: str) -> bool:
    cand_vars = _author_name_variants(candidate)
    targ_vars = _author_name_variants(target)
    if not cand_vars or not targ_vars:
        return False
    if cand_vars & targ_vars:
        return True

    c = _normalize_person_name(candidate)
    t = _normalize_person_name(target)
    if not c or not t:
        return False
    return c in t or t in c


def _get_pubmed_author_names(item: dict) -> list[str]:
    authors = item.get('authors') or []
    names: list[str] = []
    for author in authors:
        name = str(author.get('name') or '').strip()
        if name:
            names.append(name)
    return names


def _is_corresponding_author(detail_author: dict, *, is_last_author: bool = False) -> bool:
    affiliations = [str(x or '').strip() for x in (detail_author.get('affiliations') or []) if str(x or '').strip()]
    combined = ' '.join(affiliations).casefold()
    has_email = '@' in combined
    has_correspondence_marker = any(token in combined for token in [
        'correspond',
        'correspondence to',
        'corresponding author',
        'reprint requests',
        'address correspondence',
    ])
    if has_correspondence_marker:
        return True
    if has_email and is_last_author:
        return True
    return False


def classify_professor_author_role(
    professor_name: str,
    author_query: str,
    item: dict,
    detail_item: dict | None = None,
    professor_orcid: str = '',
) -> tuple[str, str, str, str]:
    detail_authors = list((detail_item or {}).get('authors') or [])
    summary_author_names = _get_pubmed_author_names(item)

    targets = [t for t in [professor_name, author_query] if str(t).strip()]
    professor_orcid = normalize_orcid(professor_orcid)

    matched_name = ''
    matched_orcid = ''
    matched_index = -1
    role_basis = ''

    if professor_orcid and detail_authors:
        for idx, author in enumerate(detail_authors):
            author_orcid = normalize_orcid(str(author.get('orcid') or ''))
            if author_orcid and author_orcid == professor_orcid:
                matched_name = str(author.get('name') or '').strip()
                matched_orcid = author_orcid
                matched_index = idx
                role_basis = 'orcid'
                break

    if matched_index < 0 and detail_authors:
        for idx, author in enumerate(detail_authors):
            author_name = str(author.get('name') or '').strip()
            if author_name and any(_names_match(author_name, target) for target in targets):
                matched_name = author_name
                matched_orcid = normalize_orcid(str(author.get('orcid') or ''))
                matched_index = idx
                role_basis = 'name_detail'
                break

    if matched_index < 0 and summary_author_names:
        for idx, author_name in enumerate(summary_author_names):
            if any(_names_match(author_name, target) for target in targets):
                matched_name = author_name
                matched_index = idx
                role_basis = 'name_summary'
                break

    if matched_index < 0:
        return '미확인', '', '', ''

    author_count = len(detail_authors) if detail_authors else len(summary_author_names)
    is_first = matched_index == 0
    is_last = author_count > 1 and matched_index == author_count - 1

    if detail_authors and 0 <= matched_index < len(detail_authors):
        if _is_corresponding_author(detail_authors[matched_index], is_last_author=is_last):
            if is_first and author_count == 1:
                return '단독저자(제1·교신)', matched_name, matched_orcid, role_basis or 'detail'
            if is_first:
                return '제1저자', matched_name, matched_orcid, role_basis or 'detail'
            return '교신저자', matched_name, matched_orcid, role_basis or 'detail'

    if author_count == 1:
        return '단독저자(제1·교신)', matched_name, matched_orcid, role_basis or 'single_author'
    if is_first:
        return '제1저자', matched_name, matched_orcid, role_basis or 'first_author'
    if is_last:
        return '교신저자(추정)', matched_name, matched_orcid, role_basis or 'last_author'
    return '참여저자', matched_name, matched_orcid, role_basis or 'middle_author'


def export_professor_achievements_csv(
    professors_file: str | Path,
    output_csv: str | Path,
    *,
    email: str = '',
    per_professor_limit: int = 20,
    group_filter: str = '전체',
    logger: Callable[[str], None] | None = None,
) -> dict[str, int | str]:
    def _log(msg: str) -> None:
        if logger:
            logger(msg)

    professors = load_professors_file(professors_file)

    normalized_group_filter = str(group_filter or '전체').strip() or '전체'
    valid_groups = {'전체', '기초의학', '임상의학', '인문의학'}
    if normalized_group_filter not in valid_groups:
        normalized_group_filter = '전체'

    if normalized_group_filter != '전체':
        filtered_professors: list[dict[str, str]] = []
        for professor in professors:
            professor_group = str(professor.get('group', '')).strip()
            if not professor_group:
                professor_group = categorize_yonsei_professor_group('', professor.get('department', ''))
            if professor_group == normalized_group_filter:
                filtered_professors.append(professor)
        professors = filtered_professors
        _log(f'[PROF] 구분 필터 적용: {normalized_group_filter} | 대상 교수 {len(professors)}명')
    else:
        _log(f'[PROF] 구분 필터 적용 안 함: {normalized_group_filter} | 대상 교수 {len(professors)}명')

    output_csv = Path(output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    rows_to_write: list[dict[str, str]] = []
    professor_count = 0
    paper_count = 0
    error_count = 0

    for professor in professors:
        professor_count += 1
        name = professor.get('name', '').strip()
        query = professor.get('query', '').strip() or name
        department = professor.get('department', '').strip()
        affiliation = professor.get('affiliation', '').strip() or 'Yonsei OR Severance'
        professor_orcid = normalize_orcid(professor.get('orcid', '').strip())

        _log(f'[PROF] 조회 시작: {name} | query={query} | affiliation={affiliation} | orcid={professor_orcid}')

        try:
            pmids = search_pubmed_by_author(
                query,
                affiliation=affiliation,
                email=email,
                retmax=per_professor_limit,
            )
            if not pmids:
                _log(f'[PROF] 결과 없음: {name}')
                continue

            summaries = fetch_pubmed_summaries(pmids, email=email)
            details_by_pmid = fetch_pubmed_author_details(pmids, email=email)
            _log(f'[PROF] {name}: PMID {len(pmids)}건, 요약 {len(summaries)}건')

            for item in summaries:
                title = str(item.get('title') or '').strip()
                journal = str(item.get('fulljournalname') or item.get('source') or '').strip()
                pubdate = str(item.get('pubdate') or '').strip()
                doi = _extract_pubmed_doi(item)
                pmid = str(item.get('uid') or '').strip()
                authors = _format_pubmed_authors(item)
                detail_item = details_by_pmid.get(pmid) or {}
                author_role, matched_author_name, matched_author_orcid, role_basis = classify_professor_author_role(
                    name,
                    query,
                    item,
                    detail_item=detail_item,
                    professor_orcid=professor_orcid,
                )
                scie, impact_factor, quartile, openalex_score = fetch_journal_metrics(journal)
                if not quartile and impact_factor:
                    quartile = classify_quartile(impact_factor)

                rows_to_write.append({
                    'professor_name': name,
                    'matched_author_name': matched_author_name,
                    'matched_author_orcid': matched_author_orcid,
                    'author_role': author_role,
                    'author_role_basis': role_basis,
                    'professor_query': query,
                    'professor_orcid': professor_orcid,
                    'group': normalized_group_filter if normalized_group_filter != '전체' else (professor.get('group', '') or categorize_yonsei_professor_group('', department)),
                    'department': department,
                    'affiliation_filter': affiliation,
                    'pmid': pmid,
                    'title': title,
                    'journal': journal,
                    'year': normalize_year(pubdate, allow_historic=True) if pubdate else '',
                    'pubdate': pubdate,
                    'doi': doi,
                    'authors': authors,
                    'scie': scie,
                    'impact_factor': impact_factor,
                    'quartile': quartile,
                    'openalex_score': openalex_score,
                    'pubmed_url': f'https://pubmed.ncbi.nlm.nih.gov/{pmid}/' if pmid else '',
                    'doi_url': f'https://doi.org/{doi}' if doi else '',
                })
                paper_count += 1
        except Exception as exc:
            error_count += 1
            _log(f'[PROF-ERROR] {name}: {exc}')

    rows_to_write.sort(key=lambda row: (row.get('professor_name', ''), row.get('pubdate', '')), reverse=False)

    fieldnames = [
        'professor_name',
        'matched_author_name',
        'matched_author_orcid',
        'author_role',
        'author_role_basis',
        'professor_query',
        'professor_orcid',
        'group',
        'department',
        'affiliation_filter',
        'pmid',
        'title',
        'journal',
        'year',
        'pubdate',
        'doi',
        'authors',
        'scie',
        'impact_factor',
        'quartile',
        'openalex_score',
        'pubmed_url',
        'doi_url',
    ]

    with output_csv.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_to_write)

    _log(f'[PROF] CSV 저장 완료: {output_csv}')
    return {
        'professors': professor_count,
        'papers': paper_count,
        'errors': error_count,
        'group_filter': normalized_group_filter,
        'output_csv': str(output_csv),
    }


# === 2026-03-27 patch: tighter professor-name extraction and more robust English-page scraping ===
BAD_NAME_WORDS = set(BAD_NAME_WORDS) | {
    "교육", "분야", "생리", "생리학", "신경생리", "이메일", "줄기세포", "평활근",
    "홈페이지", "화면인쇄", "화면축소", "화면확대", "면역학", "미생물학", "박사",
    "발생학", "생물", "세포역학", "시작", "신경과학", "전문의", "정신의학",
    "교수", "부교수", "조교수", "임상교수", "임상부교수", "임상조교수",
    "중계교수", "중견교수", "직위", "전공", "연구실", "연구분야", "학력", "경력",
    "전화", "연락처", "위치", "소개", "논문", "더보기", "목록", "상세", "프로필",
}

BAD_NAME_SUBSTRINGS = list(dict.fromkeys(BAD_NAME_SUBSTRINGS + [
    "이메일", "homepage", "home page", "연구분야", "세부전공", "전화", "연락처",
    "교육", "화면인쇄", "화면확대", "화면축소", "교실", "school of", "professor staff",
]))

PROF_SECTION_HINTS = [
    "prof", "faculty", "staff", "member", "doctor", "people", "teacher", "lab", "researcher",
    "교수", "연구원", "의사", "staff", "table", "list", "card", "profile",
]

ROLE_CONTEXT_WORDS = [
    "교수", "부교수", "조교수", "임상교수", "임상부교수", "임상조교수", "Professor", "M.D.", "Ph.D.",
]


def _looks_like_professor_name(text: str) -> bool:
    value = _normalize_professor_name(text)
    if not value:
        return False

    if len(value) < 2 or len(value) > 12:
        return False

    if value in BAD_NAME_WORDS:
        return False

    lowered = value.casefold()
    if any(token.casefold() in lowered for token in BAD_NAME_SUBSTRINGS):
        return False

    if re.search(r"\d", value):
        return False

    if re.search(r"[A-Za-z]", value) and not re.fullmatch(r"[A-Z][a-zA-Z'\-]+(?: [A-Z][a-zA-Z'\-]+){1,3}", value):
        return False

    if re.fullmatch(r"[가-힣]{2,4}", value):
        if value.endswith(("학", "과", "실", "부", "연구", "교육")):
            return False
        return True

    if re.fullmatch(r"[가-힣]{1,3}\s[가-힣]{1,3}", value):
        return True

    if re.fullmatch(r"[A-Z][a-zA-Z'\-]+(?: [A-Z][a-zA-Z'\-]+){1,3}", value):
        return True
    if re.fullmatch(r"[A-Z]\.\s?[A-Z][a-zA-Z'\-]+", value):
        return True

    return False


def _node_has_professor_context(node) -> bool:
    try:
        attrs = " ".join([
            node.get("id", "") or "",
            " ".join(node.get("class", []) or []),
        ]).casefold()
    except Exception:
        attrs = ""

    text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
    lowered = text.casefold()

    if any(h in attrs for h in PROF_SECTION_HINTS):
        return True
    if any(h in lowered for h in ["교수", "부교수", "조교수", "이메일", "homepage", "professor", "staff"]):
        return True
    return False


def _extract_names_from_node_text(text: str) -> list[str]:
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"[|/,\n]|\s{2,}", text) if part.strip()]
    candidates = []
    for part in parts:
        normalized = _normalize_professor_name(part)
        if _looks_like_professor_name(normalized):
            candidates.append(normalized)
            continue
        if any(ctx in part for ctx in ROLE_CONTEXT_WORDS) or any(ctx in part for ctx in ["이메일", "홈페이지"]):
            candidates.extend(_extract_korean_name_candidates(part))
    deduped = []
    seen = set()
    for name in candidates:
        if name not in seen:
            seen.add(name)
            deduped.append(name)
    return deduped


def _extract_names_from_rendered_html(html: str) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    soup = _remove_layout_noise(soup)

    focused_nodes = []
    for node in soup.find_all(["div", "li", "tr", "dl", "section", "article", "td", "p"]):
        text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
        if not text or len(text) > 300:
            continue
        if _node_has_professor_context(node):
            focused_nodes.append(text)

    text_nodes = focused_nodes
    if not text_nodes:
        for node in soup.select("strong, b, dt, dd, li, td, p, h3, h4, h5, span, a"):
            text = node.get_text(" ", strip=True)
            if not text or len(text) > 80:
                continue
            text_nodes.append(text)

    candidates = []
    for text in text_nodes:
        candidates.extend(_extract_names_from_node_text(text))

    deduped = []
    seen = set()
    for name in candidates:
        key = name.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(name)
    return deduped


def _looks_like_english_name(text: str) -> bool:
    value = _normalize_en_name(text)
    if not value:
        return False
    parts = value.split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    for p in parts:
        low = p.lower()
        if len(p) < 2:
            return False
        if low in EN_BAD_WORDS:
            return False
        if not re.fullmatch(r"[A-Z][a-zA-Z\-]*", p):
            return False
    surname_like = {x for vals in KOREAN_SURNAME_TO_EN.values() for x in vals}
    if parts[-1].lower() not in surname_like and parts[0].lower() not in surname_like and len(parts) > 3:
        return False
    return True


def _extract_english_names_from_rendered_html(html: str) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    soup = _remove_layout_noise(soup)
    texts = []
    for node in soup.find_all(["div", "li", "tr", "dl", "section", "article", "td", "p", "span", "strong", "a"]):
        text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
        if not text or len(text) > 250:
            continue
        if _node_has_professor_context(node) or any(w in text for w in ["Professor", "M.D.", "Ph.D.", "E-mail", "Homepage"]):
            texts.append(text)
    if not texts:
        texts.append(re.sub(r"\s+", " ", soup.get_text(" ", strip=True)))

    collected = []
    for text in texts:
        collected.extend(_extract_english_names_from_text(text))

    deduped = []
    seen = set()
    for name in collected:
        key = name.casefold()
        if key not in seen:
            seen.add(key)
            deduped.append(name)
    return deduped


def _find_professor_staff_url(page, base_url: str) -> str:
    candidates = [
        "a:has-text('Professor Staff')",
        "a:has-text('Professor')",
        "button:has-text('Professor Staff')",
        "button:has-text('Professor')",
        "text=Professor Staff",
    ]
    for selector in candidates:
        try:
            loc = page.locator(selector).first
            if loc.count() > 0:
                href = loc.get_attribute('href') or ''
                if href:
                    return urljoin(base_url, href)
        except Exception:
            continue
    try:
        anchors = page.locator('a')
        count = min(anchors.count(), 400)
    except Exception:
        return base_url
    for i in range(count):
        try:
            a = anchors.nth(i)
            href = a.get_attribute('href') or ''
            text = re.sub(r'\s+', ' ', a.inner_text() or '').strip().casefold()
            if 'professor staff' in text or text == 'professor':
                return urljoin(base_url, href)
        except Exception:
            continue
    return base_url


def _try_open_professor_staff_tab(page) -> None:
    selectors = [
        "a:has-text('Professor Staff')",
        "button:has-text('Professor Staff')",
        "a:has-text('Professor')",
        "button:has-text('Professor')",
        "text=Professor Staff",
    ]
    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if loc.count() > 0:
                loc.click(timeout=3000)
                page.wait_for_timeout(1200)
                return
        except Exception:
            continue


def _fetch_english_professor_names_for_department(page, ko_department_url: str, logger: Callable[[str], None] | None = None) -> list[str]:
    log = logger or (lambda _msg: None)
    en_url = ko_to_en_department_url(ko_department_url)
    if not en_url:
        return []
    try:
        page.goto(en_url, wait_until='domcontentloaded', timeout=20000)
        page.wait_for_timeout(1500)
        professor_url = _find_professor_staff_url(page, en_url)
        if professor_url and professor_url != en_url:
            page.goto(professor_url, wait_until='domcontentloaded', timeout=20000)
            page.wait_for_timeout(1500)
        _try_open_professor_staff_tab(page)
        html = page.content()
        names = _extract_english_names_from_rendered_html(html)
        if not names:
            for selector in ['#tab-content1', '.tab-content', '.professor', '.faculty', '.staff', '.member']:
                try:
                    loc = page.locator(selector)
                    if loc.count() > 0:
                        snippet = ' '.join([loc.nth(i).inner_text() for i in range(min(loc.count(), 5))])
                        names = _extract_english_names_from_text(snippet)
                        if names:
                            break
                except Exception:
                    continue
        log(f"[YONSEI-EN] 영문 교수명 후보 {len(names)}명 | {professor_url or en_url}")
        return names
    except Exception as exc:
        log(f"[YONSEI-EN-WARN] 영문 페이지 수집 실패: {en_url} | {exc}")
        return []


# ==============================
# Professor registry-based collection
# ==============================

@dataclass
class ProfessorSourceRow:
    id: str
    group: str
    department_ko: str
    department_en: str
    page_type: str
    url_ko: str
    url_en: str
    active: str
    note: str = ""


@dataclass
class ProfessorResultRow:
    group: str
    department: str
    name: str
    query: str
    title: str
    email: str
    affiliation: str
    source_url: str
    source_url_en: str
    match_status: str
    review_status: str = "pending"


REGISTRY_TEMPLATE_ROWS: list[ProfessorSourceRow] = [
    ProfessorSourceRow(
        id="1",
        group="기초의학",
        department_ko="생화학분자생물학교실",
        department_en="Department of Biochemistry and Molecular Biology",
        page_type="department_list",
        url_ko="https://medicine.yonsei.ac.kr/medicine/research/basic/biochemistry-molecular-biology.do",
        url_en="https://medicine.yonsei.ac.kr/medicine-en/education/basic/biochemistry-molecular-biology.do",
        active="Y",
        note="",
    ),
    ProfessorSourceRow(
        id="2",
        group="기초의학",
        department_ko="해부학교실",
        department_en="Department of Anatomy",
        page_type="department_list",
        url_ko="https://medicine.yonsei.ac.kr/medicine/research/basic/anatomy.do",
        url_en="https://medicine.yonsei.ac.kr/medicine-en/education/basic/anatomy.do",
        active="Y",
        note="",
    ),
    ProfessorSourceRow(
        id="3",
        group="임상의학",
        department_ko="안과학교실",
        department_en="Department of Ophthalmology",
        page_type="department_list",
        url_ko="https://medicine.yonsei.ac.kr/medicine/about/professor/clinic/ophthalmology.do",
        url_en="https://medicine.yonsei.ac.kr/medicine-en/education/clinical/ophthalmology.do",
        active="Y",
        note="",
    ),
]


BAD_PROF_NAME_TOKENS = {
    "교육", "분야", "이메일", "홈페이지", "화면인쇄", "연구", "블로그",
    "교수실", "전화", "연락처", "소개", "학력", "경력", "논문", "특임",
    "교원", "연세", "의과대학", "세브란스", "홈", "더보기", "링크",
}


def load_professor_source_registry(csv_path: str | Path) -> list[ProfessorSourceRow]:
    path = Path(csv_path)
    if not path.exists():
        return []

    rows: list[ProfessorSourceRow] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                ProfessorSourceRow(
                    id=str(row.get("id", "")).strip(),
                    group=str(row.get("group", "")).strip(),
                    department_ko=str(row.get("department_ko", "")).strip(),
                    department_en=str(row.get("department_en", "")).strip(),
                    page_type=str(row.get("page_type", "department_list")).strip() or "department_list",
                    url_ko=str(row.get("url_ko", "")).strip(),
                    url_en=str(row.get("url_en", "")).strip(),
                    active=str(row.get("active", "Y")).strip() or "Y",
                    note=str(row.get("note", "")).strip(),
                )
            )
    return rows


def save_professor_source_registry(csv_path: str | Path, rows: list[ProfessorSourceRow]) -> None:
    path = Path(csv_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id", "group", "department_ko", "department_en", "page_type",
                "url_ko", "url_en", "active", "note",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def create_professor_source_registry_template(csv_path: str | Path) -> None:
    save_professor_source_registry(csv_path, REGISTRY_TEMPLATE_ROWS)


def registry_clean_professor_name(name: str) -> str:
    value = str(name or "").strip()
    value = re.sub(
        r"(임상부교수|임상조교수|임상교수|부교수|조교수|교수|중계교수|중견교수)",
        "",
        value,
    ).strip()
    value = re.sub(r"\s*(연구|블로그|특임|교수실|이메일|홈페이지|학력|경력|논문)$", "", value).strip()
    value = re.sub(r"\s+", "", value)
    return value


def registry_is_valid_professor_name(name: str) -> bool:
    value = registry_clean_professor_name(name)
    if not re.fullmatch(r"[가-힣]{2,4}", value):
        return False
    if value in BAD_PROF_NAME_TOKENS:
        return False
    return True


def _extract_emails_from_html(html: str) -> list[str]:
    seen: set[str] = set()
    emails: list[str] = []
    for email in re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html or ""):
        if email.lower() not in seen:
            seen.add(email.lower())
            emails.append(email)
    return emails


def _extract_titles_from_html(html: str) -> list[str]:
    titles: list[str] = []
    for title in ["교수", "부교수", "조교수", "임상교수", "임상부교수", "임상조교수", "중계교수", "중견교수"]:
        if title in (html or ""):
            titles.append(title)
    return titles


def _safe_page_goto(page, url: str, timeout_ms: int = 20000) -> None:
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        page.wait_for_load_state("networkidle", timeout=min(4000, timeout_ms))
    except Exception:
        pass
    page.wait_for_timeout(1000)


def parse_department_list_page_from_urls(
    source: ProfessorSourceRow,
    *,
    logger: Callable[[str], None] | None = None,
    timeout_ms: int = 20000,
) -> list[ProfessorResultRow]:
    log = logger or (lambda _msg: None)
    ko_names: list[str] = []
    en_names: list[str] = []
    ko_en_map: dict[str, str] = {}
    titles: list[str] = []
    emails: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        try:
            if source.url_ko:
                _safe_page_goto(page, source.url_ko, timeout_ms)
                ko_html = page.content()
                ko_names = _extract_names_from_rendered_html(ko_html)
                titles = _extract_titles_from_html(ko_html)
                emails = _extract_emails_from_html(ko_html)
                ko_en_map = _extract_korean_english_name_pairs(ko_html)
                log(f"[REGISTRY] 국문 파싱 | {source.department_ko} | 이름후보={len(ko_names)}")

            if source.url_en:
                try:
                    _safe_page_goto(page, source.url_en, timeout_ms)
                    professor_url = _find_professor_staff_url(page, source.url_en)
                    if professor_url and professor_url != source.url_en:
                        _safe_page_goto(page, professor_url, timeout_ms)
                    en_html = page.content()
                    en_names = _extract_english_names_from_rendered_html(en_html)
                    log(f"[REGISTRY] 영문 파싱 | {source.department_ko} | 영문후보={len(en_names)}")
                except Exception as exc:
                    log(f"[REGISTRY-WARN] 영문 페이지 파싱 실패: {source.department_ko} | {exc}")
        finally:
            browser.close()

    matched = _match_korean_to_english_names(ko_names, en_names) if ko_names and en_names else {}
    for ko_name, en_name in ko_en_map.items():
        normalized_en = _normalize_english_professor_name(en_name)
        if normalized_en:
            matched[ko_name] = normalized_en

    rows: list[ProfessorResultRow] = []
    default_title = titles[0] if titles else ""
    default_email = emails[0] if emails else ""
    for name in ko_names:
        clean_name = registry_clean_professor_name(name)
        if not registry_is_valid_professor_name(clean_name):
            continue
        query = _normalize_english_professor_name(matched.get(name, "") or matched.get(clean_name, ""))
        rows.append(
            ProfessorResultRow(
                group=source.group,
                department=source.department_ko,
                name=clean_name,
                query=query,
                title=default_title,
                email=default_email,
                affiliation="Yonsei University College of Medicine",
                source_url=source.url_ko,
                source_url_en=source.url_en,
                match_status="matched" if query else "missing_en",
                review_status="pending",
            )
        )
    return rows


def parse_professor_profile_page_from_urls(
    source: ProfessorSourceRow,
    *,
    logger: Callable[[str], None] | None = None,
    timeout_ms: int = 20000,
) -> list[ProfessorResultRow]:
    rows = parse_department_list_page_from_urls(source, logger=logger, timeout_ms=timeout_ms)
    if rows:
        return rows[:1]
    return []


def dedupe_professor_result_rows(rows: list[ProfessorResultRow]) -> list[ProfessorResultRow]:
    best: dict[tuple[str, str, str], ProfessorResultRow] = {}
    for row in rows:
        clean_name = registry_clean_professor_name(row.name)
        if not registry_is_valid_professor_name(clean_name):
            continue
        row.name = clean_name
        key = (row.group, row.department, clean_name)
        current = best.get(key)
        if current is None:
            best[key] = row
            continue
        current_score = (1 if current.query else 0) + (1 if current.email else 0)
        new_score = (1 if row.query else 0) + (1 if row.email else 0)
        if new_score > current_score:
            best[key] = row
    return sorted(best.values(), key=lambda r: (r.group, r.department, r.name))


def fetch_professors_from_registered_sources(
    registry_csv: str | Path,
    *,
    group_filter: str = "전체",
    logger: Callable[[str], None] | None = None,
) -> list[ProfessorResultRow]:
    log = logger or (lambda _msg: None)
    registry = load_professor_source_registry(registry_csv)
    active_rows = [
        row for row in registry
        if str(row.active).upper() == "Y" and (group_filter == "전체" or row.group == group_filter)
    ]
    if not active_rows:
        raise RuntimeError("활성화된 교수정보 URL 등록 항목이 없습니다.")

    collected: list[ProfessorResultRow] = []
    for source in active_rows:
        log(f"[REGISTRY] 수집 시작 | {source.group} | {source.department_ko} | {source.page_type}")
        try:
            if source.page_type == "professor_profile":
                rows = parse_professor_profile_page_from_urls(source, logger=log)
            else:
                rows = parse_department_list_page_from_urls(source, logger=log)
            log(f"[REGISTRY] 수집 완료 | {source.department_ko} | {len(rows)}명")
            collected.extend(rows)
        except Exception as exc:
            log(f"[REGISTRY-ERROR] 수집 실패 | {source.department_ko} | {exc}")

    deduped = dedupe_professor_result_rows(collected)
    log(f"[REGISTRY] 최종 정제 완료 | {len(deduped)}명")
    return deduped


def export_professor_results_csv(output_csv: str | Path, rows: list[ProfessorResultRow]) -> None:
    path = Path(output_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "group", "department", "name", "query", "title", "email",
                "affiliation", "source_url", "source_url_en", "match_status", "review_status",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
