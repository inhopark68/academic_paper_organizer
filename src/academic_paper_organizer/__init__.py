from .core import (
    FIELD_CODES,
    PaperRow,
    PaperIndex,
    PaperOrganizer,
    PDFCreatedHandler,
    search_pubmed_by_title,
    scan_existing_pdfs,
    repair_and_reindex,
    export_professor_achievements_csv,
    export_latest_yonsei_professors_csv,
)

try:
    from .core import scan_existing_pdfs_fast
except Exception:
    scan_existing_pdfs_fast = None

from .gui import (
    AppConfig,
    ProfessorManagerApp,
)

__all__ = [
    "FIELD_CODES",
    "PaperRow",
    "PaperIndex",
    "PaperOrganizer",
    "PDFCreatedHandler",
    "search_pubmed_by_title",
    "scan_existing_pdfs",
    "scan_existing_pdfs_fast",
    "repair_and_reindex",
    "export_professor_achievements_csv",
    "export_latest_yonsei_professors_csv",
    "AppConfig",
    "ProfessorManagerApp",
]

