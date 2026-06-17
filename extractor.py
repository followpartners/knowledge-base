import io
import logging

from googleapiclient.http import MediaIoBaseDownload
from PyPDF2 import PdfReader
from docx import Document

logger = logging.getLogger(__name__)

GOOGLE_DOCS = "application/vnd.google-apps.document"
GOOGLE_SHEETS = "application/vnd.google-apps.spreadsheet"
GOOGLE_SLIDES = "application/vnd.google-apps.presentation"
PDF = "application/pdf"
DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def extrair_texto(service, arquivo: dict) -> str | None:
    file_id = arquivo["id"]
    mime = arquivo.get("mimeType", "")

    try:
        if mime == GOOGLE_DOCS:
            return _export(service, file_id, "text/plain")
        if mime == GOOGLE_SHEETS:
            return _export(service, file_id, "text/csv")
        if mime == GOOGLE_SLIDES:
            return _export(service, file_id, "text/plain")
        if mime == PDF:
            return _extrair_pdf(service, file_id)
        if mime == DOCX:
            return _extrair_docx(service, file_id)

        logger.warning(f"[extractor] tipo não suportado: {mime} ({arquivo.get('name')})")
        return None
    except Exception as e:
        logger.warning(f"[extractor] erro ao extrair {arquivo.get('name')}: {e}")
        return None


def _export(service, file_id: str, mime_out: str) -> str:
    data = service.files().export(fileId=file_id, mimeType=mime_out).execute()
    return data.decode("utf-8") if isinstance(data, bytes) else data


def _download(service, file_id: str) -> bytes:
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def _extrair_pdf(service, file_id: str) -> str | None:
    content = _download(service, file_id)
    reader = PdfReader(io.BytesIO(content))
    texto = "\n".join(page.extract_text() or "" for page in reader.pages)
    return texto or None


def _extrair_docx(service, file_id: str) -> str | None:
    content = _download(service, file_id)
    doc = Document(io.BytesIO(content))
    texto = "\n".join(p.text for p in doc.paragraphs)
    return texto or None
