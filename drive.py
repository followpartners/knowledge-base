import os
import io
import json
import base64
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
BRT = ZoneInfo("America/Sao_Paulo")

_service = None


def _get_credentials():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    info = json.loads(base64.b64decode(raw))
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    subject = os.getenv("GOOGLE_IMPERSONATE_EMAIL", "")
    if subject:
        creds = creds.with_subject(subject)
    return creds


def get_service():
    global _service
    if _service is None:
        _service = build("drive", "v3", credentials=_get_credentials())
    return _service


def listar_arquivos() -> list[dict]:
    service = get_service()
    hoje_brt = datetime.now(BRT).date()
    inicio_utc = datetime(
        hoje_brt.year, hoje_brt.month, hoje_brt.day, 0, 0, 0, tzinfo=BRT
    ).astimezone(timezone.utc)
    fim_utc = datetime(
        hoje_brt.year, hoje_brt.month, hoje_brt.day, 23, 59, 59, tzinfo=BRT
    ).astimezone(timezone.utc)

    arquivos = []
    page_token = None
    fields = (
        "nextPageToken, "
        "files(id, name, mimeType, createdTime, modifiedTime, webViewLink)"
    )
    ini = inicio_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    fim = fim_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    janela = (
        f"(createdTime >= '{ini}' and createdTime < '{fim}')"
        f" or (modifiedTime >= '{ini}' and modifiedTime < '{fim}')"
    )
    query = (
        "trashed = false"
        " and mimeType != 'application/vnd.google-apps.folder'"
        f" and ({janela})"
    )

    while True:
        resp = (
            service.files()
            .list(
                pageSize=1000,
                fields=fields,
                pageToken=page_token,
                q=query,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        arquivos.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    logger.info(f"[drive] {len(arquivos)} arquivos criados/modificados hoje")
    return arquivos


def download_arquivo(service, arquivo: dict) -> bytes | None:
    file_id = arquivo["id"]
    mime = arquivo.get("mimeType", "")

    try:
        if mime.startswith("application/vnd.google-apps."):
            export_map = {
                "application/vnd.google-apps.document": "application/pdf",
                "application/vnd.google-apps.spreadsheet": "text/csv",
                "application/vnd.google-apps.presentation": "application/pdf",
            }
            export_mime = export_map.get(mime)
            if not export_mime:
                logger.warning(f"[drive] tipo Google não exportável: {mime} ({arquivo.get('name')})")
                return None
            data = service.files().export(fileId=file_id, mimeType=export_mime).execute()
            return data if isinstance(data, bytes) else data.encode("utf-8")

        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buffer.getvalue()
    except Exception as e:
        logger.warning(f"[drive] erro ao baixar {arquivo.get('name')}: {e}")
        return None
