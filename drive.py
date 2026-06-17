import os
import json
import base64
import logging

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

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
    arquivos = []
    page_token = None
    fields = (
        "nextPageToken, "
        "files(id, name, mimeType, createdTime, modifiedTime, webViewLink)"
    )

    while True:
        resp = (
            service.files()
            .list(
                pageSize=1000,
                fields=fields,
                pageToken=page_token,
                q="trashed = false and mimeType != 'application/vnd.google-apps.folder'",
            )
            .execute()
        )
        arquivos.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    logger.info(f"[drive] {len(arquivos)} arquivos encontrados")
    return arquivos
