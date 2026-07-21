#!/usr/bin/env python3
"""Diagnóstico da query do Drive — valida a captura por createdTime OU modifiedTime.

Rodar no console do Railway (onde a service account e a lib do Google
estão disponíveis):

    python scripts/diagnostico_drive.py
"""
import sys
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from drive import get_service

BRT = ZoneInfo("America/Sao_Paulo")
agora_brt = datetime.now(BRT)
inicio_dia_brt = agora_brt.replace(hour=0, minute=0, second=0, microsecond=0)
inicio_utc = inicio_dia_brt.astimezone(timezone.utc)
fim_utc = (inicio_dia_brt + timedelta(days=1)).astimezone(timezone.utc)

inicio_str = inicio_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
fim_str = fim_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"Agora BRT: {agora_brt}")
print(f"Janela UTC: {inicio_str} ate {fim_str}")
print()

# Teste 1: sem filtro, ordena por modifiedTime desc
# (mantido SEM os flags de Shared Drive — baseline "só My Drive")
service = get_service()
print("TESTE 1 - 10 arquivos mais recentemente modificados:")
resp = service.files().list(
    q="trashed = false and mimeType != 'application/vnd.google-apps.folder'",
    fields="files(id,name,createdTime,modifiedTime,mimeType)",
    orderBy="modifiedTime desc",
    pageSize=10,
).execute()
for f in resp.get("files", []):
    print(f"  mod={f['modifiedTime']} | created={f['createdTime']} | {f['name']}")

print()
print("TESTE 2 - filtro so por modifiedTime hoje:")
q2 = f"trashed = false and mimeType != 'application/vnd.google-apps.folder' and modifiedTime >= '{inicio_str}' and modifiedTime < '{fim_str}'"
print(f"Query: {q2}")
resp = service.files().list(
    q=q2,
    fields="files(id,name,modifiedTime)",
    pageSize=20,
    supportsAllDrives=True,
    includeItemsFromAllDrives=True,
).execute()
files = resp.get("files", [])
print(f"Retornou {len(files)} arquivos:")
for f in files:
    print(f"  mod={f['modifiedTime']} | {f['name']}")

print()
print("TESTE 3 - query composta OR (como esta no drive.py):")
q3 = (f"trashed = false and mimeType != 'application/vnd.google-apps.folder' and "
      f"((createdTime >= '{inicio_str}' and createdTime < '{fim_str}') or "
      f"(modifiedTime >= '{inicio_str}' and modifiedTime < '{fim_str}'))")
print(f"Query: {q3}")
resp = service.files().list(
    q=q3,
    fields="files(id,name,createdTime,modifiedTime)",
    pageSize=20,
    supportsAllDrives=True,
    includeItemsFromAllDrives=True,
).execute()
files = resp.get("files", [])
print(f"Retornou {len(files)} arquivos:")
for f in files:
    print(f"  mod={f['modifiedTime']} | created={f['createdTime']} | {f['name']}")
