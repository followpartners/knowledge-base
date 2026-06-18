#!/usr/bin/env python3
import sys
import os
import argparse
import logging
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from drive import get_service, download_arquivo
from extractor import extrair_texto
from ledger import salvar_arquivo, ler_diario, compilar_ledger, _get_s3, BUCKET, _prefixo_dia

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

BRT = ZoneInfo("America/Sao_Paulo")


def listar_todos_arquivos(service, desde: date | None = None, ate: date | None = None) -> list[dict]:
    arquivos = []
    page_token = None
    fields = (
        "nextPageToken, "
        "files(id, name, mimeType, createdTime, modifiedTime, webViewLink)"
    )
    query = (
        "trashed = false"
        " and mimeType != 'application/vnd.google-apps.folder'"
    )

    if desde:
        inicio_utc = datetime(desde.year, desde.month, desde.day, 0, 0, 0, tzinfo=BRT).astimezone(timezone.utc)
        query += f" and createdTime >= '{inicio_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}'"
    if ate:
        fim_utc = datetime(ate.year, ate.month, ate.day, 23, 59, 59, tzinfo=BRT).astimezone(timezone.utc)
        query += f" and createdTime < '{fim_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}'"

    while True:
        resp = (
            service.files()
            .list(
                pageSize=1000,
                fields=fields,
                pageToken=page_token,
                q=query,
            )
            .execute()
        )
        arquivos.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return arquivos


def arquivo_existe_s3(org_id: str, arquivo: dict) -> bool:
    created = arquivo.get("createdTime", "")
    if not created:
        return False

    dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
    dia = dt.astimezone(BRT).date()
    nome = arquivo.get("name", arquivo["id"])
    key = f"{_prefixo_dia(org_id, dia)}/arquivos/{nome}"

    try:
        _get_s3().head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="Onboarding — importa histórico do Drive para o S3")
    parser.add_argument("--org_id", required=True, help="ID da organização")
    parser.add_argument("--drive_email", required=True, help="Email para impersonar via domain-wide delegation")
    parser.add_argument("--desde", type=lambda s: date.fromisoformat(s), default=None, help="Data inicial (YYYY-MM-DD)")
    parser.add_argument("--ate", type=lambda s: date.fromisoformat(s), default=None, help="Data final (YYYY-MM-DD, default hoje)")
    args = parser.parse_args()

    org_id = args.org_id
    os.environ["GOOGLE_IMPERSONATE_EMAIL"] = args.drive_email

    logger.info(f"[onboarding] org={org_id} email={args.drive_email}")
    if args.desde or args.ate:
        logger.info(f"[onboarding] período: {args.desde or 'início'} → {args.ate or 'hoje'}")

    service = get_service()
    arquivos = listar_todos_arquivos(service, desde=args.desde, ate=args.ate)
    logger.info(f"[onboarding] {len(arquivos)} arquivos encontrados no Drive")

    processados = 0
    pulados = 0
    erros = 0
    datas = defaultdict(list)

    for i, arq in enumerate(arquivos, 1):
        nome = arq.get("name", arq["id"])

        try:
            if arquivo_existe_s3(org_id, arq):
                logger.info(f"[onboarding] ({i}/{len(arquivos)}) pulando {nome} — já existe")
                pulados += 1
                continue

            binario = download_arquivo(service, arq)
            if not binario:
                logger.warning(f"[onboarding] ({i}/{len(arquivos)}) sem binário: {nome}")
                erros += 1
                time.sleep(0.5)
                continue

            texto = extrair_texto(service, arq)
            key = salvar_arquivo(org_id, arq, binario, texto)

            if key:
                created = arq.get("createdTime", "")
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                dia = dt.astimezone(BRT).date()
                datas[dia].append(nome)
                logger.info(f"[onboarding] ({i}/{len(arquivos)}) {nome} → {dia}")
                processados += 1
            else:
                erros += 1

        except Exception as e:
            logger.error(f"[onboarding] ({i}/{len(arquivos)}) erro em {nome}: {e}")
            erros += 1

        time.sleep(0.5)

    logger.info("[onboarding] arquivos concluídos — compilando ledgers")

    ledgers_compiladas = 0
    diarios_encontrados = 0

    for dia in sorted(datas.keys()):
        try:
            diario = ler_diario(dia)
            if diario:
                diarios_encontrados += 1

            secoes = compilar_ledger(org_id, dia)
            if secoes > 0:
                ledgers_compiladas += 1
                logger.info(f"[onboarding] ledger {dia} compilada com {secoes} seções")
        except Exception as e:
            logger.error(f"[onboarding] erro ao compilar ledger {dia}: {e}")

    logger.info("")
    logger.info("=" * 50)
    logger.info("[onboarding] RESUMO")
    logger.info(f"  Arquivos processados:  {processados}")
    logger.info(f"  Arquivos pulados:      {pulados}")
    logger.info(f"  Erros:                 {erros}")
    logger.info(f"  Datas com ledger:      {ledgers_compiladas}")
    logger.info(f"  Datas com diário:      {diarios_encontrados}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
