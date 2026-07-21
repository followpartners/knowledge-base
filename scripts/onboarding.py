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
from summarizer import gerar_summary
from ledger import (
    salvar_arquivo, ler_diario, compilar_ledger, determinar_data,
    _get_s3, BUCKET, _prefixo_dia, _sanitizar_nome, _ler_s3,
)

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


def _s3_key_existe(key: str) -> bool:
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

    novos = 0
    backfilled = 0
    pulados = 0
    erros = 0
    datas = defaultdict(list)

    for i, arq in enumerate(arquivos, 1):
        raw_nome = arq.get("name", arq["id"])
        nome = _sanitizar_nome(raw_nome)
        mime = arq.get("mimeType", "")

        try:
            dia, evento = determinar_data(arq)
            prefixo = _prefixo_dia(org_id, dia)
            key_meta = f"{prefixo}/arquivos/{nome}.meta.json"
            key_summary = f"{prefixo}/arquivos/{nome}.summary.md"

            meta_existe = _s3_key_existe(key_meta)
            summary_existe = _s3_key_existe(key_summary)

            if meta_existe and summary_existe:
                logger.info(f"[onboarding] ({i}/{len(arquivos)}) pulando {raw_nome} — completo ({evento})")
                pulados += 1
                continue

            if meta_existe and not summary_existe:
                key_txt = f"{prefixo}/arquivos/{nome}.txt"
                texto = _ler_s3(key_txt)
                if texto:
                    summary = gerar_summary(texto, raw_nome, mime)
                    if summary:
                        _get_s3().put_object(
                            Bucket=BUCKET,
                            Key=key_summary,
                            Body=summary.encode("utf-8"),
                            ContentType="text/markdown; charset=utf-8",
                        )
                        logger.info(f"[onboarding] ({i}/{len(arquivos)}) backfill summary: {raw_nome}")
                        backfilled += 1
                    else:
                        logger.info(f"[onboarding] ({i}/{len(arquivos)}) sem texto para summary: {raw_nome}")
                else:
                    logger.info(f"[onboarding] ({i}/{len(arquivos)}) sem .txt para backfill: {raw_nome}")
                datas[dia].append(raw_nome)
                time.sleep(0.5)
                continue

            binario = download_arquivo(service, arq)
            if not binario:
                logger.warning(f"[onboarding] ({i}/{len(arquivos)}) sem binário: {raw_nome}")
                erros += 1
                time.sleep(0.5)
                continue

            texto = extrair_texto(service, arq)
            summary = gerar_summary(texto, raw_nome, mime) if texto else None
            key = salvar_arquivo(org_id, arq, binario, texto, summary)

            if key:
                datas[dia].append(raw_nome)
                logger.info(f"[onboarding] ({i}/{len(arquivos)}) novo: {raw_nome} → {dia}")
                novos += 1
            else:
                erros += 1

        except Exception as e:
            logger.error(f"[onboarding] ({i}/{len(arquivos)}) erro em {raw_nome}: {e}")
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
    logger.info(f"  Arquivos novos:        {novos}")
    logger.info(f"  Summaries backfilled:  {backfilled}")
    logger.info(f"  Arquivos pulados:      {pulados}")
    logger.info(f"  Erros:                 {erros}")
    logger.info(f"  Datas com ledger:      {ledgers_compiladas}")
    logger.info(f"  Datas com diário:      {diarios_encontrados}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
