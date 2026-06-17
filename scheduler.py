import os
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from drive import listar_arquivos, get_service
from extractor import extrair_texto
from ledger import gravar

logger = logging.getLogger(__name__)

BRT_TZ = "America/Sao_Paulo"
ORG_ID = os.getenv("ORG_ID", "followpartners")

_scheduler = None


def pipeline() -> int:
    logger.info("[pipeline] início")

    service = get_service()
    arquivos = listar_arquivos()
    logger.info(f"[pipeline] {len(arquivos)} arquivos encontrados no Drive")

    entradas = []
    extraidos = 0
    for arq in arquivos:
        texto = extrair_texto(service, arq)
        if texto is not None:
            extraidos += 1

        entradas.append({
            "file_id": arq["id"],
            "nome": arq["name"],
            "link": arq.get("webViewLink", ""),
            "mimeType": arq.get("mimeType", ""),
            "modifiedTime": arq.get("modifiedTime", ""),
            "texto": texto,
        })

    logger.info(f"[pipeline] {extraidos}/{len(arquivos)} extraídos com sucesso")

    novos = gravar(ORG_ID, entradas)
    logger.info(f"[pipeline] {novos} entradas novas gravadas no S3")

    return novos


def iniciar():
    global _scheduler
    if _scheduler is not None:
        return

    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        pipeline,
        trigger=CronTrigger(hour=23, minute=59, timezone=BRT_TZ),
        id="sync_diario",
        name="Sync diário Drive → Ledger",
    )
    _scheduler.start()
    logger.info("[scheduler] iniciado — cron 23:59 BRT")
