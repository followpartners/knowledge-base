import os
import logging
from datetime import date

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from drive import listar_arquivos, get_service, download_arquivo
from extractor import extrair_texto
from ledger import salvar_arquivo, compilar_ledger

logger = logging.getLogger(__name__)

BRT_TZ = "America/Sao_Paulo"
ORG_ID = os.getenv("ORG_ID", "followpartners")

_scheduler = None


def pipeline() -> int:
    logger.info("[pipeline] início")

    service = get_service()
    arquivos = listar_arquivos()
    logger.info(f"[pipeline] {len(arquivos)} arquivos criados hoje no Drive")

    salvos = 0
    for arq in arquivos:
        texto = extrair_texto(service, arq)
        binario = download_arquivo(service, arq)
        key = salvar_arquivo(ORG_ID, arq, binario, texto)
        if key:
            salvos += 1

    logger.info(f"[pipeline] {salvos}/{len(arquivos)} arquivos salvos no S3")

    secoes = compilar_ledger(ORG_ID, date.today())
    logger.info(f"[pipeline] ledger compilada com {secoes} seções")

    return salvos


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
