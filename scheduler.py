import os
import logging
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from drive import listar_arquivos, get_service, download_arquivo
from extractor import extrair_texto
from summarizer import gerar_summary
from ledger import salvar_arquivo, compilar_ledger, compilar_ledger_mensal, determinar_data
from escopos import (
    folder_ids_ativos, construir_arvore_escopo,
    arquivo_pertence_a_escopo, espelhar_arquivo,
)

logger = logging.getLogger(__name__)

BRT = ZoneInfo("America/Sao_Paulo")
BRT_TZ = "America/Sao_Paulo"
ORG_ID = os.getenv("ORG_ID", "followpartners")

_scheduler = None


def pipeline() -> int:
    logger.info("[pipeline] início")

    service = get_service()
    arquivos = listar_arquivos()
    logger.info(f"[pipeline] {len(arquivos)} arquivos criados hoje no Drive")

    salvos = 0
    processados = []
    for arq in arquivos:
        texto = extrair_texto(service, arq)
        summary = gerar_summary(texto, arq.get("name", ""), arq.get("mimeType", "")) if texto else None
        binario = download_arquivo(service, arq)
        key = salvar_arquivo(ORG_ID, arq, binario, texto, summary)
        if key:
            salvos += 1
            processados.append(arq)

    logger.info(f"[pipeline] {salvos}/{len(arquivos)} arquivos salvos no S3")

    hoje = datetime.now(BRT).date()
    secoes = compilar_ledger(ORG_ID, hoje)
    logger.info(f"[pipeline] ledger compilada com {secoes} seções")

    _espelhar_escopos(service, processados)

    return salvos


def _espelhar_escopos(service, arquivos: list[dict]):
    """Espelha os arquivos processados para os escopos ativos e compila
    as ledgers (diária e mensal) de cada escopo afetado."""
    try:
        ativos = folder_ids_ativos()
    except Exception as e:
        logger.warning(f"[escopos] erro ao ler vínculos: {e}")
        return

    if not ativos or not arquivos:
        return

    logger.info(f"[escopos] {len(ativos)} escopos ativos")

    arvores = {}
    for fid in ativos:
        try:
            arvores[fid] = construir_arvore_escopo(service, fid)
        except Exception as e:
            logger.warning(f"[escopos] erro ao construir árvore de {fid}: {e}")

    dias_por_escopo = defaultdict(set)
    for arq in arquivos:
        for fid, arvore in arvores.items():
            if arquivo_pertence_a_escopo(arq, arvore):
                copiados = espelhar_arquivo(ORG_ID, fid, arq)
                if copiados:
                    dia, _ = determinar_data(arq)
                    dias_por_escopo[fid].add(dia)

    for fid, dias in dias_por_escopo.items():
        for dia in sorted(dias):
            try:
                compilar_ledger(fid, dia, incluir_diario=False)
            except Exception as e:
                logger.warning(f"[escopos] erro ao compilar ledger {fid}/{dia}: {e}")
        for ano, mes in {(d.year, d.month) for d in dias}:
            try:
                compilar_ledger_mensal(fid, ano, mes)
            except Exception as e:
                logger.warning(f"[escopos] erro ao compilar mensal {fid}/{ano}-{mes:02d}: {e}")

    if dias_por_escopo:
        logger.info(f"[escopos] espelhamento concluído para {len(dias_por_escopo)} escopos")


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
