"""Escopos — vínculos canal↔pasta e espelhamento de arquivos por escopo.

Um escopo é uma pasta do Drive (folder_id) vinculada a um ou mais canais
do Slack. Arquivos processados pela base Follow que pertencem à árvore
de um escopo são espelhados (cópia S3→S3) para orgs/{folder_id}/.
"""
import json
import logging

from ledger import (
    _get_s3, BUCKET, TASK_MANAGER_BUCKET,
    _prefixo_dia, _sanitizar_nome, determinar_data,
)

logger = logging.getLogger(__name__)

ESCOPOS_KEY = "task-manager/escopos/escopos.json"

# Artefatos gerados por ledger.salvar_arquivo, na ordem: binário, meta, texto, summary
SUFIXOS_ARTEFATOS = ["", ".meta.json", ".txt", ".summary.md"]


def ler_vinculos() -> list[dict]:
    """Lê o escopos.json do bucket do Task Manager.
    Retorna a lista de vínculos, ou [] se o arquivo não existe."""
    try:
        resp = _get_s3().get_object(Bucket=TASK_MANAGER_BUCKET, Key=ESCOPOS_KEY)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        return data.get("vinculos", [])
    except Exception:
        return []


def folder_ids_ativos() -> set[str]:
    """Conjunto de folder_ids únicos com ao menos um vínculo."""
    return {v["folder_id"] for v in ler_vinculos() if v.get("folder_id")}


def construir_arvore_escopo(service, folder_id_raiz: str) -> set[str]:
    """Expande recursivamente as subpastas de um escopo (BFS).
    Retorna o conjunto de todos os folder_ids do escopo (raiz incluída)."""
    arvore = {folder_id_raiz}
    fila = [folder_id_raiz]

    while fila:
        pai = fila.pop(0)
        page_token = None
        while True:
            resp = (
                service.files()
                .list(
                    q=(
                        f"'{pai}' in parents"
                        " and mimeType = 'application/vnd.google-apps.folder'"
                        " and trashed = false"
                    ),
                    fields="nextPageToken, files(id)",
                    pageSize=1000,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            for f in resp.get("files", []):
                fid = f["id"]
                if fid not in arvore:
                    arvore.add(fid)
                    fila.append(fid)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    logger.info(f"[escopos] árvore de {folder_id_raiz}: {len(arvore)} pastas")
    return arvore


def arquivo_pertence_a_escopo(arquivo: dict, arvore_folder_ids: set[str]) -> bool:
    """True se algum parent do arquivo está na árvore do escopo."""
    return any(p in arvore_folder_ids for p in arquivo.get("parents", []))


def espelhar_arquivo(org_origem: str, org_destino: str, arquivo: dict) -> int:
    """Copia (S3→S3, server-side) os artefatos de um arquivo já processado
    na base org_origem para o escopo org_destino, no mesmo dia YYYY/MM/DD.

    Retorna o número de artefatos copiados (0 se nada foi copiado)."""
    dia, _ = determinar_data(arquivo)
    nome = _sanitizar_nome(arquivo.get("name", arquivo["id"]))
    base_origem = f"{_prefixo_dia(org_origem, dia)}/arquivos/{nome}"
    base_destino = f"{_prefixo_dia(org_destino, dia)}/arquivos/{nome}"

    s3 = _get_s3()
    copiados = 0
    for sufixo in SUFIXOS_ARTEFATOS:
        try:
            s3.copy_object(
                Bucket=BUCKET,
                Key=f"{base_destino}{sufixo}",
                CopySource={"Bucket": BUCKET, "Key": f"{base_origem}{sufixo}"},
            )
            copiados += 1
        except Exception:
            # artefato não existe na origem (ex.: sem .txt) — segue
            pass

    if copiados:
        logger.info(f"[escopos] espelhado {nome} → orgs/{org_destino} ({copiados} artefatos)")
    else:
        logger.warning(f"[escopos] nenhum artefato encontrado para espelhar: {base_origem}")
    return copiados
