import os
import re
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

logger = logging.getLogger(__name__)

BUCKET = os.getenv("AWS_BUCKET_NAME", "")
REGION = os.getenv("AWS_REGION", "sa-east-1")
BRT = ZoneInfo("America/Sao_Paulo")

_s3 = None


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client(
            "s3",
            region_name=REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
    return _s3


def _s3_key(org_id: str, data: datetime) -> str:
    return f"orgs/{org_id}/ledger/{data.strftime('%Y-%m-%d')}.md"


def _ler_existente(key: str) -> str:
    try:
        resp = _get_s3().get_object(Bucket=BUCKET, Key=key)
        return resp["Body"].read().decode("utf-8")
    except Exception:
        return ""


def _extrair_registros(conteudo: str) -> dict[str, str]:
    registros = {}
    for match in re.finditer(r"<!-- file_id:(\S+) modified:(\S+) -->", conteudo):
        registros[match.group(1)] = match.group(2)
    return registros


def _remover_entrada(conteudo: str, file_id: str) -> str:
    return re.sub(
        rf"## [^\n]+\n<!-- file_id:{re.escape(file_id)} [^\n]+ -->\n.*?(?=\n## |\Z)",
        "",
        conteudo,
        flags=re.DOTALL,
    ).strip()


def gravar(org_id: str, entradas: list[dict]) -> int:
    agora = datetime.now(BRT)
    key = _s3_key(org_id, agora)

    existente = _ler_existente(key)
    registros = _extrair_registros(existente)

    novas = []
    for e in entradas:
        fid = e["file_id"]
        mod = e["modifiedTime"]
        if fid in registros:
            if mod <= registros[fid]:
                continue
            else:
                existente = _remover_entrada(existente, fid)
        novas.append(e)

    if not novas:
        logger.info("[ledger] nenhuma entrada nova para gravar")
        return 0

    blocos = []
    for e in novas:
        mod_dt = datetime.fromisoformat(e["modifiedTime"].replace("Z", "+00:00"))
        hora = mod_dt.astimezone(BRT).strftime("%H:%M")
        conteudo = (e.get("texto") or "")[:500] or "sem conteúdo extraído"

        bloco = (
            f"## {hora} — Google Drive\n"
            f"<!-- file_id:{e['file_id']} modified:{e['modifiedTime']} -->\n"
            f"[ARQUIVO] {e['nome']} — {e['link']}\n"
            f"[TIPO] {e['mimeType']}\n"
            f"[CONTEÚDO] {conteudo}"
        )
        blocos.append(bloco)

    if existente:
        md_final = existente.rstrip("\n") + "\n\n" + "\n\n".join(blocos) + "\n"
    else:
        md_final = "\n\n".join(blocos) + "\n"

    _get_s3().put_object(
        Bucket=BUCKET,
        Key=key,
        Body=md_final.encode("utf-8"),
        ContentType="text/markdown",
    )

    logger.info(f"[ledger] {len(novas)} entradas gravadas em {key}")
    return len(novas)
