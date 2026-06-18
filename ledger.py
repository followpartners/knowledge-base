import os
import json
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

import boto3

logger = logging.getLogger(__name__)

BUCKET = os.getenv("AWS_BUCKET_NAME", "")
TASK_MANAGER_BUCKET = os.getenv("TASK_MANAGER_BUCKET", "follow-task-manager")
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


def _prefixo_dia(org_id: str, dia: date) -> str:
    return f"orgs/{org_id}/ledger/{dia.strftime('%Y/%m/%d')}"


def _sanitizar_nome(nome: str) -> str:
    return nome.replace("/", "-").replace("\\", "-")


def salvar_arquivo(org_id: str, arquivo: dict, conteudo: bytes, texto: str | None = None) -> str | None:
    if not conteudo:
        return None

    created = arquivo.get("createdTime", "")
    if created:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        dia = dt.astimezone(BRT).date()
    else:
        dia = date.today()

    nome = _sanitizar_nome(arquivo.get("name", arquivo["id"]))
    prefixo = _prefixo_dia(org_id, dia)
    key = f"{prefixo}/arquivos/{nome}"

    try:
        s3 = _get_s3()
        s3.put_object(Bucket=BUCKET, Key=key, Body=conteudo)

        meta = {
            "nome": arquivo.get("name"),
            "file_id": arquivo.get("id"),
            "mime_type": arquivo.get("mimeType"),
            "created_at": arquivo.get("createdTime"),
            "modified_at": arquivo.get("modifiedTime"),
            "link": arquivo.get("webViewLink", ""),
        }
        key_meta = f"{prefixo}/arquivos/{nome}.meta.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key_meta,
            Body=json.dumps(meta, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

        if texto is not None:
            key_txt = f"{prefixo}/arquivos/{nome}.txt"
            s3.put_object(
                Bucket=BUCKET,
                Key=key_txt,
                Body=texto.encode("utf-8"),
                ContentType="text/plain",
            )

        logger.info(f"[ledger] arquivo salvo: {key}")
        return key
    except Exception as e:
        logger.warning(f"[ledger] erro ao salvar {key}: {e}")
        return None


def ler_diario(dia: date) -> str | None:
    key = f"task-manager/daily/{dia.strftime('%Y-%m-%d')}.md"
    try:
        resp = _get_s3().get_object(Bucket=TASK_MANAGER_BUCKET, Key=key)
        return resp["Body"].read().decode("utf-8")
    except Exception:
        return None


def _listar_keys(prefixo: str) -> list[str]:
    s3 = _get_s3()
    keys = []
    token = None

    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": prefixo}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        token = resp.get("NextContinuationToken")
        if not token:
            break

    return keys


def _ler_s3(key: str) -> str:
    try:
        resp = _get_s3().get_object(Bucket=BUCKET, Key=key)
        return resp["Body"].read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def compilar_ledger(org_id: str, dia: date) -> int:
    prefixo = _prefixo_dia(org_id, dia)
    secoes = []

    arquivos_keys = [
        k for k in _listar_keys(f"{prefixo}/arquivos/")
        if not k.endswith(".meta.json") and not k.endswith(".txt")
    ]
    if arquivos_keys:
        linhas = ["## Arquivos\n"]
        for key in arquivos_keys:
            nome = key.rsplit("/", 1)[-1]

            hora = "—"
            link = ""
            mime_type = ""
            meta_raw = _ler_s3(f"{key}.meta.json")
            if meta_raw:
                try:
                    meta = json.loads(meta_raw)
                    link = meta.get("link", "")
                    mime_type = meta.get("mime_type", "")
                    created = meta.get("created_at", "")
                    if created:
                        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                        hora = dt.astimezone(BRT).strftime("%H:%M")
                except (json.JSONDecodeError, ValueError):
                    pass

            txt_conteudo = _ler_s3(f"{key}.txt")
            if txt_conteudo:
                preview = txt_conteudo[:500]
            else:
                conteudo = _ler_s3(key)
                preview = conteudo[:500] if conteudo else "sem conteúdo extraído"

            arquivo_line = f"[ARQUIVO] {nome} — {link}" if link else f"[ARQUIVO] {nome}"
            tipo_line = f"[TIPO] {mime_type}\n" if mime_type else ""

            linhas.append(
                f"### {hora} — Google Drive\n"
                f"{arquivo_line}\n"
                f"{tipo_line}"
                f"[CONTEÚDO] {preview}\n"
            )
        secoes.append("\n".join(linhas))

    analises_keys = _listar_keys(f"{prefixo}/analises/")
    if analises_keys:
        linhas = ["## Análises\n"]
        for key in analises_keys:
            nome = key.rsplit("/", 1)[-1]
            conteudo = _ler_s3(key)
            linhas.append(
                f"### — CFO Agent\n"
                f"[ARQUIVO] {nome}\n"
                f"[ANÁLISE] {conteudo}\n"
            )
        secoes.append("\n".join(linhas))

    diario = ler_diario(dia)
    if diario:
        secoes.append(f"## Diário\n\n{diario.strip()}\n")

    if not secoes:
        logger.info(f"[ledger] nenhum conteúdo para {dia}")
        return 0

    titulo = f"# Follow Partners · {dia.strftime('%d/%m/%Y')}\n\n"
    md = titulo + "\n".join(secoes)

    key_ledger = f"{prefixo}/ledger_{dia.strftime('%Y-%m-%d')}.md"
    _get_s3().put_object(
        Bucket=BUCKET,
        Key=key_ledger,
        Body=md.encode("utf-8"),
        ContentType="text/markdown",
    )

    logger.info(f"[ledger] compilado {key_ledger} com {len(secoes)} seções")
    return len(secoes)
