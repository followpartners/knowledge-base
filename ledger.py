import os
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timezone
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


def _parse_dia(raw: str) -> date | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(BRT).date()
    except ValueError:
        return None


def determinar_data(arquivo: dict) -> tuple[date, str]:
    """Decide sob qual data (YYYY/MM/DD) o arquivo é gravado e se é
    evento de criação ou modificação.

    - modificado hoje e criado em outro dia → (hoje, "modificado")
    - caso contrário → (data de criação, "criado")
    """
    hoje = datetime.now(BRT).date()
    created_dia = _parse_dia(arquivo.get("createdTime", ""))
    modified_dia = _parse_dia(arquivo.get("modifiedTime", ""))

    if modified_dia == hoje and created_dia != hoje:
        return (hoje, "modificado")
    if created_dia is not None:
        return (created_dia, "criado")
    return (hoje, "criado")


def salvar_arquivo(org_id: str, arquivo: dict, conteudo: bytes, texto: str | None = None, summary: str | None = None) -> str | None:
    if not conteudo:
        return None

    dia, evento = determinar_data(arquivo)

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
            "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "evento": evento,
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

        if summary is not None:
            key_summary = f"{prefixo}/arquivos/{nome}.summary.md"
            s3.put_object(
                Bucket=BUCKET,
                Key=key_summary,
                Body=summary.encode("utf-8"),
                ContentType="text/markdown; charset=utf-8",
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


def _extrair_entradas_diario(diario_md: str) -> list[str]:
    """Extrai as linhas [HH:MM] [autor] ... da seção ## Registro
    do diário do Task Manager, ignorando cabeçalhos e ## Fechamento."""
    entradas = []
    em_registro = False
    for linha in diario_md.split("\n"):
        stripped = linha.strip()
        if stripped.startswith("## Registro"):
            em_registro = True
            continue
        if stripped.startswith("## ") and em_registro:
            em_registro = False
            continue
        if em_registro and stripped.startswith("["):
            entradas.append(stripped)
    return entradas


MESES_PT = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]


def _nome_org(org_id: str) -> str:
    return "Follow Partners" if org_id == "followpartners" else org_id


def compilar_ledger(org_id: str, dia: date, incluir_diario: bool = True) -> int:
    prefixo = _prefixo_dia(org_id, dia)
    secoes = []

    arquivos_keys = [
        k for k in _listar_keys(f"{prefixo}/arquivos/")
        if not k.endswith((".meta.json", ".txt", ".summary.md"))
    ]
    if arquivos_keys:
        linhas = ["## Arquivos\n"]
        for key in arquivos_keys:
            nome = key.rsplit("/", 1)[-1]

            hora = "—"
            link = ""
            mime_type = ""
            evento = "criado"
            meta_raw = _ler_s3(f"{key}.meta.json")
            if meta_raw:
                try:
                    meta = json.loads(meta_raw)
                    link = meta.get("link", "")
                    mime_type = meta.get("mime_type", "")
                    evento = meta.get("evento", "criado")
                    ts = meta.get("modified_at", "") if evento == "modificado" else meta.get("created_at", "")
                    if ts:
                        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        hora = dt.astimezone(BRT).strftime("%H:%M")
                except (json.JSONDecodeError, ValueError):
                    pass

            summary = _ler_s3(f"{key}.summary.md")

            rotulo = "ARQUIVO MODIFICADO" if evento == "modificado" else "ARQUIVO"
            arquivo_line = f"[{rotulo}] {nome} — {link}" if link else f"[{rotulo}] {nome}"
            tipo_line = f"[TIPO] {mime_type}\n" if mime_type else ""
            summary_line = f"[SUMMARY] {summary}\n" if summary else ""

            linhas.append(
                f"### {hora} — Google Drive\n"
                f"{arquivo_line}\n"
                f"{tipo_line}"
                f"{summary_line}"
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

    if incluir_diario:
        diario = ler_diario(dia)
        if diario:
            entradas = _extrair_entradas_diario(diario)
            if entradas:
                secoes.append("## Diário\n\n" + "\n".join(entradas) + "\n")

    if not secoes:
        logger.info(f"[ledger] nenhum conteúdo para {dia}")
        return 0

    titulo = f"# {_nome_org(org_id)} · {dia.strftime('%d/%m/%Y')}\n\n"
    md = titulo + "\n".join(secoes)

    key_ledger = f"{prefixo}/ledger_{dia.strftime('%Y-%m-%d')}.md"
    _get_s3().put_object(
        Bucket=BUCKET,
        Key=key_ledger,
        Body=md.encode("utf-8"),
        ContentType="text/markdown; charset=utf-8",
    )

    logger.info(f"[ledger] compilado {key_ledger} com {len(secoes)} seções")
    return len(secoes)


def compilar_ledger_mensal(org_id: str, ano: int, mes: int) -> int:
    """Compila a ledger mensal de uma org em orgs/{org_id}/monthly/YYYY-MM.md.

    Concatenação determinística (sem LLM) dos summaries dos arquivos do
    mês, agrupados por dia em ordem cronológica. Regenerada do zero a
    cada chamada — lê direto os artefatos (.meta.json/.summary.md) de
    cada arquivo sob orgs/{org_id}/ledger/YYYY/MM/, sem parsear as
    ledgers diárias.

    Retorna o número de dias com atividade no mês (0 se não gravou)."""
    base = f"orgs/{org_id}/ledger/"
    prefixo_mes = f"{base}{ano:04d}/{mes:02d}/"

    arquivos_keys = [
        k for k in _listar_keys(prefixo_mes)
        if "/arquivos/" in k and not k.endswith((".meta.json", ".txt", ".summary.md"))
    ]

    por_dia = defaultdict(list)
    for k in arquivos_keys:
        partes = k[len(base):].split("/")
        # esperado: [YYYY, MM, DD, "arquivos", nome]
        if len(partes) >= 5 and partes[3] == "arquivos":
            try:
                dia = date(int(partes[0]), int(partes[1]), int(partes[2]))
            except ValueError:
                continue
            por_dia[dia].append(k)

    if not por_dia:
        logger.info(f"[ledger] nenhum arquivo em {prefixo_mes} — mensal não gerada")
        return 0

    blocos = []
    for dia in sorted(por_dia):
        linhas = [f"## {dia.strftime('%d/%m/%Y')}\n"]
        for k in sorted(por_dia[dia]):
            nome = k.rsplit("/", 1)[-1]

            link = ""
            evento = "criado"
            meta_raw = _ler_s3(f"{k}.meta.json")
            if meta_raw:
                try:
                    meta = json.loads(meta_raw)
                    link = meta.get("link", "")
                    evento = meta.get("evento", "criado")
                except json.JSONDecodeError:
                    pass

            summary = _ler_s3(f"{k}.summary.md")

            rotulo = "ARQUIVO MODIFICADO" if evento == "modificado" else "ARQUIVO"
            bloco = f"[{rotulo}] {nome} — {link}" if link else f"[{rotulo}] {nome}"
            if summary:
                bloco += f"\n[SUMMARY] {summary}"
            linhas.append(bloco + "\n")
        blocos.append("\n".join(linhas))

    titulo = f"# {_nome_org(org_id)} · {MESES_PT[mes - 1]} {ano}\n\n"
    md = titulo + "\n".join(blocos)

    key_mensal = f"orgs/{org_id}/monthly/{ano:04d}-{mes:02d}.md"
    _get_s3().put_object(
        Bucket=BUCKET,
        Key=key_mensal,
        Body=md.encode("utf-8"),
        ContentType="text/markdown; charset=utf-8",
    )

    logger.info(f"[ledger] mensal compilada {key_mensal} — {len(por_dia)} dias com atividade")
    return len(por_dia)
