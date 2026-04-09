"""
Follow Partners — Knowledge Base
Servidor FastAPI que lê dados do S3 e monta grafo
com NetworkX para visualização D3.js.
"""

import os
import json
import logging
from datetime import datetime

import boto3
import networkx as nx
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Follow Knowledge Base")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── S3 ─────────────────────────────────────────────────────────────────────────

BUCKET = os.getenv("AWS_BUCKET_NAME", "cfo-agent-banco")
REGION = os.getenv("AWS_REGION", "sa-east-1")

_s3 = None

def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client(
            "s3",
            region_name=REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
    return _s3


def _ler_json_s3(key: str):
    """Lê um arquivo JSON do S3. Retorna None se não existir."""
    try:
        resp = get_s3().get_object(Bucket=BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except get_s3().exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning(f"[s3] erro ao ler {key}: {e}")
        return None


# ── Cache ──────────────────────────────────────────────────────────────────────

_cache = {}

def _invalidar_cache(org_id: str = None):
    """Invalida o cache do grafo. Se org_id, só invalida essa org."""
    if org_id:
        _cache.pop(org_id, None)
    else:
        _cache.clear()
    logger.info(f"[cache] invalidado org_id={org_id or 'todos'}")


# ── Grafo ──────────────────────────────────────────────────────────────────────

def _grafo_para_json(org_id: str = None) -> dict:
    """
    Lê a estrutura de documentos do S3 e monta o grafo
    com NetworkX. Retorna JSON de nós e arestas.
    """
    if not org_id:
        return {"nos": [], "arestas": [], "erro": "org_id obrigatório"}

    if org_id in _cache:
        return _cache[org_id]

    G = nx.DiGraph()
    prefix = f"orgs/{org_id}/documentos"

    # Lê índice de slugs
    indice = _ler_json_s3(f"{prefix}/indice.json")
    if not indice:
        return {"nos": [], "arestas": []}

    slugs = indice if isinstance(indice, list) else indice.get("slugs", [])

    for slug in slugs:
        base = f"{prefix}/arquivos/{slug}"

        # Lê metadata do arquivo
        meta = _ler_json_s3(f"{base}/metadata.json")
        if not meta:
            continue

        arquivo_id = slug
        G.add_node(arquivo_id, **{
            "tipo":  "arquivo",
            "nome":  meta.get("nome", slug),
            "data":  meta.get("data_upload", ""),
        })

        # Lê contextos do arquivo
        contextos = _ler_json_s3(f"{base}/contextos.json")
        if not contextos or not isinstance(contextos, list):
            continue

        for i, ctx in enumerate(contextos):
            ctx_id = f"{slug}_ctx_{i}"
            G.add_node(ctx_id, **{
                "tipo":  "contexto",
                "texto": ctx.get("texto", "")[:120],
                "fonte": ctx.get("fonte", ""),
                "data":  ctx.get("data", ""),
            })
            G.add_edge(arquivo_id, ctx_id, tipo="GERA_CONTEXTO")

    # Converte para JSON
    nos = []
    for nid, attrs in G.nodes(data=True):
        no = {"id": nid, **attrs}
        if attrs["tipo"] == "arquivo":
            no["grupo"] = 1
        else:
            no["nome"] = "Contexto"
            no["resumo"] = attrs.get("texto", "")
            no["grupo"] = 2
        nos.append(no)

    arestas = []
    for u, v, attrs in G.edges(data=True):
        arestas.append({
            "origem":  u,
            "destino": v,
            "tipo":    attrs.get("tipo", "GERA_CONTEXTO"),
        })

    resultado = {"nos": nos, "arestas": arestas}
    _cache[org_id] = resultado
    return resultado


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    """Serve a página do knowledge base."""
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/grafo")
async def grafo(org_id: str = None):
    """Retorna o grafo de uma organização como JSON."""
    try:
        return _grafo_para_json(org_id)
    except Exception as e:
        logger.error(f"[grafo] erro: {e}")
        return {"nos": [], "arestas": [], "erro": str(e)}


@app.post("/notify")
async def notify(
    x_notify_token: str = Header(None),
    org_id: str = None,
):
    """
    Chamado pelo cfo-agent quando novos dados são escritos no S3.
    Invalida o cache para forçar releitura na próxima requisição.
    """
    token_esperado = os.getenv("NOTIFY_TOKEN", "follow2024")
    if x_notify_token != token_esperado:
        raise HTTPException(status_code=401, detail="token inválido")

    _invalidar_cache(org_id)
    return {"ok": True, "invalidado": org_id or "todos"}
