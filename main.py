"""
Follow Partners — Knowledge Base
Servidor FastAPI com WebSocket para visualização
em tempo real do grafo de conhecimento no Neo4j.
"""

import os
import json
import asyncio
import logging
from typing import Set
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
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

# ── Neo4j ──────────────────────────────────────────────────────────────────────

_driver = None

def get_driver():
    global _driver
    if _driver is None:
        uri      = os.getenv("NEO4J_URI")
        user     = os.getenv("NEO4J_USERNAME", "neo4j")
        password = os.getenv("NEO4J_PASSWORD")
        if not uri or not password:
            raise ValueError("NEO4J_URI e NEO4J_PASSWORD são obrigatórios")
        _driver = GraphDatabase.driver(uri, auth=(user, password))
    return _driver


def _grafo_para_json(user_id: str = None) -> dict:
    """
    Lê o grafo do Neo4j e retorna JSON de nós e arestas.
    Busca cada tipo de nó separadamente e constrói arestas
    via propriedade empresa_id (não depende de relacionamentos).
    """
    filtro_empresa = "{user_id: $user_id}" if user_id else ""
    params = {"user_id": user_id} if user_id else {}

    nos, arestas = [], []
    empresas_idx = {}
    arquivos_idx = {}

    with get_driver().session() as session:
        # ── Empresas ───────────────────────────────────────────
        for rec in session.run(
            f"MATCH (e:Empresa {filtro_empresa}) RETURN e", **params
        ):
            e = rec["e"]
            eid = e["id"]
            empresas_idx[eid] = True
            nos.append({
                "id":     eid,
                "tipo":   "empresa",
                "nome":   e.get("nome_principal", e.get("nome", eid)),
                "setor":  e.get("setor", ""),
                "status": e.get("status", "ativa"),
                "grupo":  1,
            })

        # ── Arquivos ───────────────────────────────────────────
        q_arq = (
            "MATCH (a:Arquivo {empresa_id: $user_id}) RETURN a"
            if user_id else "MATCH (a:Arquivo) RETURN a"
        )
        for rec in session.run(q_arq, **params):
            a = rec["a"]
            aid = a["id"]
            emp = a.get("empresa_id", "")
            arquivos_idx[aid] = emp
            nos.append({
                "id":         aid,
                "tipo":       "arquivo",
                "nome":       a.get("nome", aid),
                "empresa_id": emp,
                "grupo":      2,
            })
            if emp in empresas_idx:
                arestas.append({
                    "origem":  emp,
                    "destino": aid,
                    "tipo":    "TEM_ARQUIVO",
                })

        # ── Contextos ──────────────────────────────────────────
        q_ctx = (
            "MATCH (c:Contexto {empresa_id: $user_id}) RETURN c"
            if user_id else "MATCH (c:Contexto) RETURN c"
        )
        for rec in session.run(q_ctx, **params):
            c = rec["c"]
            cid = c["id"]
            emp = c.get("empresa_id", "")
            arq = c.get("arquivo_id", "")
            nos.append({
                "id":         cid,
                "tipo":       "contexto",
                "nome":       "Contexto",
                "resumo":     c.get("texto", "")[:120],
                "empresa_id": emp,
                "grupo":      3,
            })
            if arq and arq in arquivos_idx:
                arestas.append({
                    "origem":  arq,
                    "destino": cid,
                    "tipo":    "GERA_CONTEXTO",
                })
            elif emp in empresas_idx:
                arestas.append({
                    "origem":  emp,
                    "destino": cid,
                    "tipo":    "GERA_CONTEXTO",
                })

    return {"nos": nos, "arestas": arestas}


# ── WebSocket Manager ──────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)
        logger.info(f"[ws] cliente conectado — total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)
        logger.info(f"[ws] cliente desconectado — total: {len(self.active)}")

    async def broadcast(self, data: dict):
        if not self.active:
            return
        msg = json.dumps(data, ensure_ascii=False)
        mortos = set()
        for ws in self.active:
            try:
                await ws.send_text(msg)
            except Exception:
                mortos.add(ws)
        self.active -= mortos


manager = ConnectionManager()


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    """Serve a página do knowledge base."""
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/grafo")
async def grafo(user_id: str = None):
    """Retorna o grafo atual do Neo4j como JSON."""
    try:
        return _grafo_para_json(user_id)
    except Exception as e:
        logger.error(f"[grafo] erro: {e}")
        return {"nos": [], "arestas": [], "erro": str(e)}


@app.post("/notify")
async def notify(x_notify_token: str = Header(None)):
    """
    Chamado pelo cfo-agent quando dados são atualizados no Neo4j.
    Faz broadcast do grafo atualizado para todos os clientes conectados.
    """
    token_esperado = os.getenv("NOTIFY_TOKEN", "follow2024")
    if x_notify_token != token_esperado:
        raise HTTPException(status_code=401, detail="token inválido")

    try:
        grafo = _grafo_para_json()
        await manager.broadcast({
            "tipo":      "update",
            "grafo":     grafo,
            "timestamp": datetime.now().isoformat(),
        })
        return {
            "ok":      True,
            "clients": len(manager.active),
            "nos":     len(grafo["nos"]),
        }
    except Exception as e:
        logger.error(f"[notify] erro: {e}")
        return {"ok": False, "erro": str(e)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        # Envia o grafo atual ao conectar
        grafo = _grafo_para_json()
        await ws.send_text(json.dumps({
            "tipo":      "init",
            "grafo":     grafo,
            "timestamp": datetime.now().isoformat(),
        }, ensure_ascii=False))

        # Mantém conexão viva com ping
        while True:
            await asyncio.sleep(30)
            await ws.send_text(json.dumps({"tipo": "ping"}))
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception as e:
        logger.error(f"[ws] erro: {e}")
        manager.disconnect(ws)
