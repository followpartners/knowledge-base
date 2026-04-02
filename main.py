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
    Se user_id for None, retorna todos os usuários.
    """
    if user_id:
        query = """
        MATCH (e:Empresa {user_id: $user_id})
        OPTIONAL MATCH (e)-[:TEM_ARQUIVO]->(a:Arquivo)
        OPTIONAL MATCH (a)-[:GERA_CONTEXTO]->(c:Contexto)
        RETURN e, a, c
        """
        params = {"user_id": user_id}
    else:
        query = """
        MATCH (e:Empresa)
        OPTIONAL MATCH (e)-[:TEM_ARQUIVO]->(a:Arquivo)
        OPTIONAL MATCH (a)-[:GERA_CONTEXTO]->(c:Contexto)
        RETURN e, a, c
        """
        params = {}

    nos, arestas = [], []
    ids_vistos = set()

    with get_driver().session() as session:
        result = session.run(query, **params)
        for record in result:
            e = record["e"]
            a = record["a"]
            c = record["c"]

            if e and e["id"] not in ids_vistos:
                nos.append({
                    "id":     e["id"],
                    "tipo":   "empresa",
                    "nome":   e.get("nome", e["id"]),
                    "setor":  e.get("setor", ""),
                    "status": e.get("status", "ativa"),
                    "grupo":  1,
                })
                ids_vistos.add(e["id"])

            if a and a["id"] not in ids_vistos:
                nos.append({
                    "id":         a["id"],
                    "tipo":       "arquivo",
                    "nome":       a.get("nome", a["id"]),
                    "empresa_id": a.get("empresa_id", ""),
                    "grupo":      2,
                })
                ids_vistos.add(a["id"])
                if e:
                    arestas.append({
                        "origem":  e["id"],
                        "destino": a["id"],
                        "tipo":    "TEM_ARQUIVO",
                    })

            if c and c["id"] not in ids_vistos:
                nos.append({
                    "id":         c["id"],
                    "tipo":       "contexto",
                    "nome":       "Contexto",
                    "resumo":     c.get("texto", "")[:120],
                    "empresa_id": c.get("empresa_id", ""),
                    "grupo":      3,
                })
                ids_vistos.add(c["id"])
                if a:
                    arestas.append({
                        "origem":  a["id"],
                        "destino": c["id"],
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
