import logging
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from scheduler import iniciar, pipeline
from ledger import compilar_ledger

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ORG_ID = "followpartners"


@asynccontextmanager
async def lifespan(app: FastAPI):
    iniciar()
    yield


app = FastAPI(title="Follow Knowledge Base", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/sync")
async def sync():
    entries = pipeline()
    return {"status": "ok", "entries": entries}
