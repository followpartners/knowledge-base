#!/usr/bin/env python3
"""Gerencia vínculos canal↔pasta (escopos.json no bucket do Task Manager).

Substituto temporário da tool do Task (Sprint 2). Rodar no console do
Railway:

    python scripts/criar_vinculo_manual.py vincular \
      --channel_id C0123ABC --folder_id 1zyiqZee...

    python scripts/criar_vinculo_manual.py desvincular \
      --channel_id C0123ABC --folder_id 1zyiqZee...

    python scripts/criar_vinculo_manual.py listar
"""
import sys
import os
import argparse
import json
import re
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from ledger import _get_s3, TASK_MANAGER_BUCKET
from escopos import ESCOPOS_KEY, ler_vinculos

RE_CHANNEL = re.compile(r"^[CGD][A-Z0-9]+$")
RE_FOLDER = re.compile(r"^[a-zA-Z0-9_-]{20,}$")


def _validar(channel_id: str, folder_id: str) -> bool:
    ok = True
    if not RE_CHANNEL.match(channel_id):
        print(f"ERRO: channel_id inválido: '{channel_id}' — esperado formato Slack (ex: C0123ABC)")
        ok = False
    if not RE_FOLDER.match(folder_id):
        print(f"ERRO: folder_id inválido: '{folder_id}' — esperado ID de pasta do Drive (20+ chars alfanuméricos)")
        ok = False
    return ok


def _gravar(vinculos: list[dict]):
    _get_s3().put_object(
        Bucket=TASK_MANAGER_BUCKET,
        Key=ESCOPOS_KEY,
        Body=json.dumps({"vinculos": vinculos}, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def cmd_vincular(args):
    if not _validar(args.channel_id, args.folder_id):
        sys.exit(1)

    vinculos = ler_vinculos()
    for v in vinculos:
        if v.get("channel_id") == args.channel_id and v.get("folder_id") == args.folder_id:
            print(f"Vínculo já existe: {args.channel_id} ↔ {args.folder_id} (criado em {v.get('criado_em', '?')})")
            return

    vinculos.append({
        "channel_id": args.channel_id,
        "folder_id": args.folder_id,
        "criado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "criado_por": "manual",
    })
    _gravar(vinculos)
    print(f"Vínculo criado: {args.channel_id} ↔ {args.folder_id}")
    print(f"Total de vínculos: {len(vinculos)}")


def cmd_desvincular(args):
    vinculos = ler_vinculos()
    restantes = [
        v for v in vinculos
        if not (v.get("channel_id") == args.channel_id and v.get("folder_id") == args.folder_id)
    ]
    if len(restantes) == len(vinculos):
        print(f"Vínculo não encontrado: {args.channel_id} ↔ {args.folder_id}")
        return

    _gravar(restantes)
    print(f"Vínculo removido: {args.channel_id} ↔ {args.folder_id}")
    print(f"Total de vínculos: {len(restantes)}")


def cmd_listar(args):
    vinculos = ler_vinculos()
    if not vinculos:
        print("Nenhum vínculo cadastrado.")
        return

    print(f"{len(vinculos)} vínculo(s):\n")
    for i, v in enumerate(vinculos, 1):
        print(f"  {i}. canal {v.get('channel_id', '?')} ↔ pasta {v.get('folder_id', '?')}")
        print(f"     criado em {v.get('criado_em', '?')} por {v.get('criado_por', '?')}")


def main():
    parser = argparse.ArgumentParser(description="Gerencia vínculos canal↔pasta (escopos)")
    sub = parser.add_subparsers(dest="acao", required=True)

    p_vincular = sub.add_parser("vincular", help="Cria um vínculo canal↔pasta")
    p_vincular.add_argument("--channel_id", required=True)
    p_vincular.add_argument("--folder_id", required=True)
    p_vincular.set_defaults(func=cmd_vincular)

    p_desvincular = sub.add_parser("desvincular", help="Remove um vínculo canal↔pasta")
    p_desvincular.add_argument("--channel_id", required=True)
    p_desvincular.add_argument("--folder_id", required=True)
    p_desvincular.set_defaults(func=cmd_desvincular)

    p_listar = sub.add_parser("listar", help="Lista todos os vínculos")
    p_listar.set_defaults(func=cmd_listar)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
