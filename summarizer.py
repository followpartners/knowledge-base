import os
import logging

from anthropic import Anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_INPUT_CHARS = 30000


def gerar_summary(texto: str, nome: str, mime_type: str) -> str | None:
    if not texto or not texto.strip():
        return None

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("[summarizer] ANTHROPIC_API_KEY não configurada")
        return None

    cliente = Anthropic(api_key=api_key)
    texto_truncado = texto[:MAX_INPUT_CHARS]

    prompt = f"""Resume o documento abaixo em 2 a 4 frases concisas em português, \
cobrindo: (1) tipo do documento, (2) tema ou assunto principal, \
(3) partes envolvidas se aplicável, (4) datas relevantes se aplicável.

Nome do arquivo: {nome}
Tipo MIME: {mime_type}

Conteúdo:
{texto_truncado}

Responde APENAS com o summary, sem prefixos ou explicações."""

    try:
        resp = cliente.messages.create(
            model=MODEL,
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.warning(f"[summarizer] erro ao resumir {nome}: {e}")
        return None
