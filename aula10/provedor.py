"""
provedor.py -- Configura o SDK de agentes para OpenAI, Ollama ou OpenRouter.
"""
import json
import os
import urllib.request

import httpx
from agents import set_default_openai_api, set_default_openai_client, set_tracing_disabled
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

AJUDA = (
    "Configure um provedor real: copie o .env.example para .env e preencha.\n"
    "  - OpenAI: PROVEDOR=openai e OPENAI_API_KEY=...\n"
    "  - OpenRouter: PROVEDOR=openrouter e OPENROUTER_API_KEY=...\n"
    "  - Ollama (grátis, local): PROVEDOR=ollama, `ollama serve` no ar e o modelo baixado."
)


def _cliente(base_url: str | None, api_key: str, default_headers: dict | None = None) -> AsyncOpenAI:
    http = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=0), timeout=180)
    return AsyncOpenAI(base_url=base_url, api_key=api_key, http_client=http, default_headers=default_headers)


def _modelos_do_ollama(base_url: str) -> list[str] | None:
    """Nomes dos modelos que o Ollama tem (None se ele não responde)."""
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + "/models", timeout=3) as r:
            return [m["id"] for m in json.load(r).get("data", [])]
    except (OSError, ValueError, KeyError):
        return None


def configurar() -> str:
    """Aplica o provedor do `.env` ao SDK de agentes e devolve o nome do modelo em uso."""
    provedor = os.getenv("PROVEDOR", "openai").strip().lower()
    set_tracing_disabled(True)

    if provedor == "openai":
        chave = os.getenv("OPENAI_API_KEY")
        if not chave:
            raise SystemExit("PROVEDOR=openai exige OPENAI_API_KEY.\n" + AJUDA)
        modelo = os.getenv("OPENAI_DEFAULT_MODEL", "gpt-4o-mini")
        os.environ["OPENAI_DEFAULT_MODEL"] = modelo
        set_default_openai_client(_cliente("https://api.openai.com/v1", chave))
        return f"openai:{modelo}"

    if provedor == "openrouter":
        chave = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not chave:
            raise SystemExit("PROVEDOR=openrouter exige OPENROUTER_API_KEY (ou OPENAI_API_KEY).\n" + AJUDA)
        
        # O OpenRouter exige o formato provedor/modelo (ex: openai/gpt-4o-mini ou anthropic/claude-3.5-sonnet)
        modelo = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
        os.environ["OPENAI_DEFAULT_MODEL"] = modelo
        
        headers = {
            "HTTP-Referer": "https://github.com/seu-usuario/seu-projeto", # Opcional
            "X-Title": "Agente PCDF / IBMEC", # Opcional
        }
        
        set_default_openai_client(_cliente("https://openrouter.ai/api/v1", chave, default_headers=headers))
        return f"openrouter:{modelo}"

    if provedor == "ollama":
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        modelo = os.getenv("OLLAMA_MODEL", "llama3.1")
        disponiveis = _modelos_do_ollama(base_url)
        if disponiveis is None:
            raise SystemExit(f"PROVEDOR=ollama, mas o Ollama não responde em {base_url}. Rode `ollama serve`.\n" + AJUDA)
        if not any(m == modelo or m.startswith(modelo + ":") for m in disponiveis):
            raise SystemExit(f"O modelo {modelo!r} não está no Ollama (há: {', '.join(disponiveis) or 'nenhum'}). "
                             f"Rode `ollama pull {modelo}` ou ajuste OLLAMA_MODEL no .env.")
        os.environ["OPENAI_DEFAULT_MODEL"] = modelo
        set_default_openai_client(_cliente(base_url, "ollama"))
        set_default_openai_api("chat_completions")
        return f"ollama:{modelo}"

    raise SystemExit(f"PROVEDOR desconhecido: {provedor!r}. Use openai, openrouter ou ollama.")

