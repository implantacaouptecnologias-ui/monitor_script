import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, List

import redis
import requests

# ── Logging estruturado (Railway/Render coletam stdout como JSON) ─────────────
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ── Configurações via variáveis de ambiente ───────────────────────────────────
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
ID_FIELD = "id"

API_HEADERS = {
    "Content-Type": "application/json",
    "access-token": os.environ["BETEL_ACCESS_TOKEN"],
    "secret-access-token": os.environ["BETEL_SECRET_ACCESS_TOKEN"],
}

WEBHOOK_HEADERS = {"Content-Type": "application/json"}

BETEL_BASE_URL = os.environ.get("BETEL_API_URL", "https://api.beteltecnologia.com")

MONITORS: Dict[str, Dict[str, Any]] = {
    "orcamentos": {
        "api_url": f"{BETEL_BASE_URL}/orcamentos",
        "webhook_url": os.environ["WEBHOOK_ORCAMENTOS"],
        "state_key": "orcamentos_last_id",
        "params": {
            "pagina": 1,
            "centro_custo_id": int(os.environ.get("CENTRO_CUSTO_ORCAMENTOS", "240910")),
        },
    },
    "vendas": {
        "api_url": f"{BETEL_BASE_URL}/vendas",
        "webhook_url": os.environ["WEBHOOK_VENDAS"],
        "state_key": "vendas_last_id",
        "params": {
            "pagina": 1,
            "situacao_id": int(os.environ.get("SITUACAO_VENDAS", "5943149")),
        },
    },
    "ordens_servicos": {
        "api_url": f"{BETEL_BASE_URL}/ordens_servicos",
        "webhook_url": os.environ["WEBHOOK_ORDENS_SERVICOS"],
        "state_key": "ordens_servicos_last_id",
        "params": {
            "pagina": 1,
            "centro_custo_id": int(os.environ.get("CENTRO_CUSTO_OS", "240830")),
        },
    },
}


# ── Estado no Redis ───────────────────────────────────────────────────────────

def build_redis_client() -> redis.Redis:
    return redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


def seed_state_from_env(client: redis.Redis) -> None:
    """Grava os IDs iniciais no Redis se ainda não existirem.

    Só tem efeito na primeira execução — as variáveis de ambiente abaixo
    são opcionais e devem ser removidas do painel após o primeiro deploy.
    """
    initial = {
        "orcamentos_last_id": os.environ.get("ORCAMENTOS_LAST_ID"),
        "vendas_last_id": os.environ.get("VENDAS_LAST_ID"),
        "ordens_servicos_last_id": os.environ.get("ORDENS_SERVICOS_LAST_ID"),
    }
    for key, val in initial.items():
        if val and not client.exists(key):
            client.set(key, int(val))
            log.info(f"Seeded initial state key={key} value={val}")


def load_state(client: redis.Redis) -> Dict[str, int]:
    state: Dict[str, int] = {}
    for cfg in MONITORS.values():
        key = cfg["state_key"]
        val = client.get(key)
        try:
            state[key] = int(val) if val is not None else 0
        except (ValueError, TypeError):
            state[key] = 0
    return state


def persist_last_id(client: redis.Redis, key: str, value: int) -> None:
    client.set(key, value)


# ── Lógica de negócio ─────────────────────────────────────────────────────────

def fetch_items(api_url: str, params: Dict[str, Any], last_id: int) -> List[Dict[str, Any]]:
    try:
        resp = requests.get(api_url, headers=API_HEADERS, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"API request failed url={api_url} error={e}")
        return []

    try:
        raw = resp.json()
    except Exception as e:
        log.error(f"JSON parse failed url={api_url} error={e}")
        return []

    if not isinstance(raw, dict):
        log.error(f"Unexpected response format url={api_url}")
        return []

    data = raw.get("data", [])
    if not isinstance(data, list):
        log.error(f"'data' is not a list url={api_url}")
        return []

    if last_id:
        try:
            data = [item for item in data if int(item.get(ID_FIELD, 0)) > last_id]
        except Exception as e:
            log.error(f"ID filter failed url={api_url} error={e}")

    return data


def trigger_webhook(webhook_url: str, item: Dict[str, Any], label: str) -> bool:
    try:
        resp = requests.post(
            webhook_url,
            headers=WEBHOOK_HEADERS,
            data=json.dumps(item),
            timeout=30,
        )
        if 200 <= resp.status_code < 300:
            log.info(f"Webhook sent monitor={label} id={item.get(ID_FIELD)}")
            return True
        log.error(
            f"Webhook failed monitor={label} id={item.get(ID_FIELD)} "
            f"status={resp.status_code} body={resp.text[:300]}"
        )
        return False
    except requests.RequestException as e:
        log.error(f"Webhook exception monitor={label} id={item.get(ID_FIELD)} error={e}")
        return False


def process_monitor(
    name: str,
    cfg: Dict[str, Any],
    state: Dict[str, int],
    redis_client: redis.Redis,
) -> None:
    state_key = cfg["state_key"]
    last_id = state.get(state_key, 0)
    log.info(f"Processing monitor={name} last_id={last_id}")

    items = fetch_items(cfg["api_url"], dict(cfg.get("params", {})), last_id)
    if not items:
        log.info(f"No new items monitor={name}")
        return

    try:
        items_sorted = sorted(items, key=lambda x: int(x.get(ID_FIELD, 0)))
    except Exception:
        items_sorted = items

    novos = [i for i in items_sorted if int(i.get(ID_FIELD, 0)) > last_id]
    if not novos:
        log.info(f"All items already seen monitor={name}")
        return

    log.info(f"New items found monitor={name} count={len(novos)}")

    ultimo_item = novos[-1]
    if not trigger_webhook(cfg["webhook_url"], ultimo_item, label=name):
        log.warning(f"Webhook failed, last_id not updated monitor={name}")
        return

    try:
        max_id = max(int(i.get(ID_FIELD, 0)) for i in novos)
    except Exception:
        max_id = int(ultimo_item.get(ID_FIELD, last_id))

    if max_id > last_id:
        persist_last_id(redis_client, state_key, max_id)
        log.info(f"last_id updated monitor={name} new_id={max_id}")


def process_cycle(redis_client: redis.Redis) -> None:
    state = load_state(redis_client)
    for name, cfg in MONITORS.items():
        try:
            process_monitor(name, cfg, state, redis_client)
        except Exception as e:
            log.error(f"Unexpected error monitor={name} error={e}")


# ── Shutdown gracioso (SIGTERM enviado pelo Railway/Render ao reiniciar) ──────

_running = True


def _handle_signal(signum, frame):
    global _running
    log.info(f"Signal {signum} received, finishing current cycle and exiting")
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    log.info(f"Starting monitor monitors={list(MONITORS.keys())} interval={POLL_INTERVAL}s")

    redis_client = build_redis_client()
    seed_state_from_env(redis_client)

    while _running:
        try:
            process_cycle(redis_client)
        except Exception as e:
            log.error(f"Cycle error: {e}")

        if not _running:
            break

        log.info(f"Sleeping seconds={POLL_INTERVAL}")
        # Sleep em chunks de 1s para responder ao SIGTERM rapidamente
        for _ in range(POLL_INTERVAL):
            if not _running:
                break
            time.sleep(1)

    log.info("Monitor stopped")


if __name__ == "__main__":
    main()
