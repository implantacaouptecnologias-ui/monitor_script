import time
import json
import os
from typing import List, Dict, Any
import requests

# =========================
# CONFIGURAÇÕES GERAIS
# =========================

STATE_FILE = "state.json"
POLL_INTERVAL = 60  # em segundos
ID_FIELD = "id"

API_HEADERS = {
    "Content-Type": "application/json",
    "access-token": "fb3fab57c22537e4414d28e364c3bddcb7c8a693",
    "secret-access-token": "fea306c85c162d5f0c147929ada9615c9882bb79"
}

WEBHOOK_HEADERS = {
    "Content-Type": "application/json"
}

# =========================
# CONFIG DE CADA MONITOR
# =========================
# Cada entrada representa um tipo de dado que queremos monitorar.

MONITORS = {
    "orcamentos": {
        "api_url": "https://api.beteltecnologia.com/orcamentos",
        "webhook_url": "https://tecnologiasup.app.n8n.cloud/webhook/0a8f93fb-e67f-4753-96f5-3146c0b73f36",
        "state_key": "orcamentos_last_id",
        "params": {
            "pagina": 1,
            "centro_custo_id": 240910
        }
    },
    "vendas": {
        "api_url": "https://api.beteltecnologia.com/vendas",
        "webhook_url": "https://tecnologiasup.app.n8n.cloud/webhook-test/ae631ac2-e765-4181-be96-7f2973ec3e6f",
        "state_key": "vendas_last_id",
        "params": {
            "pagina": 1,
            "situacao_id": 5943149
        }
    },
    "ordens_servicos": {
        "api_url": "https://api.beteltecnologia.com/ordens_servicos",
        "webhook_url": "https://tecnologiasup.app.n8n.cloud/webhook/821b4817-b01d-42ba-ae18-729912b1a260",
        "state_key": "ordens_servicos_last_id",
        "params": {
            "pagina": 1,
            "centro_custo_id": 240830
        }
    },
}

# =========================
# FUNÇÕES AUXILIARES
# =========================

def load_state() -> Dict[str, Any]:
    """Carrega o estado completo do arquivo (últimos IDs por tipo)."""
    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {}
    except Exception:
        return {}

    # Compatibilidade com versão antiga que só tinha "last_id" (apenas orçamentos)
    if "last_id" in data and "orcamentos_last_id" not in data:
        try:
            data["orcamentos_last_id"] = int(data["last_id"])
        except Exception:
            pass

    return data


def save_state(state: Dict[str, Any]) -> None:
    """Salva o estado completo no arquivo."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def fetch_items(api_url: str, params: Dict[str, Any], last_id: int) -> List[Dict[str, Any]]:
    """
    Busca itens na API Betel para um tipo específico.
    Espera resposta no formato: { "data": [ ... ] }
    """
    try:
        resp = requests.get(api_url, headers=API_HEADERS, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERRO] Falha ao consultar API ({api_url}): {e}")
        return []

    try:
        raw = resp.json()
    except Exception as e:
        print(f"[ERRO] Não foi possível fazer parse do JSON da API ({api_url}): {e}")
        return []

    if not isinstance(raw, dict):
        print(f"[ERRO] Resposta da API ({api_url}) não é um objeto JSON como esperado.")
        return []

    data = raw.get("data", [])

    if not isinstance(data, list):
        print(f"[ERRO] Campo 'data' da API ({api_url}) não é uma lista.")
        return []

    # Filtra manualmente por ID > last_id
    if last_id:
        try:
            data = [item for item in data if int(item.get(ID_FIELD, 0)) > last_id]
        except Exception as e:
            print(f"[ERRO] Problema ao filtrar por ID em ({api_url}): {e}")

    return data


def trigger_webhook_single(webhook_url: str, item: Dict[str, Any], label: str = "") -> bool:
    """
    Dispara o webhook para UM item.
    Mantém o formato antigo (um único JSON de item por chamada).
    """
    try:
        resp = requests.post(
            webhook_url,
            headers=WEBHOOK_HEADERS,
            data=json.dumps(item),
            timeout=30
        )
        if 200 <= resp.status_code < 300:
            print(f"[OK] Webhook '{label}' enviado. ID={item.get(ID_FIELD)}")
            return True
        else:
            print(
                f"[ERRO] Webhook '{label}' falhou para ID={item.get(ID_FIELD)} - "
                f"Status: {resp.status_code}, Resp: {resp.text}"
            )
            return False
    except requests.RequestException as e:
        print(f"[ERRO] Exceção ao enviar webhook '{label}' para ID={item.get(ID_FIELD)}: {e}")
        return False


def process_monitor(name: str, cfg: Dict[str, Any], state: Dict[str, Any]) -> None:
    """
    Processa um tipo específico (orcamentos, vendas, ordens_servicos):
    - Busca itens
    - Identifica novos
    - Dispara webhook UMA VEZ se houver novos
    - Atualiza último ID
    """
    state_key = cfg["state_key"]
    last_id = int(state.get(state_key, 0) or 0)

    print(f"[INFO][{name}] Último ID conhecido: {last_id}")

    # Copia params para não alterar o dict original
    params = dict(cfg.get("params", {}))
    items = fetch_items(cfg["api_url"], params, last_id)

    if not items:
        print(f"[INFO][{name}] Nenhum item retornado pela API (após filtro por ID).")
        return

    # Ordena por ID crescente
    try:
        items_sorted = sorted(items, key=lambda x: int(x.get(ID_FIELD, 0)))
    except Exception:
        items_sorted = items

    novos = [i for i in items_sorted if int(i.get(ID_FIELD, 0)) > last_id]

    if not novos:
        print(f"[INFO][{name}] Nenhum item novo encontrado.")
        return

    print(f"[INFO][{name}] Encontrados {len(novos)} itens novos.")

    # Envia o webhook apenas UMA VEZ, usando o último item novo (mais recente)
    ultimo_item = novos[-1]

    ok = trigger_webhook_single(cfg["webhook_url"], ultimo_item, label=name)

    if not ok:
        print(f"[AVISO][{name}] Webhook falhou, não atualizando last_id para evitar perder itens.")
        return

    # Atualiza o last_id para o maior ID encontrado
    try:
        max_id = max(int(i.get(ID_FIELD, 0)) for i in novos)
    except Exception:
        max_id = int(ultimo_item.get(ID_FIELD, last_id))

    if max_id > last_id:
        state[state_key] = max_id
        save_state(state)
        print(f"[INFO][{name}] last_id atualizado para {max_id}")
    else:
        print(f"[INFO][{name}] last_id permaneceu em {last_id}")


def process_cycle():
    """Executa um ciclo completo, processando todos os monitores configurados."""
    state = load_state()

    for name, cfg in MONITORS.items():
        print(f"\n==============================")
        print(f"[INFO] Processando monitor: {name}")
        print(f"==============================")
        try:
            process_monitor(name, cfg, state)
        except Exception as e:
            # Evita que um erro em um tipo pare os demais
            print(f"[ERRO][{name}] Erro inesperado no monitor: {e}")


def main_loop():
    """Loop infinito que roda o ciclo em intervalos configurados."""
    print("[INFO] Iniciando monitor de API Betel...")
    print(f"[INFO] Monitores configurados: {', '.join(MONITORS.keys())}")
    print(f"[INFO] Intervalo de consulta: {POLL_INTERVAL} segundos.\n")

    while True:
        try:
            process_cycle()
        except Exception as e:
            print(f"[ERRO] Erro inesperado no ciclo principal: {e}")
        print(f"\n[INFO] Aguardando {POLL_INTERVAL} segundos...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main_loop()
