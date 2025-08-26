# core/intelipost.py

import time
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from core.utils import retry
import threading
import math

def _execute_graphql_via_selenium(driver, query: str, variables: dict, token: str) -> dict | None:
    payload = { "query": query, "variables": variables }
    script = f"""
        const payload = {json.dumps(payload)};
        const token = arguments[1];
        const callback = arguments[arguments.length - 1];
        if (!token) {{
            callback({{ "js_error": "Token de acesso não foi fornecido para a chamada JavaScript." }});
            return;
        }}
        fetch("https://graphql.intelipost.com.br/", {{
            "method": "POST",
            "headers": {{ "Content-Type": "application/json", "Authorization": `Bearer ${{token}}` }},
            "body": JSON.stringify(payload)
        }})
        .then(response => response.json())
        .then(data => callback(data))
        .catch(error => callback({{ "js_error": error.toString() }}));
    """
    try:
        response_data = driver.execute_async_script(script, payload, token)
        if response_data and "js_error" in response_data:
            print(f"ERRO (GraphQL via JS): {response_data['js_error']}")
            return None
        return response_data
    except Exception as e:
        print(f"ERRO (Selenium execute_script): Falha ao executar a query. Detalhe: {e}")
        return None

@retry(tentativas=3, delay=5)
def preparar_pagina_e_capturar_token(driver, client_id: str):
    wait = WebDriverWait(driver, 60)
    print("INFO: Iniciando preparação para o cliente (login via sysnode)...")
    driver.get(f"https://api-sysnode.intelipost.com.br/sysnode/edit_client?q={client_id}")
    print("INFO: Clicando no link de e-mail para ativar a sessão...")
    email_login_link_xpath = f"//td[normalize-space()='{client_id}']/following-sibling::td[1]/a"
    link_elemento = wait.until(EC.visibility_of_element_located((By.XPATH, email_login_link_xpath)))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", link_elemento)
    time.sleep(1)
    actions = ActionChains(driver)
    actions.move_to_element(link_elemento).pause(0.5).click_and_hold().pause(0.2).release().perform()

def obter_centros_de_distribuicao_api(driver, token: str) -> list:
    if not driver or not token: return []
    query = "{ warehouses { id official_name } }"
    json_response = _execute_graphql_via_selenium(driver, query, {}, token)
    if json_response:
        warehouses = json_response.get("data", {}).get("warehouses", [])
        print(f"INFO (API): {len(warehouses)} Centros de Distribuição encontrados.")
        return [(wh['id'], wh['official_name']) for wh in warehouses]
    return []

def obter_transportadoras_api(driver, token: str) -> list:
    if not driver or not token: return []
    query = "query ($active: Boolean) { logisticProviders(active: $active) { id name } }"
    variables = {"active": False}
    json_response = _execute_graphql_via_selenium(driver, query, variables, token)
    if json_response:
        providers = json_response.get("data", {}).get("logisticProviders", [])
        print(f"INFO (API): {len(providers)} Transportadoras encontradas.")
        return [(lp['id'], lp['name']) for lp in providers]
    return []

def obter_pre_faturas_prontas_por_data(driver, token: str, data_inicio_str: str, data_fim_str: str, lista_ids_warehouses: list, lista_ids_transportadoras: list, stop_event: threading.Event) -> list:
    if not driver or not token: return []
    todos_os_itens = []
    pagina_atual = 1
    limite_por_pagina = 500
    total_paginas = 0
    query_string = """
        query ($warehouses: [Int], $delivery_methods: [Int], $logistic_providers: [Int], $status: [String], $margin_status: String, $difference: String, $date_range: DateRangeInput, $search_by: String!, $search_values: [String], $page: Int, $limit: Int) {
            preInvoicesV2(warehouses: $warehouses, delivery_methods: $delivery_methods, logistic_providers: $logistic_providers, status: $status, margin_status: $margin_status, difference: $difference, date_range: $date_range, search_by: $search_by, search_values: $search_values, page: $page, limit: $limit) {
                total
                hasNextPage
                items { id, cte { key }, invoice { order_number }, status, tms_value, cte_value }
            }
        }
    """
    try:
        print("INFO (API Paginada): Calculando total de páginas...")
        variables = {
            "date_range": {"start": data_inicio_str, "end": data_fim_str},
            "logistic_providers": lista_ids_transportadoras, "warehouses": lista_ids_warehouses,
            "delivery_methods": [], 
            "status": ["WAITING_FOR_CONCILIATION"], # <-- IGNORANDO PEDIDOS PAGOS
            "margin_status": "", "difference": "",
            "search_by": "order_number", "search_values": [],
            "page": 1, "limit": 1
        }
        json_response = _execute_graphql_via_selenium(driver, query_string, variables, token)
        if not json_response or ("errors" in json_response and json_response["errors"]):
             print(f"ERRO (API Paginada): A API GraphQL retornou erros: {json_response.get('errors') if json_response else 'N/A'}")
             return []
        data = json_response.get("data", {}).get("preInvoicesV2", {})
        total_itens = data.get("total", 0)
        if total_itens > 0:
            total_paginas = math.ceil(total_itens / limite_por_pagina)
        print(f"INFO (API Paginada): {total_itens} itens encontrados, totalizando {total_paginas} páginas.")
    except Exception as e:
        print(f"ERRO (API Paginada): Não foi possível pré-calcular o total de páginas. Detalhe: {e}")
        return []
    if total_paginas == 0: return []
    while pagina_atual <= total_paginas:
        if stop_event.is_set(): break
        try:
            print(f"INFO (API Paginada): Buscando página {pagina_atual}/{total_paginas}...")
            variables['page'] = pagina_atual
            variables['limit'] = limite_por_pagina
            json_response = _execute_graphql_via_selenium(driver, query_string, variables, token)
            if not json_response:
                print(f"ERRO (API Paginada): Falha ao buscar a página {pagina_atual}. Resposta vazia.")
                break
            data = json_response.get("data", {}).get("preInvoicesV2", {})
            items_da_pagina = data.get("items", [])
            if items_da_pagina: todos_os_itens.extend(items_da_pagina)
            if not data.get("hasNextPage", False): break
            pagina_atual += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"ERRO (API Paginada): Falha ao buscar a página {pagina_atual}. Detalhe: {e}")
            break
    return todos_os_itens

def obter_detalhes_pre_fatura(driver, token: str, pre_fatura_id: str) -> dict | None:
    if not driver or not pre_fatura_id: return None
    query_string = """
        query ($id: String!, $action: String!) {
            preInvoiceDetail(id: $id, action: $action) {
                values {
                    origin_zipcode, destination_zipcode, invoice { number },
                    volumes { weight, squared_weight, selected_weight, dimensions { width, height, length } }
                }
            }
        }
    """
    variables = {"id": pre_fatura_id, "action": "values"}
    json_response = _execute_graphql_via_selenium(driver, query_string, variables, token)
    if json_response and "data" in json_response:
        detail_data = json_response.get("data", {}).get("preInvoiceDetail", {})
        if detail_data and detail_data.get("values"):
            return detail_data["values"][0]
    return None

def obter_configuracao_margem_api(driver, token: str) -> dict | None:
    if not driver: return None
    query = "{ reconConfig { marginType, marginFixedValue, marginPercentageValue, marginMixedFixedValue, marginMixedPercentageValue } }"
    json_response = _execute_graphql_via_selenium(driver, query, {}, token)
    if json_response and "data" in json_response:
        recon_config = json_response.get("data", {}).get("reconConfig")
        if recon_config is None: return None
        margin_type = recon_config.get("marginType")
        config_margem = {}
        if margin_type is None:
            config_margem['type'] = "SYSTEM_DEFAULT"
        elif margin_type == "ABSOLUTE":
            config_margem.update({'type': "ABSOLUTE", 'value': recon_config.get("marginFixedValue", 0.0)})
        elif margin_type == "PERCENTAGE":
            config_margem.update({'type': "PERCENTAGE", 'value': recon_config.get("marginPercentageValue", 0.0)})
        elif margin_type == "MIXED_GREATER":
            config_margem.update({
                'type': 'DYNAMIC_CHOICE', 'absolute_value': recon_config.get("marginMixedFixedValue", 0.0),
                'percentage_value': recon_config.get("marginMixedPercentageValue", 0.0)
            })
        else:
            return None
        return config_margem
    return None