# core/intelipost.py

import time
import json
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from core.utils import retry
import threading
import math

def _execute_graphql_via_selenium(driver, query: str, variables: dict, token: str, monitor=None) -> dict | None:
    if monitor:
        monitor.increment_api_call()
    
    payload = {"query": query, "variables": variables}
    script = f"""
        const payload = arguments[0];
        const token = arguments[1];
        const callback = arguments[arguments.length - 1];
        if (!token) {{
            callback({{"js_error": "Token de acesso não fornecido."}});
            return;
        }}
        fetch("https://graphql.intelipost.com.br/", {{
            "method": "POST",
            "headers": {{ "Content-Type": "application/json", "Authorization": `Bearer ${{token}}` }},
            "body": JSON.stringify(payload)
        }})
        .then(response => {{
            if (!response.ok) {{ throw new Error(`Erro de rede: ${{response.status}}`); }}
            return response.json();
        }})
        .then(data => callback(data))
        .catch(error => callback({{"js_error": error.toString()}}));
    """
    try:
        response_data = driver.execute_async_script(script, payload, token)
        if response_data and "js_error" in response_data:
            print(f"ERRO (GraphQL via JS): {response_data['js_error']}")
            return None
        if response_data and response_data.get("errors"):
             print(f"ERRO (GraphQL API): {response_data['errors']}")
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

    sysnode_tab = driver.current_window_handle
    
    print("INFO: Forçando abertura do link em uma nova aba, mantendo o método de clique original...")
    actions = ActionChains(driver)
    actions.key_down(Keys.CONTROL) \
           .move_to_element(link_elemento) \
           .pause(0.5) \
           .click_and_hold() \
           .pause(0.2) \
           .release() \
           .key_up(Keys.CONTROL) \
           .perform()

    print("INFO: Aguardando a nova aba da Intelipost ser aberta...")
    wait.until(EC.number_of_windows_to_be(2), "A aba de login da Intelipost não abriu.")
    
    intelipost_tab = [handle for handle in driver.window_handles if handle != sysnode_tab][0]
    driver.switch_to.window(intelipost_tab)
    print("INFO: Foco alterado para a aba da Intelipost.")

    print("INFO: Limpando sessão da aba Intelipost para forçar nova autenticação...")
    driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    driver.refresh()

    print("INFO: Aguardando a aplicação web finalizar a nova autenticação e salvar o token...")
    token_data_json = wait.until(lambda d: d.execute_script("return window.localStorage.getItem('ls.user');"), "Tempo limite atingido esperando pelo token no localStorage.")
    print("SUCESSO: Token de autenticação detectado no localStorage.")
    
    driver.switch_to.window(sysnode_tab)
    driver.close()
    driver.switch_to.window(intelipost_tab)
    print("SUCESSO: Sessão no contexto do cliente estabelecida com sucesso.")
    
    token_data = json.loads(token_data_json)
    captured_token = token_data.get("access_token")
    if not captured_token:
        raise ValueError("access_token não foi encontrado dentro do objeto 'ls.user'.")
    
    return token_data_json, captured_token

def obter_centros_de_distribuicao_api(driver, token: str, monitor=None) -> list:
    if not driver or not token: return []
    query = "{ warehouses { id official_name } }"
    json_response = _execute_graphql_via_selenium(driver, query, {}, token, monitor=monitor)
    if json_response and "data" in json_response:
        warehouses = json_response.get("data", {}).get("warehouses", [])
        print(f"INFO (API): {len(warehouses)} Centros de Distribuição encontrados.")
        return [(wh['id'], wh['official_name']) for wh in warehouses]
    return []

def obter_transportadoras_api(driver, token: str, monitor=None) -> list:
    if not driver or not token: return []
    query = "query ($active: Boolean) { logisticProviders(active: $active) { id name } }"
    variables = {"active": True}
    json_response = _execute_graphql_via_selenium(driver, query, variables, token, monitor=monitor)
    if json_response and "data" in json_response:
        providers = json_response.get("data", {}).get("logisticProviders", [])
        print(f"INFO (API): {len(providers)} Transportadoras encontradas.")
        return [(lp['id'], lp['name']) for lp in providers]
    return []

def obter_pre_faturas_prontas_por_data(driver, token: str, data_inicio_str: str, data_fim_str: str, lista_ids_warehouses: list, lista_ids_transportadoras: list, stop_event: threading.Event, monitor=None) -> list:
    if not driver or not token: return []
    todos_os_itens = []
    
    query_string = """
        query ($warehouses: [Int], $logistic_providers: [Int], $status: [String], $date_range: DateRangeInput, $page: Int, $limit: Int) {
            preInvoicesV2(warehouses: $warehouses, logistic_providers: $logistic_providers, status: $status, date_range: $date_range, page: $page, limit: $limit) {
                total
                items { id, cte { key }, invoice { order_number }, tms_value, cte_value }
            }
        }
    """
    variables = {
        "date_range": {"start": data_inicio_str, "end": data_fim_str},
        "logistic_providers": lista_ids_transportadoras, 
        "warehouses": lista_ids_warehouses,
        "status": ["WAITING_FOR_CONCILIATION"], "page": 1, "limit": 1
    }
    
    try:
        print("INFO (API Paginada): Calculando total de páginas...")
        json_response = _execute_graphql_via_selenium(driver, query_string, variables, token, monitor=monitor)
        if not json_response or "data" not in json_response:
            raise Exception("Resposta inválida da API ao calcular páginas.")
        
        total_itens = json_response["data"].get("preInvoicesV2", {}).get("total", 0)
        limite_por_pagina = 500
        total_paginas = math.ceil(total_itens / limite_por_pagina)
        print(f"INFO (API Paginada): {total_itens} itens encontrados, totalizando {total_paginas} páginas.")
        
        if total_paginas == 0: return []

        variables['limit'] = limite_por_pagina
        for page in range(1, total_paginas + 1):
            if stop_event.is_set(): break
            print(f"INFO (API Paginada): Buscando página {page}/{total_paginas}...")
            variables['page'] = page
            page_response = _execute_graphql_via_selenium(driver, query_string, variables, token, monitor=monitor)
            if page_response and "data" in page_response:
                items_da_pagina = page_response["data"].get("preInvoicesV2", {}).get("items", [])
                todos_os_itens.extend(items_da_pagina)
            else:
                print(f"AVISO (API Paginada): Falha ao buscar a página {page}. Pulando...")
            time.sleep(0.1)
            
    except Exception as e:
        print(f"ERRO (API Paginada): {e}")
        return []
        
    return todos_os_itens

def obter_detalhes_em_lote(driver, token: str, pre_fatura_ids: list[str], monitor=None) -> dict:
    """
    Busca os detalhes de um lote de pré-faturas usando a estratégia de "aliases" do GraphQL,
    que é a forma correta e compatível com a API, fazendo uma única chamada por lote.
    """
    if not driver or not token or not pre_fatura_ids:
        return {}
    
    sub_query_template = """
        values {
            origin_zipcode, destination_zipcode, invoice { number },
            volumes { weight, squared_weight, selected_weight, dimensions { width, height, length } }
        }
    """
    
    query_parts = []
    for i, fatura_id in enumerate(pre_fatura_ids):
        safe_fatura_id = fatura_id.replace('"', '\\"')
        alias = f'fatura_{i}'
        query_parts.append(f'{alias}: preInvoiceDetail(id: "{safe_fatura_id}", action: "values") {{ {sub_query_template} }}')
    
    full_query = f"query {{ {' '.join(query_parts)} }}"
    
    json_response = _execute_graphql_via_selenium(driver, full_query, {}, token, monitor=monitor)

    if json_response and "data" in json_response:
        data = json_response["data"]
        resultados = {}
        for i, fatura_id in enumerate(pre_fatura_ids):
            resultado_alias = data.get(f'fatura_{i}')
            if resultado_alias and resultado_alias.get("values"):
                resultados[fatura_id] = resultado_alias["values"][0]
        return resultados
        
    return {}

def obter_configuracao_margem_api(driver, token: str, monitor=None) -> dict | None:
    if not driver: return None
    query = "{ reconConfig { marginType, marginFixedValue, marginPercentageValue, marginMixedFixedValue, marginMixedPercentageValue } }"
    json_response = _execute_graphql_via_selenium(driver, query, {}, token, monitor=monitor)
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