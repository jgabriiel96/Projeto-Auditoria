# core/intelipost.py

import time
import requests
import math
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from core.utils import retry
import threading

# ... (funções preparar_pagina_e_capturar_token, obter_centros_de_distribuicao_api, obter_transportadoras_api permanecem inalteradas) ...
@retry(tentativas=3, delay=5)
def preparar_pagina_e_capturar_token(driver, client_id: str) -> str | None:
    # (Código inalterado)
    original_window = None
    try:
        original_window = driver.current_window_handle
        driver.switch_to.new_window('tab')
        print("INFO: Nova aba de trabalho aberta para não interferir na navegação do usuário.")
        wait = WebDriverWait(driver, 60)
        print("INFO: Iniciando preparação para o cliente (login via sysnode)...")
        driver.get(f"https://api-sysnode.intelipost.com.br/sysnode/edit_client?q={client_id}")
        abas_antes_do_clique = set(driver.window_handles)
        print("INFO: Clicando no link de e-mail para ativar a sessão...")
        email_login_link_xpath = f"//td[normalize-space()='{client_id}']/following-sibling::td[1]/a"
        print("INFO: Aguardando visibilidade do link de login...")
        link_elemento = wait.until(EC.visibility_of_element_located((By.XPATH, email_login_link_xpath)))
        print("INFO: Rolando a página para garantir que o link esteja visível...")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", link_elemento)
        time.sleep(1)
        print("INFO: Utilizando ActionChains para simular um clique humano detalhado...")
        actions = ActionChains(driver)
        actions.move_to_element(link_elemento).pause(0.5).click_and_hold().pause(0.2).release().perform()
        print("INFO: Aguardando a nova aba ser aberta...")
        wait.until(lambda d: len(set(d.window_handles) - abas_antes_do_clique) > 0)
        nova_aba_intelipost = (set(driver.window_handles) - abas_antes_do_clique).pop()
        current_handle_before_switch = driver.current_window_handle
        if current_handle_before_switch in abas_antes_do_clique:
            driver.close()
        driver.switch_to.window(nova_aba_intelipost)
        print("INFO: Foco alterado para a aba da Intelipost.")
        print("INFO: Aguardando a página 'Welcome' ou Dashboard carregar completamente...")
        wait.until(EC.visibility_of_element_located((By.XPATH, "//*[contains(text(), 'Home') or contains(text(), 'Bem-vindo') or contains(@id, 'dashboard')]")))
        print("SUCESSO: Página 'Welcome' carregada. A sessão foi estabelecida.")
        url_alvo = "https://secure.intelipost.com.br/recon/pre-invoice-generation"
        print(f"INFO: Navegando diretamente para a página de Geração de Pré-fatura: {url_alvo}")
        driver.get(url_alvo)
        wait.until(EC.visibility_of_element_located((By.ID, "pre-invoice-search-button")))
        print(f"SUCESSO: Sessão no contexto do cliente {client_id} foi estabelecida com sucesso.")
        print("INFO: Capturando token de autorização da sessão...")
        time.sleep(3)
        script_busca_token_final = """
            try {
                const userItem = window.localStorage.getItem('ls.user');
                if (userItem) {
                    const parsedItem = JSON.parse(userItem);
                    return parsedItem.access_token;
                }
                return null;
            } catch (e) {
                return null;
            }
        """
        token = driver.execute_script(script_busca_token_final)
        if token:
            auth_header = f"Bearer {token}"
            print("SUCESSO: Token de autorização JWT encontrado e capturado com sucesso.")
            print(f"DEBUG: Token capturado: {token[:30]}...")
            return auth_header
        else:
            print("ERRO CRÍTICO: Não foi possível encontrar o token JWT na chave 'ls.user' do localStorage.")
            driver.save_screenshot('erro_token_final.png')
            return None
    except Exception as e:
        print(f"ERRO: Falha ao preparar a sessão e capturar o token. Detalhe: {e}")
        driver.save_screenshot('erro_login_final.png')
        raise e
    finally:
        if driver:
            current_handle = driver.current_window_handle
            if original_window and original_window in driver.window_handles and current_handle != original_window:
                driver.close()
                driver.switch_to.window(original_window)
                print("INFO: Aba de trabalho fechada e foco retornado ao usuário.")

def obter_centros_de_distribuicao_api(api_token: str) -> list:
    # (Código inalterado)
    if not api_token: return []
    graphql_query = { "query": "{ warehouses { id official_name } }" }
    auth_token = api_token if api_token.lower().startswith('bearer ') else f'Bearer {api_token}'
    headers = {"Authorization": auth_token, "Content-Type": "application/json", "Origin": "https://secure.intelipost.com.br", "Referer": "https://secure.intelipost.com.br/"}
    try:
        response = requests.post("https://graphql.intelipost.com.br/", json=graphql_query, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        warehouses = data.get("data", {}).get("warehouses", [])
        print(f"INFO (API): {len(warehouses)} Centros de Distribuição encontrados.")
        return [(wh['id'], wh['official_name']) for wh in warehouses]
    except Exception as e:
        print(f"ERRO (API): Falha ao buscar Centros de Distribuição. Detalhe: {e}")
        return []

def obter_transportadoras_api(api_token: str) -> list:
    # (Código inalterado)
    if not api_token: return []
    graphql_query = {
        "variables": {"active": False},
        "query": "query ($active: Boolean) { logisticProviders(active: $active) { id name } }"
    }
    auth_token = api_token if api_token.lower().startswith('bearer ') else f'Bearer {api_token}'
    headers = {"Authorization": auth_token, "Content-Type": "application/json", "Origin": "https://secure.intelipost.com.br", "Referer": "https://secure.intelipost.com.br/"}
    try:
        response = requests.post("https://graphql.intelipost.com.br/", json=graphql_query, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        providers = data.get("data", {}).get("logisticProviders", [])
        print(f"INFO (API): {len(providers)} Transportadoras encontradas.")
        return [(lp['id'], lp['name']) for lp in providers]
    except Exception as e:
        print(f"ERRO (API): Falha ao buscar Transportadoras. Detalhe: {e}")
        return []

def obter_pre_faturas_prontas_por_data(
    api_token: str,
    data_inicio_str: str,
    data_fim_str: str,
    lista_ids_warehouses: list,
    lista_ids_transportadoras: list,
    stop_event: threading.Event
) -> list:
    if not api_token:
        return []

    todos_os_itens = []
    pagina_atual = 1
    limite_por_pagina = 500
    total_paginas = 0
    
    query_string = """
        query ($warehouses: [Int], $logistic_providers: [Int], $date_range: DateRangeInput, $page: Int, $limit: Int, $status: [String]) {
            preInvoicesV2(
                warehouses: $warehouses, logistic_providers: $logistic_providers,
                date_range: $date_range, page: $page, limit: $limit, status: $status
            ) {
                total
                hasNextPage
                items {
                    cte { key }
                    invoice { order_number }
                    cte_value
                    tms_value 
                    status
                }
            }
        }
    """
    
    auth_token = api_token if api_token.lower().startswith('bearer ') else f'Bearer {api_token}'
    headers = {"Authorization": auth_token, "Content-Type": "application/json", "Origin": "https://secure.intelipost.com.br", "Referer": "https://secure.intelipost.com.br/"}

    try:
        print("INFO (API Paginada): Calculando total de páginas...")

        variables = {
            "warehouses": lista_ids_warehouses, 
            "logistic_providers": lista_ids_transportadoras,
            "date_range": {"start": data_inicio_str, "end": data_fim_str},
            "status": ["AUDITABLE", "WAITING_FOR_CONCILIATION"],
            "page": 1, 
            "limit": 1
        }
        
        response = requests.post("https://graphql.intelipost.com.br/", json={"query": query_string, "variables": variables}, headers=headers, timeout=180)
        response.raise_for_status()
        json_response = response.json()
        
        if "errors" in json_response and json_response["errors"]:
             print(f"ERRO (API Paginada): A API GraphQL retornou erros: {json_response['errors']}")
             return []

        data = json_response.get("data", {}).get("preInvoicesV2", {})
        total_itens = data.get("total", 0)

        if total_itens > 0:
            total_paginas = math.ceil(total_itens / limite_por_pagina)
        print(f"INFO (API Paginada): {total_itens} itens encontrados, totalizando {total_paginas} páginas.")
    
    except requests.exceptions.HTTPError as http_err:
        print(f"ERRO HTTP (API Paginada): Falha ao pré-calcular páginas. Status: {http_err.response.status_code}.")
        try:
            print(f"Detalhe do erro da API: {http_err.response.json()}")
        except Exception:
            print(f"Conteúdo da resposta (não-JSON): {http_err.response.text}")
        return []
    except Exception as e:
        print(f"ERRO (API Paginada): Não foi possível pré-calcular o total de páginas. Detalhe: {e}")
        return []

    if total_paginas == 0:
        return []

    while pagina_atual <= total_paginas:
        if stop_event.is_set():
            print("INFO (API Paginada): Solicitação de parada recebida. Interrompendo coleta.")
            break
            
        try:
            print(f"INFO (API Paginada): Buscando página {pagina_atual}/{total_paginas} (limite de {limite_por_pagina} itens)...")
            
            variables['page'] = pagina_atual
            variables['limit'] = limite_por_pagina
            
            response = requests.post("https://graphql.intelipost.com.br/", json={"query": query_string, "variables": variables}, headers=headers, timeout=180)
            response.raise_for_status()
            data = response.json().get("data", {}).get("preInvoicesV2", {})
            
            items_da_pagina = data.get("items", [])
            if items_da_pagina:
                todos_os_itens.extend(items_da_pagina)
            
            if not data.get("hasNextPage", False):
                print("INFO (API Paginada): Fim da paginação. Todas as pré-faturas foram coletadas.")
                break

            pagina_atual += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"ERRO (API Paginada): Falha ao buscar a página {pagina_atual}. Detalhe: {e}")
            break

    return todos_os_itens

def obter_configuracao_margem_api(api_token: str) -> dict | None:
    # (Código inalterado)
    if not api_token: return None
    graphql_query = {
        "operationName": None,
        "variables": {},
        "query": """{
            reconConfig {
                marginType
                marginFixedValue
                marginPercentageValue
                marginMixedFixedValue
                marginMixedPercentageValue
            }
        }"""
    }
    auth_token = api_token if api_token.lower().startswith('bearer ') else f'Bearer {api_token}'
    headers = {"Authorization": auth_token, "Content-Type": "application/json", "Origin": "https://secure.intelipost.com.br", "Referer": "https://secure.intelipost.com.br/"}
    try:
        print("INFO (API): Buscando configuração da margem de tolerância...")
        response = requests.post("https://graphql.intelipost.com.br/", json=graphql_query, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        recon_config = data.get("data", {}).get("reconConfig")
        if recon_config is None:
            print("ERRO (API): Estrutura 'reconConfig' não encontrada ou nula na resposta da API.")
            return None
        margin_type = recon_config.get("marginType")
        config_margem = {}
        if margin_type is None:
            config_margem['type'] = "SYSTEM_DEFAULT"
            print("SUCESSO (API): Margem configurada como Padrão do Sistema (1%), pois 'marginType' é nulo.")
        elif margin_type == "ABSOLUTE":
            config_margem['type'] = "ABSOLUTE"
            config_margem['value'] = recon_config.get("marginFixedValue", 0.0)
            print(f"SUCESSO (API): Margem configurada como valor FIXO de R$ {config_margem['value']}.")
        elif margin_type == "PERCENTAGE":
            config_margem['type'] = "PERCENTAGE"
            config_margem['value'] = recon_config.get("marginPercentageValue", 0.0)
            print(f"SUCESSO (API): Margem configurada como PERCENTUAL de {config_margem['value']}%.")
        elif margin_type == "MIXED_GREATER":
            absolute_val = recon_config.get("marginMixedFixedValue", 0.0)
            percentage_val = recon_config.get("marginMixedPercentageValue", 0.0)
            config_margem = {
                'type': 'DYNAMIC_CHOICE',
                'absolute_value': absolute_val,
                'percentage_value': percentage_val
            }
            print(f"SUCESSO (API): Margem configurada como Dinâmica (Maior entre R$ {absolute_val} e {percentage_val}%).")
        else:
            print(f"AVISO (API): Tipo de margem desconhecido ou não suportado ('{margin_type}').")
            return None
        return config_margem
    except Exception as e:
        print(f"ERRO (API): Falha ao buscar configuração da margem. Detalhe: {e}")
        return None