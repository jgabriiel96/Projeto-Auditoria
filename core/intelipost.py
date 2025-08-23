# core/intelipost.py (Versão Final V2.10 - Gerenciamento de Abas)

import time
import requests
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from core.utils import retry
import threading

@retry(tentativas=3, delay=5)
def preparar_pagina_e_capturar_token(driver, client_id: str) -> str | None:
    # V2.10 - Lógica de gerenciamento de abas para não interferir com o usuário
    original_window = None
    work_window = None
    
    try:
        # V2.10 - Salva a janela/aba original do usuário
        original_window = driver.current_window_handle
        
        # V2.10 - Abre uma nova aba dedicada para o trabalho do robô
        driver.switch_to.new_window('tab')
        work_window = driver.current_window_handle
        print("INFO: Nova aba de trabalho aberta para não interferir na navegação do usuário.")

        wait = WebDriverWait(driver, 60)
        print("INFO: Iniciando preparação para o cliente (login via sysnode)...")
        
        driver.get(f"https://api-sysnode.intelipost.com.br/sysnode/edit_client?q={client_id}")
        
        # A lógica de clique que abre a terceira aba (Intelipost) continua a mesma
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
        
        # V2.10 - O robô fecha a aba intermediária do sysnode que não é mais necessária
        driver.close() 
        
        # V2.10 - E foca na aba final da Intelipost que foi aberta pelo clique
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
        # V2.10 - Bloco de limpeza: fecha a aba da Intelipost e retorna para a aba original do usuário
        if driver:
            # Garante que estamos na aba correta antes de fechar para não fechar a do usuário por engano
            current_handle = driver.current_window_handle
            if current_handle != original_window:
                driver.close()
            
            # Devolve o foco para a janela original do usuário
            if original_window and original_window in driver.window_handles:
                 driver.switch_to.window(original_window)
                 print("INFO: Aba de trabalho fechada e foco retornado ao usuário.")


# O restante do arquivo (funções de API) permanece o mesmo da V2.5
# ... (código de obter_centros_de_distribuicao_api, obter_transportadoras_api, etc. sem alterações)
def obter_centros_de_distribuicao_api(api_token: str) -> list:
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

def _obter_dados_via_api_worker(order_number: str, api_token: str, data_inicio_str: str, data_fim_str: str, lista_ids_warehouses: list, result_container: dict):
    if not api_token:
        result_container['result'] = (None, None)
        return
    graphql_query = {
        "variables": {
            "warehouses": lista_ids_warehouses,
            "date_range": {"start": data_inicio_str, "end": data_fim_str},
            "search_by": "order_number",
            "search_values": [order_number]
        },
        "query": "query ($warehouses: [Int], $date_range: DateRangeInput, $search_by: String!, $search_values: [String]) { preInvoicesV2(warehouses: $warehouses, date_range: $date_range, search_by: $search_by, search_values: $search_values) { items { cte { key } cte_value } } }"
    }
    auth_token = api_token if api_token.lower().startswith('bearer ') else f'Bearer {api_token}'
    headers = {"Authorization": auth_token, "Content-Type": "application/json", "Origin": "https://secure.intelipost.com.br", "Referer": "https://secure.intelipost.com.br/"}
    try:
        response = requests.post("https://graphql.intelipost.com.br/", json=graphql_query, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        items = data.get("data", {}).get("preInvoicesV2", {}).get("items", [])
        if not items:
            print(f"AVISO: Nenhum item retornado pela API para o pedido {order_number} com os filtros atuais.")
            result_container['result'] = (None, None)
            return
        primeiro_item = items[0]
        frete = primeiro_item.get("cte_value")
        chave_cte = primeiro_item.get("cte", {}).get("key")
        if frete is None or not chave_cte:
            print(f"AVISO: Dados de frete ('cte_value') ou chave ausentes na resposta da API para o pedido {order_number}.")
            result_container['result'] = (None, None)
            return
        print(f"SUCESSO (API): Chave: {chave_cte}, Frete: R$ {frete}")
        result_container['result'] = (str(chave_cte), float(frete))
    except Exception as e:
        print(f"ERRO (API): Falha ao buscar dados para o pedido {order_number}. Detalhe: {e}")
        result_container['result'] = (None, None)

def obter_dados_via_api_threaded(order_number: str, api_token: str, data_inicio_str: str, data_fim_str: str, lista_ids_warehouses: list, stop_event: threading.Event) -> tuple[str | None, float | None]:
    result_container = {'result': (None, None)}
    worker_thread = threading.Thread(
        target=_obter_dados_via_api_worker,
        args=(order_number, api_token, data_inicio_str, data_fim_str, lista_ids_warehouses, result_container)
    )
    worker_thread.start()
    while worker_thread.is_alive():
        if stop_event.is_set():
            print(f"INFO: Ignorando chamada de API para o pedido {order_number} devido à solicitação de parada.")
            return None, None
        worker_thread.join(timeout=0.1)
    return result_container['result']

def obter_configuracao_margem_api(api_token: str) -> dict | None:
    if not api_token: return None
    graphql_query = {
        "operationName": None,
        "variables": {},
        "query": "{\n  reconConfig {\n    marginType\n    marginFixedValue\n    marginPercentageValue\n  }\n}\n"
    }
    auth_token = api_token if api_token.lower().startswith('bearer ') else f'Bearer {api_token}'
    headers = {"Authorization": auth_token, "Content-Type": "application/json", "Origin": "https://secure.intelipost.com.br", "Referer": "https://secure.intelipost.com.br/"}
    try:
        print("INFO (API V2): Buscando configuração da margem de tolerância...")
        response = requests.post("https://graphql.intelipost.com.br/", json=graphql_query, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        recon_config = data.get("data", {}).get("reconConfig")
        if not recon_config:
            print("ERRO (API V2): Estrutura 'reconConfig' não encontrada na resposta da API.")
            return None
        margin_type = recon_config.get("marginType")
        config_margem = {}
        if margin_type == "ABSOLUTE":
            config_margem['type'] = "ABSOLUTE"
            config_margem['value'] = recon_config.get("marginFixedValue", 0.0)
            print(f"SUCESSO (API V2): Margem configurada como valor FIXO de R$ {config_margem['value']}.")
        elif margin_type == "PERCENTAGE":
            config_margem['type'] = "PERCENTAGE"
            config_margem['value'] = recon_config.get("marginPercentageValue", 0.0)
            print(f"SUCESSO (API V2): Margem configurada como PERCENTUAL de {config_margem['value']}%.")
        else:
            print(f"AVISO (API V2): Tipo de margem desconhecido ou não definido ('{margin_type}').")
            return None
        return config_margem
    except Exception as e:
        print(f"ERRO (API V2): Falha ao buscar configuração da margem. Detalhe: {e}")
        return None