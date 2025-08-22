import time
import requests
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from core.utils import retry

@retry(tentativas=3, delay=5)
def preparar_pagina_e_capturar_token(driver, client_id: str) -> str | None:
    """
    Automatiza o login na sessão de um cliente específico e captura o token de
    autenticação JWT (JSON Web Token) do localStorage do navegador.

    Esta função utiliza a estratégia de um clique 'super-humano' para evitar
    sistemas de detecção de bots e adiciona uma espera estratégica por uma página
    intermediária ('Welcome') para garantir que a sessão seja totalmente estabelecida.

    Args:
        driver: A instância do WebDriver do Selenium.
        client_id (str): O ID do cliente que será usado para o login.

    Returns:
        str | None: O cabeçalho de autorização completo ("Bearer <token>") ou None se falhar.
    """
    wait = WebDriverWait(driver, 60)
    print("INFO: Iniciando preparação para o cliente (login via sysnode)...")
    try:
        driver.get(f"https://api-sysnode.intelipost.com.br/sysnode/edit_client?q={client_id}")
        abas_antes_do_clique = set(driver.window_handles)
        print("INFO: Clicando no link de e-mail para ativar a sessão...")
        email_login_link_xpath = f"//td[normalize-space()='{client_id}']/following-sibling::td[1]/a"

        # --- PARTE 1: CLIQUE ---
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
        nova_aba = (set(driver.window_handles) - abas_antes_do_clique).pop()
        driver.switch_to.window(nova_aba)
        print("INFO: Foco alterado para a nova aba.")

        # --- PARTE 2: ESPERA PELA 'WELCOME PAGE' (ETAPA NECESSÁRIA) ---
        print("INFO: Aguardando a página 'Welcome' ou Dashboard carregar completamente...")
        wait.until(EC.visibility_of_element_located((By.XPATH, "//*[contains(text(), 'Home') or contains(text(), 'Bem-vindo') or contains(@id, 'dashboard')]")))
        print("SUCESSO: Página 'Welcome' carregada. A sessão foi estabelecida.")
        
        url_alvo = "https://secure.intelipost.com.br/recon/pre-invoice-generation"
        print(f"INFO: Navegando diretamente para a página de Geração de Pré-fatura: {url_alvo}")
        driver.get(url_alvo)
        wait.until(EC.visibility_of_element_located((By.ID, "pre-invoice-search-button")))
        print(f"SUCESSO: Sessão no contexto do cliente {client_id} foi estabelecida com sucesso.")
        
        print("INFO: Capturando token de autorização da sessão...")
        time.sleep(3) # Pausa final para garantir que o JS escreva o token

        # --- SCRIPT DEFINITIVO PARA CAPTURAR O TOKEN CORRETO ---
        script_busca_token_final = """
            try {
                // Pega o item da chave 'ls.user'
                const userItem = window.localStorage.getItem('ls.user');
                if (userItem) {
                    // Converte a string JSON em um objeto
                    const parsedItem = JSON.parse(userItem);
                    // Retorna o access_token de dentro do objeto
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
            print(f"DEBUG: Token capturado: {token[:30]}...") # Imprime o início do token para confirmação
            return auth_header
        else:
            print("ERRO CRÍTICO: Não foi possível encontrar o token JWT na chave 'ls.user' do localStorage.")
            driver.save_screenshot('erro_token_final.png')
            return None

    except Exception as e:
        print(f"ERRO: Falha ao preparar a sessão e capturar o token. Detalhe: {e}")
        driver.save_screenshot('erro_login_final.png')
        raise e

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

def obter_dados_via_api(order_number: str, api_token: str, data_inicio_str: str, data_fim_str: str, lista_ids_warehouses: list) -> tuple[str | None, float | None]:
    if not api_token: return None, None
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
        response = requests.post("https://graphql.intelipost.com.br/", json=graphql_query, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        items = data.get("data", {}).get("preInvoicesV2", {}).get("items", [])
        if not items:
            print(f"AVISO: Nenhum item retornado pela API para o pedido {order_number} com os filtros atuais.")
            return None, None
        primeiro_item = items[0]
        frete = primeiro_item.get("cte_value")
        chave_cte = primeiro_item.get("cte", {}).get("key")
        if frete is None or not chave_cte:
            print(f"AVISO: Dados de frete ('cte_value') ou chave ausentes na resposta da API para o pedido {order_number}.")
            return None, None
        print(f"SUCESSO (API): Chave: {chave_cte}, Frete: R$ {frete}")
        return str(chave_cte), float(frete)
    except Exception as e:
        print(f"ERRO (API): Falha ao buscar dados para o pedido {order_number}. Detalhe: {e}")
        return None, None