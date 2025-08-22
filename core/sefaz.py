# core/sefaz.py (com Busca em Múltiplas Abas)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random

def consultar_dados_sefaz(chave_acesso: str) -> dict:
    dados_sefaz = {'valor': None}
    driver = None 
    
    try:
        print("INFO: Iniciando navegador Brave 'fantasma' para a SEFAZ...")
        options = uc.ChromeOptions()
        caminho_executavel_brave = r"C:\Users\joao.gabriel_intelip\AppData\Local\BraveSoftware\Brave-Browser\Application\brave.exe"
        options.binary_location = caminho_executavel_brave
        options.headless = False

        driver = uc.Chrome(options=options, use_subprocess=True)
        driver.set_window_size(1280, 800)
        
        url_consulta = "https://www.cte.fazenda.gov.br/portal/consultaRecaptcha.aspx?tipoConsulta=resumo&tipoConteudo=cktLvUUKqh0%3D"
        driver.get(url_consulta)
        
        # --- ETAPA 1: ROBÔ PREPARA O TERRENO ---
        ID_DO_CAMPO_CHAVE = "ctl00_ContentPlaceHolder1_txtChaveAcessoResumo"
        wait = WebDriverWait(driver, 30)
        campo_chave = wait.until(EC.presence_of_element_located((By.ID, ID_DO_CAMPO_CHAVE)))
        
        print("INFO: Digitanto a chave de acesso...")
        for caractere in chave_acesso:
            campo_chave.send_keys(caractere)
            time.sleep(random.uniform(0.05, 0.1))

        # --- ETAPA 2: VOCÊ ASSUME O CONTROLE TOTAL ---
        print("\n" + "="*50)
        print(">>> SUA VEZ, MESTRE! <<<")
        print("A chave foi digitada. Agora é com você:")
        print("1. Tente o CAPTCHA (vai falhar).")
        print("2. DUPLIQUE A ABA.")
        print("3. Na nova aba, resolva o CAPTCHA e clique em 'Continuar'.")
        print("-" * 50)
        print("O robô está esperando pacientemente pela página de resultados...")
        print("="*50 + "\n")

        # --- ETAPA 3: ROBÔ PROCURA O RESULTADO EM TODAS AS ABAS ---
        XPATH_DO_CAMPO_VALOR_FINAL = "//*[@id='conteudoDinamico']/div[3]/div[1]/div[5]/div/p"
        
        resultado_encontrado = False
        start_time = time.time()
        
        # O robô tentará encontrar o resultado por até 5 minutos
        while time.time() - start_time < 300:
            # Loop para verificar todas as abas abertas
            for handle in driver.window_handles:
                driver.switch_to.window(handle)
                print(f"INFO: Verificando a aba com título: '{driver.title}'...")
                try:
                    # Tenta encontrar o elemento na aba atual, com um tempo de espera curto (1s)
                    elemento_valor = WebDriverWait(driver, 1).until(EC.visibility_of_element_located((By.XPATH, XPATH_DO_CAMPO_VALOR_FINAL)))
                    
                    print("INFO: SUCESSO! Página de resultado encontrada nesta aba!")
                    valor_total_str = elemento_valor.text
                    valor_formatado = valor_total_str.split(':')[-1].replace('R$', '').strip()
                    valor_numerico = valor_formatado.replace('.', '').replace(',', '.')
                    dados_sefaz['valor'] = float(valor_numerico)
                    print(f"INFO: Leitura do valor do CT-e OK. Valor: R$ {dados_sefaz['valor']}")
                    resultado_encontrado = True
                    break # Sai do loop de abas
                except Exception:
                    # Se não encontrou, continua para a próxima aba silenciosamente
                    continue
            if resultado_encontrado:
                break # Sai do loop de tempo
            
            time.sleep(5) # Espera 5 segundos antes de verificar as abas novamente

        if not resultado_encontrado:
            print("ERRO: O tempo limite de 5 minutos foi atingido e o robô não encontrou a página de resultados em nenhuma aba.")

    except Exception as e:
        print(f"ERRO: Falha crítica durante a consulta na SEFAZ. Detalhe: {e}")
        driver.save_screenshot('erro_sefaz.png')
    finally:
        if driver:
            print("INFO: Fechando navegador da SEFAZ...")
            driver.quit()
            
    return dados_sefaz