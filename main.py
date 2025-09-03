# main.py

import interface_usuario as gui
from core import database, comparator, sheets, intelipost
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
# REMOVIDO: Não precisamos mais do webdriver_manager
# from webdriver_manager.chrome import ChromeDriverManager 
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import tkinter as tk
from tkinter import messagebox
import sys
import threading
import queue
import configparser
import time
import os
import subprocess
import psutil
import socket
import pandas as pd
import traceback
import json

class Logger:
    def __init__(self, queue_gui, terminal_original):
        self.queue = queue_gui
        self.terminal = terminal_original
    def write(self, message):
        self.terminal.write(message)
        self.queue.put(message)
    def flush(self):
        self.terminal.flush()

class PerformanceMonitor:
    def __init__(self):
        self.api_call_count = 0
        self.lock = threading.Lock()
    def increment_api_call(self):
        with self.lock:
            self.api_call_count += 1
    def get_count(self):
        with self.lock:
            return self.api_call_count

def _get_browser_paths():
    paths = {'chrome_exec': '', 'chrome_user_data': '', 'brave_exec': '', 'brave_user_data': ''}
    if sys.platform == 'win32':
        local_app_data = os.getenv('LOCALAPPDATA', '')
        prog_files = [os.getenv('ProgramFiles(x86)'), os.getenv('ProgramFiles')]
        paths['chrome_user_data'] = os.path.join(local_app_data, 'Google', 'Chrome', 'User Data')
        for prog_file in prog_files:
            if prog_file:
                chrome_path = os.path.join(prog_file, 'Google', 'Chrome', 'Application', 'chrome.exe')
                if os.path.isfile(chrome_path):
                    paths['chrome_exec'] = chrome_path
                    break
        paths['brave_user_data'] = os.path.join(local_app_data, 'BraveSoftware', 'Brave-Browser', 'User Data')
        for prog_file in prog_files:
            if prog_file:
                brave_path = os.path.join(prog_file, 'BraveSoftware', 'Brave-Browser', 'Application', 'brave.exe')
                if os.path.isfile(brave_path):
                    paths['brave_exec'] = brave_path
                    break
    return paths

def is_port_in_use(port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def kill_browser_processes(exec_name: str):
    for proc in psutil.process_iter(['name', 'exe']):
        if proc.info['name'] == exec_name:
            try:
                proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

def carregar_filtros_thread(queue_gui, client_id):
    driver = None
    monitor = PerformanceMonitor()
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        caminho_executavel = config.get('BROWSER', 'caminho_executavel', fallback=None)
        # O config.ini original não tinha remote_debugging_port, adicionando fallback seguro
        debug_port_str = config.get('BROWSER', 'remote_debugging_port', fallback='9222')
        debug_port = int(debug_port_str)
        headless_mode = config.getboolean('AUTOMATION', 'headless', fallback=False)
        paths = _get_browser_paths()
        if not caminho_executavel:
            caminho_executavel = paths.get('brave_exec') or paths.get('chrome_exec')
        if not caminho_executavel or not os.path.isfile(caminho_executavel):
            raise FileNotFoundError("Não foi possível localizar o navegador.")
        exec_name = os.path.basename(caminho_executavel)
        user_data_dir = paths.get('brave_user_data') if 'brave' in exec_name.lower() else paths.get('chrome_user_data')
        if not os.path.isdir(user_data_dir):
            raise FileNotFoundError(f"Não foi possível localizar o diretório do perfil: {user_data_dir}")
        
        if not is_port_in_use(debug_port):
            is_running = any(p.name() == exec_name for p in psutil.process_iter(['name']))
            if is_running:
                messagebox.showwarning("Reinicialização Necessária", f"O {exec_name} já está em execução.\n\nPara que a automação funcione, o robô precisa reiniciá-lo em um modo especial.\n\nPor favor, salve seu trabalho e clique em OK.")
                kill_browser_processes(exec_name)
                time.sleep(2)
            command = [caminho_executavel, f'--remote-debugging-port={debug_port}', f'--user-data-dir={user_data_dir}']
            if headless_mode:
                command.extend(['--headless', '--window-size=1920,1080'])
            subprocess.Popen(command)
            time.sleep(5)
        
        options = ChromeOptions()
        options.binary_location = caminho_executavel
        options.add_experimental_option("debuggerAddress", f"127.0.0.1:{debug_port}")
        
        # *** A ÚNICA MUDANÇA ESTÁ AQUI ***
        # Deixamos o Selenium gerenciar o download do driver correto, resolvendo o erro de versão.
        servico = Service() 
        
        driver = webdriver.Chrome(service=servico, options=options)
        print("SUCESSO: Robô conectado com sucesso ao navegador!")
        
        token_data_json, captured_token = intelipost.preparar_pagina_e_capturar_token(driver, str(client_id))
        
        warehouses = intelipost.obter_centros_de_distribuicao_api(driver, captured_token, monitor=monitor)
        carriers = intelipost.obter_transportadoras_api(driver, captured_token, monitor=monitor)
        margin_config = intelipost.obter_configuracao_margem_api(driver, captured_token, monitor=monitor)
        
        print(f"\n--- LOG DE PERFORMANCE (FILTROS) ---")
        print(f"Total de requisições GraphQL nesta etapa: {monitor.get_count()}")
        print(f"-------------------------------------\n")
        
        queue_gui.put({
            "type": "filters_loaded", "driver": driver, "token": captured_token,
            "warehouses": warehouses, "carriers": carriers
        })
        if margin_config:
            queue_gui.put({"type": "margin_info", "config": margin_config})
        else:
            queue_gui.put({"type": "margin_info", "config": {}})

    except Exception as e:
        print(f"\nERRO: {e}")
        traceback.print_exc()
        queue_gui.put({"type": "error", "title": "Erro na Preparação", "message": f"Ocorreu uma falha inesperada:\n\n{e}"})

# TODO: Adicione o resto do seu código original (save_report_thread, executar_auditoria_thread, __main__, etc.) aqui.
# Eles não precisam de nenhuma alteração.
def save_report_thread(queue_gui, data, final=True):
    try:
        lista_divergencias, client_id, start_date, end_date, total_pedidos, duration_seconds = data
        config = configparser.ConfigParser()
        config.read('config.ini')
        recipient_email = config['SHEETS']['email_destinatario']
        sheet_name = f"Auditoria Frete - Cliente {client_id}"
        
        spreadsheet_url = sheets.reportar_divergencias(lista_divergencias, sheet_name, client_id, start_date, end_date, recipient_email)
        
        if spreadsheet_url:
            df_divergencias = pd.DataFrame(lista_divergencias)
            gspread_client = sheets.get_sheets_client()
            spreadsheet = gspread_client.open_by_url(spreadsheet_url)
            sheets.criar_aba_sumario(spreadsheet, df_divergencias, total_pedidos)
            
            formatted_duration = time.strftime("%M minutos e %S segundos", time.gmtime(duration_seconds))
            message = (f'{len(lista_divergencias)} divergências encontradas e reportadas.\n\nTempo total da auditoria: {formatted_duration}\n\nDeseja abrir a planilha no navegador?')
            title = "Processo Finalizado!" if final else "Relatório Salvo"
            
            queue_gui.put({"type": "ask_open_sheet", "title": title, "message": message, "url": spreadsheet_url, "done": True})
        else:
            queue_gui.put({"type": "error", "title": "Erro no Relatório", "message": "Falha ao criar ou atualizar a planilha.", "done": True})
    except Exception as e:
        print(f"\nERRO NA THREAD DE SALVAMENTO: {e}")
        queue_gui.put({"type": "error", "title": "Erro ao Salvar", "message": f"Falha ao salvar o relatório:\n\n{e}", "done": True})

def executar_auditoria_thread(queue_gui, client_id, data_inicio, data_fim, lista_ids_transportadoras, lista_ids_warehouses, driver, token, stop_event, q_control):
    start_time = time.time()
    lista_final_divergencias = []
    df_aggregated = pd.DataFrame()
    monitor = PerformanceMonitor()
    try:
        config_margem = intelipost.obter_configuracao_margem_api(driver, token, monitor=monitor)
        if not config_margem:
            raise ValueError("Não foi possível obter a configuração da margem.")
        
        print("\nINFO: Etapa 1/3 - Buscando lista de pré-faturas na API...")
        pre_faturas_api = intelipost.obter_pre_faturas_prontas_por_data(driver, token, data_inicio, data_fim, lista_ids_transportadoras, lista_ids_warehouses, stop_event, monitor=monitor)

        if stop_event.is_set(): raise InterruptedError("Processo interrompido.")
        if not pre_faturas_api:
            queue_gui.put({"type": "info", "title": "Aviso", "message": "Nenhuma pré-fatura foi encontrada na Intelipost para os filtros selecionados.", "done": True})
            return

        print(f"INFO: {len(pre_faturas_api)} pré-faturas encontradas. Etapa 2/3 - Enriqueçendo dados com detalhes...")
        ids_para_buscar = [item.get("id") for item in pre_faturas_api if item]
        
        chunk_size = 25
        
        lotes_de_ids = [ids_para_buscar[i:i + chunk_size] for i in range(0, len(ids_para_buscar), chunk_size)]
        detalhes_completos = {}
        total_lotes = len(lotes_de_ids)

        for i, lote in enumerate(lotes_de_ids):
            if stop_event.is_set(): break
            
            progress_label = f"Enriquecendo dados: Lote {i+1}/{total_lotes}"
            queue_gui.put({"type": "progress_update", "current": i, "total": total_lotes, "label": progress_label})
            
            resultado_lote = intelipost.obter_detalhes_em_lote(driver, token, lote, monitor=monitor)
            if resultado_lote:
                detalhes_completos.update(resultado_lote)
            else:
                print(f"AVISO: Lote {i+1}/{total_lotes} não retornou dados. Pode haver falha parcial na API.")
            time.sleep(0.2)

        queue_gui.put({"type": "progress_update", "current": total_lotes, "total": total_lotes, "label": "Dados enriquecidos."})
        
        dados_api_list = []
        for item in pre_faturas_api:
            if not item: continue
            detalhes = detalhes_completos.get(item.get("id"))
            if item.get("invoice") and len(item["invoice"]) > 0 and detalhes:
                order_number = item["invoice"][0].get("order_number")
                if order_number:
                    total_squared_weight, total_selected_weight = 0, 0
                    dimensions_list = []
                    for volume in detalhes.get("volumes", []):
                        total_squared_weight += volume.get("squared_weight", 0) or 0
                        total_selected_weight += volume.get("selected_weight", 0) or 0
                        dims = volume.get("dimensions", {})
                        dimensions_list.append(f"{dims.get('length', 0)}x{dims.get('width', 0)}x{dims.get('height', 0)}")
                    dados_api_list.append({
                        "so_order_number": order_number, "cte_value": item.get("cte_value"),
                        "tms_value": item.get("tms_value"), "cte": item.get("cte"),
                        "api_peso_cubado": total_squared_weight, "api_peso_cobrado": total_selected_weight,
                        "api_dimensoes": " | ".join(dimensions_list)
                    })
        df_api = pd.DataFrame(dados_api_list)
        if df_api.empty:
            raise ValueError("Nenhum dado detalhado pôde ser obtido da API após o enriquecimento.")

        print(f"INFO: Etapa 3/3 - Cruzando informações com o banco de dados...")
        df_pedidos_db = database.obter_dados_de_pedidos_especificos(client_id, df_api["so_order_number"].unique().tolist())
        if df_pedidos_db.empty:
            raise ValueError("Nenhum dos pedidos corresponde a um pedido no banco de dados.")

        print("INFO: Unindo dados da API e do Banco de Dados...")
        df_pedidos_db['so_order_number'] = df_pedidos_db['so_order_number'].astype(str)
        df_api['so_order_number'] = df_api['so_order_number'].astype(str)
        df_merged = pd.merge(df_pedidos_db, df_api, on="so_order_number", how="inner")
        
        df_merged['so_provider_shipping_costs'] = df_merged['so_provider_shipping_costs'].fillna(df_merged['tms_value'])
        if df_merged.empty:
            raise ValueError("Falha ao unir os dados da API e do banco de dados.")

        print("INFO: Agregando dados de volumes por pedido para análise consolidada...")
        df_merged['config_margem'] = df_merged.apply(lambda row: config_margem, axis=1)

        df_aggregated = df_merged.groupby('so_order_number').agg(
            soma_peso_declarado=('db_peso_declarado', 'sum'),
            numeros_volumes=('db_numero_volume', lambda x: ' | '.join(sorted(x.astype(str)))),
            config_margem=('config_margem', 'first'),
            valor_intelipost=('cte_value', 'first'),
            so_provider_shipping_costs=('so_provider_shipping_costs', 'first'),
            api_peso_cubado=('api_peso_cubado', 'first'),
            api_peso_cobrado=('api_peso_cobrado', 'first'),
            chave_cte=('cte', lambda x: x.iloc[0].get('key') if x.iloc[0] and x.iloc[0].get('key') else ''),
            lp_name=('lp_name', 'first'),
            db_canal_venda=('db_canal_venda', 'first'),
            db_pedido_canal_venda=('db_pedido_canal_venda', 'first'),
            nota_fiscal=('nota_fiscal_db', 'first'),
            cep_origem_db=('cep_origem_db', 'first'),
            cep_destino_db=('cep_destino_db', 'first'),
            db_cidade_destino=('db_cidade_destino', 'first'),
            api_dimensoes=('api_dimensoes', 'first')
        ).reset_index()

        print(f"INFO: {len(df_aggregated)} pedidos únicos prontos para comparação. Iniciando processamento...")
        queue_gui.put({"type": "progress_update", "current": 0, "total": len(df_aggregated), "label": "Analisando divergências..."})
        
        resultados = df_aggregated.apply(comparator.encontrar_divergencias, axis=1)
        
        lista_final_divergencias = [item for sublist in resultados.dropna() for item in sublist]
        
        queue_gui.put({"type": "progress_update", "current": len(df_aggregated), "total": len(df_aggregated), "label": "Análise finalizada."})
        print(f"SUCESSO: Comparação concluída. {len(lista_final_divergencias)} divergências encontradas.")

    except InterruptedError:
        print("\nINFO: Processo interrompido pelo usuário.")
    except Exception as e:
        print(f"\nERRO CRÍTICO NA THREAD DE AUDITORIA: {e}")
        traceback.print_exc()
        queue_gui.put({"type": "error", "title": "Erro Crítico!", "message": f'Erro inesperado:\n\n{e}', "done": True})
        return
    finally:
        if driver:
            pass
        
        print(f"\n--- LOG DE PERFORMANCE (AUDITORIA) ---")
        print(f"Total de requisições GraphQL na auditoria: {monitor.get_count()}")
        print(f"----------------------------------------\n")
        
        duration_seconds = time.time() - start_time
        total_pedidos_auditados = len(df_aggregated) if not df_aggregated.empty else 0
        data_para_salvar = (lista_final_divergencias, client_id, data_inicio, data_fim, total_pedidos_auditados, duration_seconds)
        
        if stop_event.is_set():
            if lista_final_divergencias:
                queue_gui.put({"type": "ask_save", "data": data_para_salvar})
            else:
                q_control.put({"action": "finish_stop"})
        else:
            q_control.put({"action": "save_report", "data": data_para_salvar})
        if not stop_event.is_set():
            print("\nPROCESSO DE AUDITORIA FINALIZADO.")

if __name__ == "__main__":
    q_gui = queue.Queue()
    q_control = queue.Queue()
    stop_event = threading.Event()
    root = tk.Tk()
    app = gui.App(root, q_gui, q_control)
    sys.stdout = Logger(q_gui, sys.__stdout__)
    automation_thread = None
    
    def on_closing():
        if app.driver:
            print("INFO: Desconectando do navegador...")
        root.destroy()
    root.protocol("WM_DELETE_WINDOW", on_closing)

    def control_queue_monitor(gui_q=q_gui, ctrl_q=q_control):
        global automation_thread
        try:
            message = ctrl_q.get_nowait()
            action = message.get("action")
            
            if action == "load_filters":
                threading.Thread(target=carregar_filtros_thread, args=(gui_q, message["client_id"]), daemon=True).start()

            elif action == "start":
                if app.driver and app.captured_token:
                    if not automation_thread or not automation_thread.is_alive():
                        stop_event.clear()
                        automation_thread = threading.Thread(
                            target=executar_auditoria_thread,
                            args=(
                                gui_q, message["client_id"], message["start_date"], 
                                message["end_date"], message["carrier_ids"], 
                                message["warehouse_ids"], app.driver, app.captured_token, stop_event,
                                ctrl_q
                            ),
                            daemon=True)
                        app.driver = None
                        app.captured_token = None
                        automation_thread.start()
                else:
                    gui_q.put({"type": "error", "title": "Erro de Sessão", "message": "Sessão ou token não encontrados. Carregue os filtros novamente.", "done": True})
            
            elif action == "stop":
                stop_event.set()
            elif action == "save_report":
                final = not stop_event.is_set()
                threading.Thread(target=save_report_thread, args=(gui_q, message["data"], final), daemon=True).start()
            elif action == "finish_stop":
                gui_q.put({"type": "info", "title": "Processo Interrompido", "message": "A auditoria foi parada pelo usuário.", "done": True})
        
        except queue.Empty:
            pass
        if root.winfo_exists():
            root.after(100, control_queue_monitor)
            
    root.after(100, control_queue_monitor)
    root.mainloop()
    sys.stdout = sys.__stdout__