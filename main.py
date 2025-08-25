# main.py

import interface_usuario as gui
from core import database, comparator, sheets, intelipost
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException
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

class Logger:
    def __init__(self, queue_gui, terminal_original):
        self.queue = queue_gui
        self.terminal = terminal_original
    def write(self, message):
        self.terminal.write(message)
        self.queue.put(message)
    def flush(self):
        self.terminal.flush()

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
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        caminho_executavel = config.get('BROWSER', 'caminho_executavel', fallback=None)
        debug_port_str = config.get('BROWSER', 'remote_debugging_port', fallback='9222')
        debug_port = int(debug_port_str)
        headless_mode = config.getboolean('AUTOMATION', 'headless', fallback=False)
        paths = _get_browser_paths()
        if not caminho_executavel:
            caminho_executavel = paths.get('brave_exec') or paths.get('chrome_exec')
        if not caminho_executavel or not os.path.isfile(caminho_executavel):
            raise FileNotFoundError("Não foi possível localizar o navegador. Verifique se o Chrome/Brave está instalado.")
        exec_name = os.path.basename(caminho_executavel)
        user_data_dir = paths.get('brave_user_data') if 'brave' in exec_name.lower() else paths.get('chrome_user_data')
        if not os.path.isdir(user_data_dir):
            raise FileNotFoundError(f"Não foi possível localizar o diretório do perfil: {user_data_dir}")
        if not is_port_in_use(debug_port):
            is_running = any(p.name() == exec_name for p in psutil.process_iter(['name']))
            if is_running:
                print("AVISO: Navegador detectado em modo normal. Solicitando reinício ao usuário.")
                messagebox.showwarning(
                    "Reinicialização Necessária",
                    f"O {exec_name} já está em execução.\n\nPara que a automação funcione, o robô precisa reiniciá-lo em um modo especial.\n\n"
                    "Por favor, salve seu trabalho em todas as janelas e clique em OK."
                )
                print("INFO: Usuário notificado. Fechando processos existentes...")
                kill_browser_processes(exec_name)
                time.sleep(2)
            print(f"INFO: Iniciando o navegador com o perfil do usuário no modo de depuração (porta {debug_port})...")
            command = [caminho_executavel, f'--remote-debugging-port={debug_port}', f'--user-data-dir={user_data_dir}']
            if headless_mode:
                print("INFO: Modo headless ativado. Adicionando argumentos '--headless' e '--window-size'.")
                command.append('--headless')
                command.append('--window-size=1920,1080')
            subprocess.Popen(command)
            time.sleep(5)
            print("SUCESSO: Navegador iniciado em modo de depuração.")
        options = ChromeOptions()
        options.binary_location = caminho_executavel
        options.add_experimental_option("debuggerAddress", f"127.0.0.1:{debug_port}")
        servico = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=servico, options=options)
        print("SUCESSO: Robô conectado com sucesso ao navegador!")
        api_token = intelipost.preparar_pagina_e_capturar_token(driver, str(client_id))
        warehouses = []
        carriers = []
        margin_config = None
        if api_token:
            warehouses = intelipost.obter_centros_de_distribuicao_api(api_token)
            carriers = intelipost.obter_transportadoras_api(api_token)
            margin_config = intelipost.obter_configuracao_margem_api(api_token)
        queue_gui.put({
            "type": "filters_loaded",
            "token": api_token,
            "warehouses": warehouses,
            "carriers": carriers
        })
        if margin_config:
            queue_gui.put({"type": "margin_info", "config": margin_config})
        else:
             queue_gui.put({"type": "margin_info", "config": {}})
    except Exception as e:
        print(f"\nERRO: {e}")
        queue_gui.put({"type": "error", "title": "Erro na Preparação", "message": f"Ocorreu uma falha inesperada:\n\n{e}"})
    finally:
        if driver:
            print("\nINFO: Processo de coleta de dados finalizado. O navegador permanecerá aberto.")

def save_report_thread(queue_gui, data, final=True):
    try:
        lista_divergencias, client_id, total_pedidos, duration_seconds = data
        config = configparser.ConfigParser()
        config.read('config.ini')
        recipient_email = config['SHEETS']['email_destinatario']
        sheet_name = f"Auditoria Frete - Cliente {client_id}"
        url_planilha = sheets.reportar_divergencias(lista_divergencias, sheet_name, recipient_email)
        if url_planilha:
            sheets.criar_aba_sumario(sheet_name, total_pedidos, lista_divergencias)
            formatted_duration = time.strftime("%M minutos e %S segundos", time.gmtime(duration_seconds))
            message = (f'{len(lista_divergencias)} divergências encontradas e reportadas.\n\n'
                       f'Tempo total da auditoria: {formatted_duration}\n\n'
                       f'Deseja abrir a planilha no navegador?')
            title = "Processo Finalizado!" if final else "Relatório Salvo"
            
            queue_gui.put({
                "type": "ask_open_sheet", 
                "title": title, 
                "message": message, 
                "url": url_planilha, 
                "done": True
            })
        else:
            queue_gui.put({"type": "error", "title": "Erro no Relatório", "message": "Falha ao criar ou atualizar a planilha.", "done": True})
    except Exception as e:
        print(f"\nERRO NA THREAD DE SALVAMENTO: {e}")
        queue_gui.put({"type": "error", "title": "Erro ao Salvar", "message": f"Falha ao salvar o relatório:\n\n{e}", "done": True})

def executar_auditoria_thread(queue_gui, client_id, data_inicio, data_fim, lista_ids_transportadoras, lista_ids_warehouses, api_token, stop_event, q_control):
    start_time = time.time()
    lista_final_divergencias = []

    try:
        config_margem = intelipost.obter_configuracao_margem_api(api_token)
        if not config_margem:
            raise ValueError("Não foi possível obter a configuração da margem.")

        print("\nINFO: Buscando todas as pré-faturas auditáveis na API. Isso pode levar um tempo...")
        pre_faturas_api = intelipost.obter_pre_faturas_prontas_por_data(
            api_token,
            data_inicio,
            data_fim,
            lista_ids_warehouses,
            lista_ids_transportadoras,
            stop_event
        )

        if stop_event.is_set(): raise InterruptedError("Processo interrompido.")
        if not pre_faturas_api:
            queue_gui.put({"type": "info", "title": "Aviso", "message": "Nenhuma pré-fatura pronta para auditoria foi encontrada na Intelipost para o período.", "done": True})
            return

        print(f"INFO: A API retornou {len(pre_faturas_api)} pré-faturas prontas para auditoria.")
        
        dados_api_list = []
        for item in pre_faturas_api:
            if item.get("invoice") and len(item["invoice"]) > 0:
                order_number = item["invoice"][0].get("order_number")
                if order_number:
                    dados_api_list.append({
                        "so_order_number": order_number,
                        "chave_cte": item.get("cte", {}).get("key"),
                        "valor_intelipost": item.get("cte_value")
                    })
        df_api = pd.DataFrame(dados_api_list)

        lista_pedidos_api = df_api["so_order_number"].unique().tolist()
        df_pedidos_db = database.obter_dados_de_pedidos_especificos(client_id, lista_pedidos_api)
        if df_pedidos_db.empty:
            raise ValueError("Nenhum dos pedidos encontrados na API corresponde a um pedido no banco de dados.")

        if stop_event.is_set(): raise InterruptedError("Processo interrompido.")

        print("INFO: Unindo dados da API e do Banco de Dados...")
        df_pedidos_db['so_order_number'] = df_pedidos_db['so_order_number'].astype(str)
        df_api['so_order_number'] = df_api['so_order_number'].astype(str)
        
        df_merged = pd.merge(df_pedidos_db, df_api, on="so_order_number", how="inner")
        
        if df_merged.empty:
            raise ValueError("Falha ao unir os dados da API e do banco de dados.")

        total_pedidos_para_auditar = len(df_merged)
        print(f"INFO: {total_pedidos_para_auditar} pedidos prontos para comparação. Iniciando processamento vectorizado...")
        queue_gui.put({"type": "progress_update", "current": 0, "total": total_pedidos_para_auditar})
        
        df_merged['config_margem'] = [config_margem] * total_pedidos_para_auditar
        
        resultados = df_merged.apply(comparator.encontrar_divergencias, axis=1)
        
        lista_final_divergencias = resultados.dropna().tolist()
        
        queue_gui.put({"type": "progress_update", "current": total_pedidos_para_auditar, "total": total_pedidos_para_auditar})
        print(f"SUCESSO: Comparação vectorizada concluída. {len(lista_final_divergencias)} divergências encontradas.")

    except InterruptedError:
        print("\nINFO: Processo interrompido pelo usuário.")
    except Exception as e:
        print(f"\nERRO CRÍTICO NA THREAD DE AUDITORIA: {e}")
        queue_gui.put({"type": "error", "title": "Erro Crítico!", "message": f'Erro inesperado:\n\n{e}', "done": True})
        return
    finally:
        duration_seconds = time.time() - start_time
        formatted_duration = time.strftime("%H horas, %M minutos e %S segundos", time.gmtime(duration_seconds))
        print(f"\nINFO: Tempo total da auditoria: {formatted_duration}.")
        
        total_pedidos_auditados = len(df_merged) if 'df_merged' in locals() else 0
        data_para_salvar = (lista_final_divergencias, client_id, total_pedidos_auditados, duration_seconds)
        
        if stop_event.is_set():
            if lista_final_divergencias:
                queue_gui.put({"type": "ask_save", "data": data_para_salvar})
            else:
                q_control.put({"action": "finish_stop"})
        else:
            if lista_final_divergencias:
                q_control.put({"action": "save_report", "data": data_para_salvar})
            else:
                # Se não houver divergências, também faz a pergunta para abrir a planilha (vazia)
                message_final = "Nenhuma divergência encontrada.\n\nDeseja abrir a planilha mesmo assim?"
                url_planilha_vazia = f"https://docs.google.com/spreadsheets/d/{os.getenv('GOOGLE_SHEET_TEMPLATE_ID', '')}" # Fallback para o template
                # Tentativa de criar/obter a URL da planilha vazia
                try:
                    config = configparser.ConfigParser(); config.read('config.ini')
                    sheet_name = f"Auditoria Frete - Cliente {client_id}"
                    recipient_email = config['SHEETS']['email_destinatario']
                    # A função reportar_divergencias pode retornar a URL mesmo com lista vazia
                    url_planilha_vazia = sheets.reportar_divergencias([], sheet_name, recipient_email) or url_planilha_vazia
                except Exception:
                    pass
                
                queue_gui.put({
                    "type": "ask_open_sheet", 
                    "title": "Processo Finalizado!", 
                    "message": message_final,
                    "url": url_planilha_vazia,
                    "done": True
                })
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
    
    def control_queue_monitor(gui_q=q_gui, ctrl_q=q_control):
        global automation_thread
        try:
            message = ctrl_q.get_nowait()
            action = message.get("action")
            
            if action == "load_filters":
                threading.Thread(target=carregar_filtros_thread, args=(gui_q, message["client_id"]), daemon=True).start()
            elif action == "start":
                if not automation_thread or not automation_thread.is_alive():
                    stop_event.clear()
                    automation_thread = threading.Thread(
                        target=executar_auditoria_thread,
                        args=(
                            gui_q, message["client_id"], message["start_date"], 
                            message["end_date"], message["carrier_ids"], 
                            message["warehouse_ids"], message["api_token"], stop_event,
                            ctrl_q
                        ),
                        daemon=True)
                    automation_thread.start()
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