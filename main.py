# main.py (Versão Final V2.10 - Correção de Escopo)

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

class Logger:
    # (código original sem alterações)
    def __init__(self, queue_gui, terminal_original):
        self.queue = queue_gui
        self.terminal = terminal_original
    def write(self, message):
        self.terminal.write(message)
        self.queue.put(message)
    def flush(self):
        self.terminal.flush()

def _get_browser_paths():
    # (código original sem alterações)
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
    # (código original sem alterações)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def kill_browser_processes(exec_name: str):
    # (código original sem alterações)
    for proc in psutil.process_iter(['name', 'exe']):
        if proc.info['name'] == exec_name:
            try:
                proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

def carregar_filtros_thread(queue_gui, client_id):
    # (código da V2.9 sem alterações)
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
        if api_token:
            warehouses = intelipost.obter_centros_de_distribuicao_api(api_token)
            carriers = intelipost.obter_transportadoras_api(api_token)
        queue_gui.put({ "type": "filters_loaded", "token": api_token, "warehouses": warehouses, "carriers": carriers })
    except Exception as e:
        print(f"\nERRO: {e}")
        queue_gui.put({"type": "error", "title": "Erro na Preparação", "message": f"Ocorreu uma falha inesperada:\n\n{e}"})
    finally:
        if driver:
            print("\nINFO: Processo de coleta de dados finalizado. O navegador permanecerá aberto.")

def save_report_thread(queue_gui, data, final=True):
    # (código original sem alterações)
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
                       f'Acesse a planilha em:\n{url_planilha}')
            title = "Processo Finalizado!" if final else "Relatório Salvo"
            queue_gui.put({"type": "info", "title": title, "message": message, "done": True})
        else:
            queue_gui.put({"type": "error", "title": "Erro no Relatório", "message": "Falha ao criar ou atualizar a planilha.", "done": True})
    except Exception as e:
        print(f"\nERRO NA THREAD DE SALVAMENTO: {e}")
        queue_gui.put({"type": "error", "title": "Erro ao Salvar", "message": f"Falha ao salvar o relatório:\n\n{e}", "done": True})

def executar_auditoria_thread(queue_gui, client_id, data_inicio, data_fim, lista_ids_transportadoras, lista_ids_warehouses, api_token, stop_event):
    # (código original sem alterações)
    start_time = time.time()
    lista_final_divergencias = []
    total_pedidos_original = 0
    try:
        config_margem = intelipost.obter_configuracao_margem_api(api_token)
        if not config_margem:
            queue_gui.put({"type": "error", "title": "Erro Crítico!", "message": 'Não foi possível obter a configuração da margem de tolerância da Intelipost.\n\nA auditoria não pode continuar.', "done": True})
            return
        queue_gui.put({"type": "margin_info", "config": config_margem})
        df_pedidos = database.obter_pedidos_para_auditoria(client_id, data_inicio, data_fim, lista_ids_transportadoras)
        total_pedidos_original = len(df_pedidos)
        if df_pedidos.empty:
            queue_gui.put({"type": "info", "title": "Aviso", "message": "Nenhum pedido encontrado.", "done": True})
            return
        print(f"\nINFO: Token obtido. Iniciando processamento de {total_pedidos_original} pedidos via API...")
        pedidos_processados = 0
        for _, pedido in df_pedidos.iterrows():
            if stop_event.is_set():
                print("\nINFO: Processo interrompido pelo usuário.")
                break
            pedidos_processados += 1
            order_number = pedido['so_order_number']
            transportadora_nome = pedido['lp_name']
            print(f"--- Processando Pedido {pedidos_processados}/{total_pedidos_original}: {order_number} ({transportadora_nome}) ---")
            chave_cte, valor_frete = intelipost.obter_dados_via_api_threaded(
                str(order_number), api_token, data_inicio, data_fim, lista_ids_warehouses, stop_event
            )
            if stop_event.is_set():
                break
            if chave_cte and valor_frete is not None:
                divergencia = comparator.encontrar_divergencias(pedido, valor_frete, chave_cte, transportadora_nome, config_margem)
                if divergencia:
                    lista_final_divergencias.append(divergencia)
            else:
                pass
            time.sleep(0.01)
    except Exception as e:
        print(f"\nERRO CRÍTICO NA THREAD DE AUDITORIA: {e}")
        queue_gui.put({"type": "error", "title": "Erro Crítico!", "message": f'Erro inesperado:\n\n{e}', "done": True})
        return
    finally:
        duration_seconds = time.time() - start_time
        data_para_salvar = (lista_final_divergencias, client_id, total_pedidos_original, duration_seconds)
        if stop_event.is_set():
            if lista_final_divergencias:
                queue_gui.put({"type": "ask_save", "data": data_para_salvar})
            else:
                q_control.put({"action": "finish_stop"})
        else:
            if lista_final_divergencias:
                q_control.put({"action": "save_report", "data": data_para_salvar})
            else:
                queue_gui.put({"type": "info", "title": "Processo Finalizado!", "message": 'Nenhuma divergência encontrada.', "done": True})
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
    
    # V2.10 - CORREÇÃO: A função agora recebe as filas como argumentos padrão para
    # garantir o escopo correto e evitar o erro do Pylance.
    def control_queue_monitor(gui_q=q_gui, ctrl_q=q_control):
        global automation_thread
        try:
            # Usa as referências passadas como argumento
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
                            message["warehouse_ids"], message["api_token"], stop_event
                        ),
                        daemon=True)
                    automation_thread.start()
            elif action == "stop":
                stop_event.set()
            elif action == "save_report":
                final = not stop_event.is_set()
                threading.Thread(target=save_report_thread, args=(gui_q, message["data"], final), daemon=True).start()
            elif action == "finish_stop":
                # A linha que causava o erro agora usa a referência correta.
                gui_q.put({"type": "info", "title": "Processo Interrompido", "message": "A auditoria foi parada pelo usuário.", "done": True})
        except queue.Empty:
            pass
        if root.winfo_exists():
            root.after(100, control_queue_monitor)
            
    root.after(100, control_queue_monitor)
    root.mainloop()
    sys.stdout = sys.__stdout__