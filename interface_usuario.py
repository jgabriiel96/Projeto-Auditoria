# interface_usuario.py

import tkinter as tk
from tkinter import ttk, messagebox
import webbrowser
from datetime import datetime, timedelta
from tkcalendar import Calendar
import queue
import time
import re

class App:
    def __init__(self, root, queue_gui, queue_control):
        self.root = root
        self.queue_gui = queue_gui
        self.queue_control = queue_control
        self.root.title("Auditoria de Frete Automatizada")
        
        self.is_running = False
        self.last_searched_client_id = None
        self.driver = None
        self.captured_token = None
        self.start_time = None
        
        self.vars_transportadoras = {}
        self.vars_warehouses = {}
        
        self.calendar_window = None
        
        s = ttk.Style()
        s.configure('Invalid.TEntry', fieldbackground='#ffdddd')
        
        self.create_widgets()
        self.process_gui_queue()
        
        self.root.bind("<Button-1>", self._close_calendar_if_open)

    def _carregar_filtros(self, event=None, force_refresh=False):
        """
        Inicia o processo de carregamento de filtros para um novo cliente.
        A lógica de reset do navegador agora é 100% gerenciada pelo backend.
        """
        client_id_str = self.client_id_entry.get()

        if not client_id_str.isdigit():
            # Limpa a UI se o ID for inválido
            self._limpar_checkboxes(self.scrollable_frame_wh, self.vars_warehouses)
            self._limpar_checkboxes(self.scrollable_frame_carrier, self.vars_transportadoras)
            self.margin_label.config(text="Margem de Tolerância: (Aguardando cliente)")
            return
        
        if not force_refresh and client_id_str == self.last_searched_client_id:
            return
        
        # Limpa o estado visual da UI imediatamente
        self.last_searched_client_id = client_id_str
        self.driver = None
        self.captured_token = None
        self._limpar_checkboxes(self.scrollable_frame_wh, self.vars_warehouses)
        self._limpar_checkboxes(self.scrollable_frame_carrier, self.vars_transportadoras)
        self.margin_label.config(text="Margem de Tolerância: (Aguardando cliente)")
        self._validate_all_fields()
        
        self.update_log("INFO: Solicitando nova sessão para o cliente...\n")
        self._update_ui_state(False, loading_filters=True)
        
        # Envia a solicitação para o backend. O backend cuidará da limpeza da sessão.
        self.queue_control.put({"action": "load_filters", "client_id": int(client_id_str)})

    # O restante do arquivo pode ser colado da sua versão original/última versão.
    # Nenhuma outra função precisa de alteração.

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(2, weight=1)

        self.params_frame = ttk.LabelFrame(main_frame, text="Parâmetros da Auditoria", padding="10")
        self.params_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        self.params_frame.columnconfigure(1, weight=1)
        
        vcmd_number = (self.root.register(self.validate_number), "%P")
        ttk.Label(self.params_frame, text="ID do Cliente:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.client_id_entry = ttk.Entry(self.params_frame, validate="key", validatecommand=vcmd_number)
        self.client_id_entry.grid(row=0, column=1, sticky="ew", padx=5)
        self.client_id_entry.bind("<FocusOut>", self._carregar_filtros)
        self.client_id_entry.bind("<Return>", self._carregar_filtros)
        
        self.update_button = ttk.Button(self.params_frame, text="Atualizar Filtros", command=lambda: self._carregar_filtros(force_refresh=True))
        self.update_button.grid(row=0, column=2, sticky="e", padx=(0, 5))
        
        ttk.Label(self.params_frame, text="Data Início (YYYY-MM-DD):").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        start_date_frame = ttk.Frame(self.params_frame)
        start_date_frame.grid(row=1, column=1, columnspan=2, sticky="ew")
        self.start_date_entry = ttk.Entry(start_date_frame)
        self.start_date_entry.pack(side="left", fill="x", expand=True, padx=(5, 0))
        start_cal_button = ttk.Button(start_date_frame, text="📅", width=3,
                                        command=lambda: self._open_calendar(self.start_date_entry))
        start_cal_button.pack(side="left")
        self.start_date_entry.bind("<KeyRelease>", self._on_date_change)

        ttk.Label(self.params_frame, text="Data Fim (YYYY-MM-DD):").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        end_date_frame = ttk.Frame(self.params_frame)
        end_date_frame.grid(row=2, column=1, columnspan=2, sticky="ew")
        self.end_date_entry = ttk.Entry(end_date_frame)
        self.end_date_entry.pack(side="left", fill="x", expand=True, padx=(5, 0))
        end_cal_button = ttk.Button(end_date_frame, text="📅", width=3,
                                        command=lambda: self._open_calendar(self.end_date_entry))
        end_cal_button.pack(side="left")
        self.end_date_entry.bind("<KeyRelease>", self._on_date_change)
        
        self.margin_label = ttk.Label(self.params_frame, text="Margem de Tolerância: (Aguardando cliente)")
        self.margin_label.grid(row=3, column=0, columnspan=3, sticky="w", padx=5, pady=(10, 5))
        
        filters_frame = ttk.LabelFrame(main_frame, text="Filtros Obrigatórios", padding="10")
        filters_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        filters_frame.columnconfigure(0, weight=1)
        filters_frame.columnconfigure(1, weight=1)
        filters_frame.rowconfigure(0, weight=1)
        wh_frame = ttk.LabelFrame(filters_frame, text="Centros de Distribuição *")
        wh_frame.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        wh_frame.columnconfigure(0, weight=1)
        wh_frame.rowconfigure(1, weight=1)
        btn_frame_wh = ttk.Frame(wh_frame)
        btn_frame_wh.grid(row=0, column=0, sticky="ew", pady=(0, 5))
        self.btn_marcar_wh = ttk.Button(btn_frame_wh, text="Marcar Todos", command=lambda: self._marcar_desmarcar_todos(self.vars_warehouses, True))
        self.btn_marcar_wh.pack(side="left", padx=2)
        self.btn_desmarcar_wh = ttk.Button(btn_frame_wh, text="Desmarcar Todos", command=lambda: self._marcar_desmarcar_todos(self.vars_warehouses, False))
        self.btn_desmarcar_wh.pack(side="left", padx=2)
        canvas_wh = tk.Canvas(wh_frame, height=100, borderwidth=0, highlightthickness=0)
        scrollbar_wh = ttk.Scrollbar(wh_frame, orient="vertical", command=canvas_wh.yview)
        self.scrollable_frame_wh = ttk.Frame(canvas_wh)
        self.scrollable_frame_wh.bind("<Configure>", lambda e: canvas_wh.configure(scrollregion=canvas_wh.bbox("all")))
        canvas_wh.create_window((0, 0), window=self.scrollable_frame_wh, anchor="nw")
        canvas_wh.configure(yscrollcommand=scrollbar_wh.set)
        canvas_wh.grid(row=1, column=0, sticky="nsew")
        scrollbar_wh.grid(row=1, column=1, sticky="ns")
        carrier_frame = ttk.LabelFrame(filters_frame, text="Transportadoras *")
        carrier_frame.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
        carrier_frame.columnconfigure(0, weight=1)
        carrier_frame.rowconfigure(1, weight=1)
        btn_frame_carrier = ttk.Frame(carrier_frame)
        btn_frame_carrier.grid(row=0, column=0, sticky="ew", pady=(0, 5))
        self.btn_marcar_carrier = ttk.Button(btn_frame_carrier, text="Marcar Todos", command=lambda: self._marcar_desmarcar_todos(self.vars_transportadoras, True))
        self.btn_marcar_carrier.pack(side="left", padx=2)
        self.btn_desmarcar_carrier = ttk.Button(btn_frame_carrier, text="Desmarcar Todos", command=lambda: self._marcar_desmarcar_todos(self.vars_transportadoras, False))
        self.btn_desmarcar_carrier.pack(side="left", padx=2)
        canvas_carrier = tk.Canvas(carrier_frame, height=100, borderwidth=0, highlightthickness=0)
        scrollbar_carrier = ttk.Scrollbar(carrier_frame, orient="vertical", command=canvas_carrier.yview)
        self.scrollable_frame_carrier = ttk.Frame(canvas_carrier)
        self.scrollable_frame_carrier.bind("<Configure>", lambda e: canvas_carrier.configure(scrollregion=canvas_carrier.bbox("all")))
        canvas_carrier.create_window((0, 0), window=self.scrollable_frame_carrier, anchor="nw")
        canvas_carrier.configure(yscrollcommand=scrollbar_carrier.set)
        canvas_carrier.grid(row=1, column=0, sticky="nsew")
        scrollbar_carrier.grid(row=1, column=1, sticky="ns")

        log_frame = ttk.LabelFrame(main_frame, text="Log de Execução", padding="10")
        log_frame.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, height=10, width=80, state="disabled", wrap="word", bg="black", fg="white")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.config(yscrollcommand=log_scrollbar.set)
        
        status_frame = ttk.Frame(log_frame)
        status_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5,0), padx=5)
        status_frame.columnconfigure(1, weight=1)
        
        self.timer_label = ttk.Label(status_frame, text="Tempo de Execução: 00:00:00")
        self.timer_label.grid(row=0, column=0, sticky="w")
        
        self.progress_label = ttk.Label(status_frame, text="")
        self.progress_label.grid(row=0, column=1, sticky="e")

        self.progress_bar = ttk.Progressbar(log_frame, orient='horizontal', mode='determinate')
        self.progress_bar.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(2,0), padx=5)
        self.progress_bar.grid_remove()

        action_btn_frame = ttk.Frame(main_frame)
        action_btn_frame.grid(row=3, column=0, pady=10, padx=5, sticky="e")
        self.start_button = ttk.Button(action_btn_frame, text="Iniciar Auditoria", command=self.start_audit)
        self.start_button.pack(side="right")
        self.stop_button = ttk.Button(action_btn_frame, text="Parar Auditoria", command=self.stop_audit)
        self.stop_button.pack(side="right", padx=(0, 5))
        self._update_ui_state(False)
    
    def start_audit(self):
        if self.is_running: return
        is_valid, error_message = self.run_final_validation()
        if not is_valid:
            messagebox.showerror("Erro de Validação", error_message)
            return

        self.progress_bar.grid()
        self.progress_bar['value'] = 0
        self.progress_label.config(text="0.0% | ETA: Calculando...")

        client_id = int(self.client_id_entry.get())
        start_date = self.start_date_entry.get()
        end_date = self.end_date_entry.get()

        all_warehouses_selected = all(item['var'].get() for item in self.vars_warehouses.values()) if self.vars_warehouses else False
        selected_warehouse_ids = [] if all_warehouses_selected else [wh_id for wh_id, item in self.vars_warehouses.items() if item['var'].get()]

        all_carriers_selected = all(item['var'].get() for item in self.vars_transportadoras.values()) if self.vars_transportadoras else False
        selected_carrier_ids = [] if all_carriers_selected else [lp_id for lp_id, item in self.vars_transportadoras.items() if item['var'].get()]
        
        self._update_ui_state(True)
        self.log_text.config(state="normal")
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state="disabled")
        self.start_time = time.time()
        self._update_timer()
        self.queue_control.put({
            "action": "start", "client_id": client_id, "start_date": start_date, "end_date": end_date,
            "carrier_ids": selected_carrier_ids, "warehouse_ids": selected_warehouse_ids
        })
    
    def process_gui_queue(self):
        try:
            message = self.queue_gui.get_nowait()
            if isinstance(message, dict):
                msg_type = message.get("type")
                
                if msg_type == "filters_loaded":
                    self.update_log("INFO: Recebendo dados de filtros do backend...\n")
                    self._update_ui_state(False)
                    self.driver = message.get("driver") 
                    self.captured_token = message.get("token")
                    self._popular_checkboxes(self.scrollable_frame_wh, self.vars_warehouses, message["warehouses"], "Centros de Distribuição")
                    self._popular_checkboxes(self.scrollable_frame_carrier, self.vars_transportadoras, message["carriers"], "Transportadoras")
                    if not self.driver or not self.captured_token:
                        messagebox.showerror("Erro de Autenticação", "Não foi possível obter a sessão do navegador.")
                
                elif msg_type == "progress_update":
                    self._update_progress(message["current"], message["total"], message.get("label", ""))
                elif msg_type == "margin_info":
                    config = message.get("config", {})
                    margin_type = config.get("type")
                    texto_margem = "Margem de Tolerância: "
                    if margin_type == "ABSOLUTE":
                        margin_value = config.get("value", 0.0)
                        texto_margem += f"R$ {margin_value:.2f} (Valor Fixo)"
                    elif margin_type == "PERCENTAGE":
                        margin_value = config.get("value", 0.0)
                        texto_margem += f"{margin_value}% (Percentual)"
                    elif margin_type == "SYSTEM_DEFAULT":
                        texto_margem += "Padrão do Sistema (1%)"
                    elif margin_type == "DYNAMIC_CHOICE":
                        absolute_val = config.get("absolute_value", 0.0)
                        percentage_val = config.get("percentage_value", 0.0)
                        texto_margem += f"Dinâmico (Maior entre R$ {absolute_val:.2f} e {percentage_val}%)"
                    else:
                        texto_margem += "Não identificada ou não configurada"
                    self.margin_label.config(text=texto_margem)
                elif msg_type == "ask_open_sheet":
                    url = message.get("url")
                    if messagebox.askyesno(message["title"], message["message"]):
                        if url:
                            print(f"INFO: Abrindo a planilha em {url}...")
                            webbrowser.open_new_tab(url)
                    if message.get("done"):
                        self._update_ui_state(False)
                elif msg_type == "ask_save":
                    data_to_save = message["data"]
                    if messagebox.askyesno("Salvar Relatório?", "A auditoria foi interrompida. Deseja salvar as divergências encontradas até agora?"):
                        self.queue_control.put({"action": "save_report", "data": data_to_save})
                    else:
                        print("INFO: Relatório descartado pelo usuário.")
                        self.queue_control.put({"action": "finish_stop"})
                elif msg_type in ("info", "error"):
                    if msg_type == "info": messagebox.showinfo(message["title"], message["message"])
                    else: messagebox.showerror(message["title"], message["message"])
                    if message.get("done"):
                        self._update_ui_state(False)
            else:
                self.update_log(message)
        except queue.Empty:
            pass
        if self.root.winfo_exists():
            self.root.after(100, self.process_gui_queue)

    def _validate_all_fields(self, *args):
        client_id_ok = self.client_id_entry.get().isdigit()
        dates_ok = False
        try:
            start_date_obj = datetime.strptime(self.start_date_entry.get(), '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(self.end_date_entry.get(), '%Y-%m-%d').date()
            if end_date_obj >= start_date_obj:
                dates_ok = True
        except (ValueError, TypeError):
            dates_ok = False
        carrier_ok = any(item['var'].get() for item in self.vars_transportadoras.values())
        warehouse_ok = any(item['var'].get() for item in self.vars_warehouses.values())
        if client_id_ok and dates_ok and carrier_ok and warehouse_ok and self.driver and self.captured_token:
            self.start_button.config(state="normal")
        else:
            self.start_button.config(state="disabled")

    def _open_calendar(self, entry_widget):
        self._close_calendar_if_open() 
        x = entry_widget.winfo_rootx()
        y = entry_widget.winfo_rooty() + entry_widget.winfo_height()
        self.calendar_window = tk.Toplevel(self.root)
        self.calendar_window.wm_overrideredirect(True) 
        self.calendar_window.wm_geometry(f"+{x}+{y}")
        try:
            current_date = datetime.strptime(entry_widget.get(), '%Y-%m-%d')
        except ValueError:
            current_date = datetime.now()
        earliest_date = datetime.now() - timedelta(days=90)
        cal = Calendar(self.calendar_window, selectmode='day', date_pattern='y-mm-dd',
                       year=current_date.year, month=current_date.month, day=current_date.day,
                       mindate=earliest_date, maxdate=datetime.now())
        cal.pack()
        cal.bind("<<CalendarSelected>>", lambda event: self._on_date_selected(event, entry_widget))
        self.calendar_window.bind("<Button-1>", lambda event: "break")
    
    def _on_date_selected(self, event, entry_widget):
        widget = event.widget
        selected_date = widget.get_date()
        entry_widget.delete(0, tk.END)
        entry_widget.insert(0, selected_date)
        self._close_calendar_if_open()
        self._validate_all_fields()
    
    def _close_calendar_if_open(self, event=None):
        if self.calendar_window:
            self.calendar_window.destroy()
            self.calendar_window = None
    
    def _on_date_change(self, event):
        widget = event.widget
        if event.keysym in ('BackSpace', 'Delete'):
            self._validate_all_fields()
            return
        current_text = widget.get()
        digits_only = re.sub(r'\D', '', current_text)[:8]
        formatted_text = ""
        if len(digits_only) > 0:
            formatted_text += digits_only[:4]
        if len(digits_only) > 4:
            formatted_text += '-' + digits_only[4:6]
        if len(digits_only) > 6:
            formatted_text += '-' + digits_only[6:8]
        if formatted_text != current_text:
            widget.delete(0, tk.END)
            widget.insert(0, formatted_text)
            widget.icursor(tk.END)
        self._validate_all_fields()
    
    def run_final_validation(self):
        try:
            start_date = datetime.strptime(self.start_date_entry.get(), '%Y-%m-%d').date()
            end_date = datetime.strptime(self.end_date_entry.get(), '%Y-%m-%d').date()
            if end_date < start_date:
                return False, "A 'Data Fim' não pode ser anterior à 'Data Início'."
            earliest_date = (datetime.now() - timedelta(days=90)).date()
            if start_date < earliest_date:
                return False, f"O período de auditoria não pode começar antes de {earliest_date.strftime('%d/%m/%Y')} (limite de 90 dias)."
        except (ValueError, TypeError):
                return False, "O formato de uma das datas é inválido. Use YYYY-MM-DD."
        if not self.client_id_entry.get().isdigit():
            return False, "O ID do Cliente é inválido."
        if not any(item['var'].get() for item in self.vars_warehouses.values()):
            return False, "Selecione ao menos um Centro de Distribuição."
        if not any(item['var'].get() for item in self.vars_transportadoras.values()):
            return False, "Selecione ao menos uma Transportadora."
        return True, ""

    def validate_number(self, P):
        return P.isdigit() or P == ""

    def stop_audit(self):
        print("\nAVISO: Solicitação de parada enviada. Finalizando o pedido atual...")
        self.stop_button.config(text="Parando...", state="disabled")
        self.queue_control.put({"action": "stop"})
    
    def _update_ui_state(self, is_running, loading_filters=False):
        self.is_running = is_running
        new_state = "disabled" if is_running or loading_filters else "normal"
        self.client_id_entry.config(state=new_state)
        self.update_button.config(state=new_state)
        self.btn_marcar_wh.config(state=new_state)
        self.btn_desmarcar_wh.config(state=new_state)
        self.btn_marcar_carrier.config(state=new_state)
        self.btn_desmarcar_carrier.config(state=new_state)
        for child in self.scrollable_frame_wh.winfo_children(): child.configure(state=new_state)
        for child in self.scrollable_frame_carrier.winfo_children(): child.configure(state=new_state)
        
        if not is_running:
            self.progress_bar.grid_remove()
            self.progress_label.config(text="")
            
        if is_running:
            self.start_button.pack_forget()
            self.stop_button.pack(side="right", padx=(0, 5))
            self.stop_button.config(text="Parar Auditoria", state="normal")
        else:
            self.stop_button.pack_forget()
            self.start_button.pack(side="right")
            self._validate_all_fields()
            self.start_time = None
    
    def _update_timer(self):
        if self.is_running and self.start_time:
            elapsed_seconds = time.time() - self.start_time
            formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_seconds))
            self.timer_label.config(text=f"Tempo de Execução: {formatted_time}")
            self.root.after(1000, self._update_timer)
        else:
            self.timer_label.config(text="Tempo de Execução: 00:00:00")
    
    def update_log(self, message):
        if self.log_text.winfo_exists():
            self.log_text.config(state="normal")
            self.log_text.insert(tk.END, message)
            self.log_text.see(tk.END)
            self.log_text.config(state="disabled")
            self.root.update_idletasks()
    
    def _limpar_checkboxes(self, frame, var_dict):
        for widget in frame.winfo_children(): widget.destroy()
        var_dict.clear()
    
    def _popular_checkboxes(self, frame, var_dict, items, nome_item):
        self._limpar_checkboxes(frame, var_dict)
        if items:
            for item_id, item_name in items:
                var = tk.BooleanVar(value=True)
                var.trace_add("write", self._validate_all_fields)
                chk = ttk.Checkbutton(frame, text=f"{item_name} (ID: {item_id})", variable=var)
                chk.pack(anchor="w", padx=5)
                var_dict[item_id] = {'var': var, 'data': (item_id, item_name)}
            self.update_log(f"INFO: Lista de {nome_item} carregada e pré-selecionada.\n")
        else:
            self.update_log(f"AVISO: Nenhum(a) {nome_item} encontrado(a).\n")
        self._validate_all_fields()
    
    def _marcar_desmarcar_todos(self, var_dict, marcar: bool):
        for item in var_dict.values():
            item['var'].set(marcar)

    def _update_progress(self, current: int, total: int, label: str = ""):
        if total > 0:
            percent = (current / total) * 100
            self.progress_bar['value'] = percent
            eta_str = "--:--"
            if current > 10:
                if self.start_time:
                    elapsed_time = time.time() - self.start_time
                    time_per_item = elapsed_time / current
                    remaining_items = total - current
                    eta_seconds = time_per_item * remaining_items
                    if eta_seconds > 3600:
                        eta_str = time.strftime("%H:%M:%S", time.gmtime(eta_seconds))
                    else:
                        eta_str = time.strftime("%M:%S", time.gmtime(eta_seconds))
            progress_text = f"{percent:.1f}% ({current}/{total})"
            if label:
                progress_text = f"{label}: {progress_text}"
            self.progress_label.config(text=f"{progress_text} | ETA: {eta_str}")