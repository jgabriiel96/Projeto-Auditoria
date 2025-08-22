# interface_usuario.py (Versão Final V2.1)

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from tkcalendar import DateEntry
import queue
import time

class App:
    def __init__(self, root, queue_gui, queue_control):
        self.root = root
        self.queue_gui = queue_gui
        self.queue_control = queue_control
        self.root.title("Auditoria de Frete Automatizada")
        self.is_running = False
        
        self.vars_transportadoras = {}
        self.vars_warehouses = {}
        
        self.last_searched_client_id = None
        self.api_token = None
        self.start_time = None
        self.create_widgets()
        self.process_gui_queue()

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

        ttk.Label(self.params_frame, text="Data Início:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.start_date_entry = DateEntry(self.params_frame, width=12, date_pattern='y-mm-dd', maxdate=datetime.now())
        self.start_date_entry.grid(row=1, column=1, columnspan=2, sticky="ew", padx=5)
        self.start_date_entry.bind("<<DateEntrySelected>>", self._validate_all_fields)

        ttk.Label(self.params_frame, text="Data Fim:").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.end_date_entry = DateEntry(self.params_frame, width=12, date_pattern='y-mm-dd', maxdate=datetime.now())
        self.end_date_entry.grid(row=2, column=1, columnspan=2, sticky="ew", padx=5)
        self.end_date_entry.bind("<<DateEntrySelected>>", self._validate_all_fields)

        # V2.1 - Adiciona o Label para exibir a margem de tolerância
        self.margin_label = ttk.Label(self.params_frame, text="Margem de Tolerância: (Aguardando auditoria)")
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
        self.timer_label = ttk.Label(log_frame, text="Tempo de Execução: 00:00:00")
        self.timer_label.grid(row=1, column=0, sticky="w", pady=(5,0), padx=5)

        action_btn_frame = ttk.Frame(main_frame)
        action_btn_frame.grid(row=3, column=0, pady=10, padx=5, sticky="e")
        self.start_button = ttk.Button(action_btn_frame, text="Iniciar Auditoria", command=self.start_audit)
        self.start_button.pack(side="right")
        self.stop_button = ttk.Button(action_btn_frame, text="Parar Auditoria", command=self.stop_audit)
        self.stop_button.pack(side="right", padx=(0, 5))
        self._update_ui_state(False)

    def _update_ui_state(self, is_running, loading_filters=False):
        self.is_running = is_running
        new_state = "disabled" if is_running or loading_filters else "normal"
        self.client_id_entry.config(state=new_state)
        self.update_button.config(state=new_state)
        self.start_date_entry.config(state=new_state)
        self.end_date_entry.config(state=new_state)
        self.btn_marcar_wh.config(state=new_state)
        self.btn_desmarcar_wh.config(state=new_state)
        self.btn_marcar_carrier.config(state=new_state)
        self.btn_desmarcar_carrier.config(state=new_state)
        for child in self.scrollable_frame_wh.winfo_children(): child.configure(state=new_state)
        for child in self.scrollable_frame_carrier.winfo_children(): child.configure(state=new_state)
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

    def _carregar_filtros(self, event=None, force_refresh=False):
        client_id_str = self.client_id_entry.get()
        if not client_id_str.isdigit():
            self._limpar_checkboxes(self.scrollable_frame_wh, self.vars_warehouses)
            self._limpar_checkboxes(self.scrollable_frame_carrier, self.vars_transportadoras)
            self.margin_label.config(text="Margem de Tolerância: (Aguardando cliente)")
            self._validate_all_fields()
            return
        if not force_refresh and client_id_str == self.last_searched_client_id:
            return
        self.last_searched_client_id = client_id_str
        self.api_token = None
        self.margin_label.config(text="Margem de Tolerância: (Aguardando cliente)")
        self.update_log("INFO: Autenticando e buscando filtros para o cliente...\n")
        self._update_ui_state(False, loading_filters=True)
        self._limpar_checkboxes(self.scrollable_frame_wh, self.vars_warehouses)
        self._limpar_checkboxes(self.scrollable_frame_carrier, self.vars_transportadoras)
        self.queue_control.put({"action": "load_filters", "client_id": int(client_id_str)})

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

    def start_audit(self):
        if self.is_running: return
        
        # V2.1 - Limpa o texto da margem ao iniciar uma nova auditoria
        self.margin_label.config(text="Margem de Tolerância: (Buscando...)")
        
        client_id = int(self.client_id_entry.get())
        start_date = self.start_date_entry.get()
        end_date = self.end_date_entry.get()
        selected_warehouse_ids = [wh_id for wh_id, item in self.vars_warehouses.items() if item['var'].get()]
        selected_carrier_ids = [lp_id for lp_id, item in self.vars_transportadoras.items() if item['var'].get()]
        if not selected_warehouse_ids or not selected_carrier_ids:
            messagebox.showerror("Erro de Validação", "Pelo menos um Centro de Distribuição e uma Transportadora devem ser selecionados.")
            return
        self._update_ui_state(True)
        self.log_text.config(state="normal")
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state="disabled")
        self.start_time = time.time()
        self._update_timer()
        self.queue_control.put({
            "action": "start", "client_id": client_id, "start_date": start_date, "end_date": end_date,
            "carrier_ids": selected_carrier_ids, "warehouse_ids": selected_warehouse_ids, "api_token": self.api_token
        })
    
    def stop_audit(self):
        print("\nAVISO: Solicitação de parada enviada. Finalizando o pedido atual...")
        self.stop_button.config(text="Parando...", state="disabled")
        self.queue_control.put({"action": "stop"})
    
    def process_gui_queue(self):
        try:
            message = self.queue_gui.get_nowait()
            if isinstance(message, dict):
                msg_type = message.get("type")
                
                if msg_type == "filters_loaded":
                    self.update_log("INFO: Recebendo dados de filtros do backend...\n")
                    self._update_ui_state(False) # Reabilita a UI
                    self.api_token = message["token"]
                    self._popular_checkboxes(self.scrollable_frame_wh, self.vars_warehouses, message["warehouses"], "Centros de Distribuição")
                    self._popular_checkboxes(self.scrollable_frame_carrier, self.vars_transportadoras, message["carriers"], "Transportadoras")
                    if not self.api_token:
                        messagebox.showerror("Erro de Autenticação", "Não foi possível obter o token. Verifique o perfil do navegador e a conexão.")
                
                # V2.1 - Lógica para tratar a nova mensagem e atualizar a interface
                elif msg_type == "margin_info":
                    config = message.get("config", {})
                    margin_type = config.get("type")
                    margin_value = config.get("value")
                    texto_margem = "Margem de Tolerância: "
                    if margin_type == "ABSOLUTE":
                        texto_margem += f"R$ {margin_value:.2f} (Valor Fixo)"
                    elif margin_type == "PERCENTAGE":
                        texto_margem += f"{margin_value}% (Percentual)"
                    else:
                        texto_margem += "Não identificada"
                    self.margin_label.config(text=texto_margem)
                
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
                    if message.get("done"): self._update_ui_state(False)
            else:
                self.update_log(message)
        except queue.Empty:
            pass
        if self.root.winfo_exists():
            self.root.after(100, self.process_gui_queue)

    def validate_number(self, P): return P.isdigit() or P == ""

    def update_log(self, message):
        if self.log_text.winfo_exists():
            self.log_text.config(state="normal")
            self.log_text.insert(tk.END, message)
            self.log_text.see(tk.END)
            self.log_text.config(state="disabled")
            self.root.update_idletasks()

    def _validate_all_fields(self, *args):
        try:
            client_id_ok = self.client_id_entry.get().isdigit()
            datas_ok = self.end_date_entry.get_date() >= self.start_date_entry.get_date()
            carrier_ok = any(item['var'].get() for item in self.vars_transportadoras.values())
            warehouse_ok = any(item['var'].get() for item in self.vars_warehouses.values())
            if client_id_ok and datas_ok and carrier_ok and warehouse_ok and self.api_token:
                self.start_button.config(state="normal")
            else:
                self.start_button.config(state="disabled")
        except Exception:
            self.start_button.config(state="disabled")