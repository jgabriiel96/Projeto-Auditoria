# core/sheets.py

import gspread
import os
from datetime import datetime
import pandas as pd
import time

def get_sheets_client():
    """
    Função de autenticação ATUALIZADA para usar o método moderno
    e recomendado pelo Google, evitando erros com bibliotecas descontinuadas.
    """
    return gspread.service_account(filename='credentials.json')

def reportar_divergencias(lista_divergencias: list, sheet_name: str, recipient_email: str):
    if not lista_divergencias:
        print(f"INFO: Nenhuma divergência para reportar.")
        return None
    try:
        client = get_sheets_client()
        template_name = os.getenv("GOOGLE_SHEET_TEMPLATE_NAME")
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

        if not template_name or not folder_id:
            print("ERRO: Verifique se GOOGLE_SHEET_TEMPLATE_NAME e GOOGLE_DRIVE_FOLDER_ID estão no arquivo .env.")
            return None
        
        try:
            spreadsheet = client.open(sheet_name)
            print(f"INFO: Planilha '{sheet_name}' já existe. Atualizando dados...")

        except gspread.exceptions.SpreadsheetNotFound:
            print(f"INFO: Planilha '{sheet_name}' não encontrada. Criando uma nova a partir do template...")
            
            template_spreadsheet = client.open(template_name)
            
            copied_file = client.copy(
                template_spreadsheet.id, 
                title=sheet_name, 
                copy_permissions=True
            )
            
            drive_service = client.drive_service
            file_id = copied_file.id
            file = drive_service.files().get(field='parents', fileId=file_id).execute()
            previous_parents = ",".join(file.get('parents'))
            
            drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            print(f"INFO: Planilha copiada e movida para a pasta correta no Drive.")
            
            spreadsheet = client.open_by_key(copied_file.id)

            if recipient_email:
                print(f"INFO: Compartilhando planilha com {recipient_email}...")
                spreadsheet.share(recipient_email, perm_type='user', role='writer', notify=False)
        
        sheet = spreadsheet.sheet1
        sheet.clear()
        sheet.update_title("Divergências Detalhadas")
        
        novos_cabecalhos = [
            'Data Auditoria', 'Pedido', 'Transportadora', 'Chave Acesso (Ct-e)', 'Campo Divergente',
            'Custo de envio pedido', 'Custo de envio SEFAZ',
            'Valor da Diferença', 'Margem Aplicada', 'Status']
        
        linhas_para_adicionar = [novos_cabecalhos]
        for div in lista_divergencias:
            linha = [
                datetime.now().strftime('%d/%m/%Y %H:%M:%S'), div.get('id_pedido'),
                div.get('transportadora'), div.get('chave_acesso'), div.get('campo'),
                f"R$ {div.get('valor_banco', 0):.2f}".replace('.', ','),
                f"R$ {div.get('valor_intelipost', 0):.2f}".replace('.', ','),
                f"R$ {div.get('diferenca_valor', 0):.2f}".replace('.', ','),
                div.get('margem_aplicada', 'N/A'),
                div.get('status', '')]
            linhas_para_adicionar.append(linha)
        
        sheet.update(linhas_para_adicionar, value_input_option='USER_ENTERED')
        print(f"SUCESSO: {len(linhas_para_adicionar) - 1} divergências reportadas.")
        print(f"URL da Planilha: {spreadsheet.url}")
        return spreadsheet.url
    except Exception as e:
        print(f"ERRO CRÍTICO AO ACESSAR O GOOGLE SHEETS. Detalhe: {e}")
        return None

def criar_aba_sumario(sheet_name: str, total_pedidos_auditados: int, lista_divergencias: list):
    try:
        client = get_sheets_client()
        spreadsheet = client.open(sheet_name)
        
        try:
            worksheet_to_delete = spreadsheet.worksheet("Sumário")
            spreadsheet.del_worksheet(worksheet_to_delete)
        except gspread.exceptions.WorksheetNotFound:
            pass 
        
        sheet = spreadsheet.add_worksheet(title="Sumário", rows="100", cols="20")
        print("INFO: Criando/Atualizando aba de Sumário...")
        
        df = pd.DataFrame(lista_divergencias)
        total_divergencias = len(df)
        valor_total_diferenca = df['diferenca_valor'].sum() if not df.empty else 0
        
        resumo_geral = [
            ["Sumário da Auditoria"],
            ["Data da Execução", datetime.now().strftime('%d/%m/%Y %H:%M:%S')],
            [],
            ["**Resultados Gerais**"],
            ["Total de Pedidos para Auditoria", total_pedidos_auditados],
            ["Total de Divergências Encontradas", total_divergencias],
            ["Valor Total da Diferença", f"R$ {valor_total_diferenca:.2f}".replace('.', ',')]
        ]
        sheet.update('A1', resumo_geral, value_input_option='USER_ENTERED')
        sheet.format('A1:A4', {'textFormat': {'bold': True}})
        
        if not df.empty:
            sumario_transportadora = df.groupby('transportadora').agg(
                total_divergencias=('id_pedido', 'count'),
                valor_total_diferenca=('diferenca_valor', 'sum')
            ).reset_index().sort_values(by='valor_total_diferenca', ascending=False)
            
            sumario_transportadora['valor_total_diferenca'] = sumario_transportadora['valor_total_diferenca'].apply(
                lambda x: f"R$ {x:.2f}".replace('.', ',')
            )
            
            dados_para_sheets = [["**Divergências por Transportadora**"]]
            dados_para_sheets.append(sumario_transportadora.columns.tolist())
            dados_para_sheets.extend(sumario_transportadora.values.tolist())
            
            proxima_linha = len(resumo_geral) + 3
            sheet.update(f'A{proxima_linha}', dados_para_sheets, value_input_option='USER_ENTERED')
            sheet.format(f'A{proxima_linha}', {'textFormat': {'bold': True}})
            sheet.format(f'A{proxima_linha + 1}:{chr(ord("A") + len(sumario_transportadora.columns))}{proxima_linha + 1}', {'textFormat': {'bold': True}})
            
        print("SUCESSO: Aba de Sumário criada/atualizada.")
    except Exception as e:
        print(f"ERRO: Falha ao criar a aba de sumário. Detalhe: {e}")