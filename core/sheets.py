# core/sheets.py (Versão Profissional com Formatação Avançada)

import gspread
import os
from datetime import datetime
import pandas as pd
import time
import math
import traceback

# --- CONFIGURAÇÕES DE ESTILO (PALETA INTELIPOST ATUALIZADA) ---
COLORS = {
    "intelipost_dark_green": {"red": 0/255, "green": 95/255, "blue": 33/255},
    "intelipost_vibrant_green": {"red": 0/255, "green": 200/255, "blue": 0/255},
    "white": {"red": 1.0, "green": 1.0, "blue": 1.0},
    "light_gray_background": {"red": 0.95, "green": 0.95, "blue": 0.95},
    "light_red_background": {"red": 0.988, "green": 0.898, "blue": 0.898},
    "light_green_background": {"red": 0.894, "green": 0.964, "blue": 0.913},
    "black": {"red": 0.0, "green": 0.0, "blue": 0.0},
    "red_text": {"red": 204/255, "green": 0/255, "blue": 0/255},
    "gray_border": {"red": 0.8, "green": 0.8, "blue": 0.8},
}

def get_sheets_client():
    return gspread.service_account(filename='credentials.json')

def _execute_batch_update(spreadsheet, requests_body):
    try:
        spreadsheet.batch_update(requests_body)
    except Exception as e:
        print(f"AVISO: Falha ao aplicar formatação avançada. Detalhe: {e}")

def reportar_divergencias(lista_divergencias: list, sheet_name: str, client_id: int, start_date: str, end_date: str, recipient_email: str):
    try:
        gspread_client = get_sheets_client()
        
        template_name = os.getenv("GOOGLE_SHEET_TEMPLATE_NAME")
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

        if not template_name or not folder_id:
            raise ValueError("GOOGLE_SHEET_TEMPLATE_NAME e GOOGLE_DRIVE_FOLDER_ID devem estar no .env")

        try:
            spreadsheet = gspread_client.open(sheet_name)
            print(f"INFO: Planilha '{sheet_name}' já existe. Limpando e reformatando...")
            
            worksheets = spreadsheet.worksheets()
            for i in range(len(worksheets) - 1, 0, -1):
                spreadsheet.del_worksheet(worksheets[i])
            
            sheet = spreadsheet.sheet1
            sheet.clear()

        except gspread.exceptions.WorksheetNotFound:
            print(f"INFO: Planilha '{sheet_name}' não encontrada. Criando uma nova...")
            template_spreadsheet = gspread_client.open(template_name)
            copied_file = gspread_client.copy(template_spreadsheet.id, title=sheet_name, copy_permissions=True)
            
            drive_service = gspread_client.drive_service
            
            file = drive_service.files().get(field='parents', fileId=copied_file.id).execute()
            previous_parents = ",".join(file.get('parents'))
            drive_service.files().update(fileId=copied_file.id, addParents=folder_id, removeParents=previous_parents, fields='id, parents').execute()
            
            spreadsheet = gspread_client.open_by_key(copied_file.id)
            if recipient_email:
                spreadsheet.share(recipient_email, perm_type='user', role='writer', notify=False)
            sheet = spreadsheet.sheet1
        
        sheet.update_title("Divergências Detalhadas")
        sheet_id = sheet.id
        
        header = [
            'Pedido', 'Transportadora', 'Chave Acesso (CT-e)', 'Campo Divergente',
            'Custo Pedido', 'Custo SEFAZ', 'Valor da Diferença', 
            'Margem Aplicada', 'Status'
        ]
        
        rows_to_add = []
        if lista_divergencias:
            for div in lista_divergencias:
                rows_to_add.append([
                    div.get('id_pedido'), div.get('transportadora'), div.get('chave_acesso'),
                    div.get('campo'), div.get('valor_banco', 0), div.get('valor_intelipost', 0),
                    div.get('diferenca_valor', 0), div.get('margem_aplicada', 'N/A'),
                    div.get('status', '')
                ])

        header_data = [
            ["Relatório de Auditoria de Frete"],
            [f"Cliente: {client_id}"],
            [f"Período Analisado: {start_date} a {end_date}"],
            [f"Relatório Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"]
        ]
        sheet.update('A1', header_data, value_input_option='USER_ENTERED')
        sheet.update('A6', [header] + rows_to_add, value_input_option='USER_ENTERED')
        print(f"SUCESSO: {len(rows_to_add)} divergências detalhadas escritas na planilha.")

        requests_body = {'requests': []}
        requests = requests_body['requests']

        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 9}, 'mergeType': 'MERGE_ALL'}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'horizontalAlignment': 'CENTER', 'textFormat': {'foregroundColor': COLORS['white'], 'fontSize': 16, 'bold': True}}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': 9}, 'mergeType': 'MERGE_ALL'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 2, 'endRowIndex': 3, 'startColumnIndex': 0, 'endColumnIndex': 9}, 'mergeType': 'MERGE_ALL'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 0, 'endColumnIndex': 9}, 'mergeType': 'MERGE_ALL'}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background'], 'textFormat': {'bold': True}}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 1, 'endRowIndex': 4}, 'fields': 'userEnteredFormat'}},
        ])
        
        requests.append({'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': 0, 'endColumnIndex': 9}, 'fields': 'userEnteredFormat'}})
        
        currency_format = {'numberFormat': {'type': 'CURRENCY', 'pattern': 'R$ #,##0.00'}}
        requests.append({'repeatCell': {'cell': {'userEnteredFormat': currency_format}, 'range': {'sheetId': sheet_id, 'startRowIndex': 6, 'startColumnIndex': 4, 'endColumnIndex': 7}, 'fields': 'userEnteredFormat(numberFormat)'}})
        
        requests.append({'updateSheetProperties': {'properties': {'sheetId': sheet_id, 'gridProperties': {'frozenRowCount': 6}}, 'fields': 'gridProperties.frozenRowCount'}})
        requests.append({'setBasicFilter': {'filter': {'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': len(rows_to_add) + 6, 'startColumnIndex': 0, 'endColumnIndex': 9}}}})
        requests.append({'addBanding': {'bandedRange': {'range': {'sheetId': sheet_id, 'startRowIndex': 6, 'startColumnIndex': 0, 'endColumnIndex': 9}, 'rowProperties': {'headerColor': COLORS['intelipost_dark_green'], 'firstBandColor': COLORS['white'], 'secondBandColor': COLORS['light_gray_background']}}}})
        requests.extend([
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 6, 'startColumnIndex': 0, 'endColumnIndex': 9}], 'booleanRule': {'condition': {'type': 'TEXT_CONTAINS', 'values': [{'userEnteredValue': 'superior'}]}, 'format': {'backgroundColor': COLORS['light_red_background']}}}, 'index': 0}},
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 6, 'startColumnIndex': 0, 'endColumnIndex': 9}], 'booleanRule': {'condition': {'type': 'TEXT_CONTAINS', 'values': [{'userEnteredValue': 'inferior'}]}, 'format': {'backgroundColor': COLORS['light_green_background']}}}, 'index': 1}}
        ])
        requests.append({'autoResizeDimensions': {'dimensions': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 9}}})
        
        _execute_batch_update(spreadsheet, requests_body)
        print("SUCESSO: Formatação profissional aplicada na aba de Divergências.")
        
        return spreadsheet.url
        
    except Exception as e:
        print(f"ERRO CRÍTICO AO GERAR RELATÓRIO NO GOOGLE SHEETS. Detalhe: {e}")
        traceback.print_exc()
        return None

def criar_aba_sumario(spreadsheet, df_divergencias, total_pedidos_auditados):
    try:
        total_divergencias = len(df_divergencias)
        valor_pago_a_menos = abs(df_divergencias[df_divergencias['diferenca_valor'] < 0]['diferenca_valor'].sum()) if not df_divergencias.empty else 0
        valor_pago_a_mais = df_divergencias[df_divergencias['diferenca_valor'] > 0]['diferenca_valor'].sum() if not df_divergencias.empty else 0
        saldo_final = valor_pago_a_menos - valor_pago_a_mais

        try:
            sumario_sheet = spreadsheet.worksheet("Sumário")
            sumario_sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            sumario_sheet = spreadsheet.add_worksheet(title="Sumário", rows="200", cols="20")
        
        sheet_id = sumario_sheet.id
        
        header_data = [
            ["Dashboard de Auditoria de Frete"],
            [spreadsheet.title.replace("Auditoria Frete - ", "")],
        ]
        sumario_sheet.update('A1', header_data, value_input_option='USER_ENTERED')
        
        kpi_gerais = [
            ["Visão Geral da Auditoria"],
            ["Total de Pedidos Auditados", total_pedidos_auditados],
            ["Pedidos com Divergência", total_divergencias],
            ["Percentual com Divergência", f"{(total_divergencias / total_pedidos_auditados if total_pedidos_auditados > 0 else 0):.2%}"]
        ]
        sumario_sheet.update('A4', kpi_gerais, value_input_option='USER_ENTERED')
        
        # ✅ NOVO LAYOUT DE DADOS PARA MAIOR CLAREZA
        kpi_financeiro = [
            ["🔴 PREJUÍZO CLIENTE (Valor Pago a Mais)"],
            [valor_pago_a_mais],
            ["Valor a ser contestado/ressarcido."],
            [],
            ["SALDO FINAL DA AUDITORIA"],
            [saldo_final]
        ]
        sumario_sheet.update('D4', kpi_financeiro, value_input_option='USER_ENTERED')
        
        kpi_credito = [
            ["🟢 CRÉDITO TRANSPORTADORA (Valor Pago a Menos)"],
            [valor_pago_a_menos],
            ["Valor a ser pago/complementado à transportadora."]
        ]
        sumario_sheet.update('F4', kpi_credito, value_input_option='USER_ENTERED')

        if not df_divergencias.empty:
            df_divergencias['prejuizo_cliente'] = df_divergencias['diferenca_valor'].apply(lambda x: x if x > 0 else 0)
            df_divergencias['credito_transportadora'] = df_divergencias['diferenca_valor'].apply(lambda x: abs(x) if x < 0 else 0)
            
            sumario_transportadora = df_divergencias.groupby('transportadora').agg(
                n_divergencias=('id_pedido', 'count'),
                prejuizo_cliente=('prejuizo_cliente', 'sum'),
                credito_transportadora=('credito_transportadora', 'sum')
            ).reset_index()
            sumario_transportadora['saldo'] = sumario_transportadora['credito_transportadora'] - sumario_transportadora['prejuizo_cliente']
            sumario_transportadora = sumario_transportadora.sort_values(by='saldo', ascending=True)
            
            table_header = ["Análise por Transportadora"]
            table_data = [
                ["Transportadora", "Nº de Divergências", "Prejuízo Cliente", "Crédito Transportadora", "Saldo"]
            ] + sumario_transportadora.values.tolist()
            sumario_sheet.update('A11', [table_header], value_input_option='USER_ENTERED')
            sumario_sheet.update('A12', table_data, value_input_option='USER_ENTERED')

        # ✅ BLOCO DE FORMATAÇÃO COMPLETAMENTE REFEITO
        requests_body = {'requests': []}
        requests = requests_body['requests']
        currency_format = {'numberFormat': {'type': 'CURRENCY', 'pattern': 'R$ #,##0.00'}}
        border_style = {'style': 'SOLID', 'width': 1, 'color': COLORS['gray_border']}
        
        # Cabeçalho Principal e KPIs Gerais
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 7}, 'mergeType': 'MERGE_ALL'}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'fontSize': 16, 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': 7}, 'mergeType': 'MERGE_ALL'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 0, 'endColumnIndex': 2}, 'mergeType': 'MERGE_ALL'}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'fields': 'userEnteredFormat'}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 7, 'startColumnIndex': 0, 'endColumnIndex': 3}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background']}}, 'fields': 'userEnteredFormat'}},
        ])

        # Caixa de Prejuízo
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'mergeType': 'MERGE_ALL'}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_red_background']}}, 'fields': 'userEnteredFormat'}},
            {'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'top': border_style, 'bottom': border_style, 'left': border_style, 'right': border_style}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'cell': {'userEnteredFormat': {'textFormat': {'fontSize': 14, 'bold': True}, 'horizontalAlignment': 'CENTER', **currency_format}}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'mergeType': 'MERGE_ALL'}},
        ])

        # Caixa de Crédito Transportadora
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'mergeType': 'MERGE_ALL'}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_green_background']}}, 'fields': 'userEnteredFormat'}},
            {'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'top': border_style, 'bottom': border_style, 'left': border_style, 'right': border_style}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'cell': {'userEnteredFormat': {'textFormat': {'fontSize': 14, 'bold': True}, 'horizontalAlignment': 'CENTER', **currency_format}}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'mergeType': 'MERGE_ALL'}},
        ])
        
        # Caixa do Saldo Final
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 7, 'endRowIndex': 8, 'startColumnIndex': 3, 'endColumnIndex': 7}, 'mergeType': 'MERGE_ALL'}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 7, 'endRowIndex': 9}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background']}}, 'fields': 'userEnteredFormat'}},
            {'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': 7, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}, 'top': border_style, 'bottom': border_style, 'left': border_style, 'right': border_style}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}, 'cell': {'userEnteredFormat': {'textFormat': {'fontSize': 16, 'bold': True}, 'horizontalAlignment': 'CENTER', **currency_format}}, 'fields': 'userEnteredFormat'}},
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}], 'booleanRule': {'condition': {'type': 'NUMBER_GREATER', 'values': [{'userEnteredValue': '0'}]}, 'format': {'textFormat': {'foregroundColor': COLORS['intelipost_vibrant_green']}}}}, 'index': 0}},
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}], 'booleanRule': {'condition': {'type': 'NUMBER_LESS', 'values': [{'userEnteredValue': '0'}]}, 'format': {'textFormat': {'foregroundColor': COLORS['red_text']}}}}, 'index': 1}}
        ])

        # Tabela por Transportadora
        if not df_divergencias.empty:
            requests.extend([
                {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 10, 'endRowIndex': 11, 'startColumnIndex': 0, 'endColumnIndex': 5}, 'mergeType': 'MERGE_ALL'}},
                {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 10, 'endRowIndex': 11}, 'fields': 'userEnteredFormat'}},
                {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background'], 'textFormat': {'bold': True}}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 11, 'endRowIndex': 12}, 'fields': 'userEnteredFormat'}},
                {'repeatCell': {'cell': {'userEnteredFormat': currency_format}, 'range': {'sheetId': sheet_id, 'startRowIndex': 12, 'startColumnIndex': 2, 'endColumnIndex': 5}, 'fields': 'userEnteredFormat(numberFormat)'}}
            ])

        requests.append({'autoResizeDimensions': {'dimensions': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 7}}})
        
        _execute_batch_update(spreadsheet, requests_body)
        print("SUCESSO: Aba de Sumário profissional criada/atualizada.")

    except Exception as e:
        print(f"Erro ao criar aba de sumário: {str(e)}")
        traceback.print_exc()