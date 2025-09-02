# core/sheets.py

import gspread
import os
from datetime import datetime
import pandas as pd
import time
import traceback

COLORS = {
    "intelipost_dark_green": {"red": 0/255, "green": 95/255, "blue": 33/255},
    "intelipost_vibrant_green": {"red": 0/255, "green": 200/255, "blue": 0/255},
    "white": {"red": 1.0, "green": 1.0, "blue": 1.0},
    "light_gray_background": {"red": 0.95, "green": 0.95, "blue": 0.95},
    "light_red_background": {"red": 0.988, "green": 0.898, "blue": 0.898},
    "light_green_background": {"red": 0.894, "green": 0.964, "blue": 0.913},
    "black": {"red": 0.0, "green": 0.0, "blue": 0.0},
    "red_text": {"red": 204/255, "green": 0/255, "blue": 0/255},
    "gray_border": {"red": 0.7, "green": 0.7, "blue": 0.7},
}

def get_sheets_client():
    return gspread.service_account(filename='credentials.json')

def _execute_batch_update(spreadsheet, all_requests: list, batch_size: int = 50000):
    """
    Executa as requisições de batch_update em lotes para evitar o erro de limite da API.
    """
    if not all_requests:
        return
    
    try:
        for i in range(0, len(all_requests), batch_size):
            batch = all_requests[i:i + batch_size]
            requests_body = {'requests': batch}
            print(f"INFO (SHEETS): Enviando lote de {len(batch)} requisições de formatação...")
            spreadsheet.batch_update(requests_body)
            time.sleep(1) # Pequena pausa para não sobrecarregar a API
        
        print("SUCESSO: Formatação profissional aplicada com sucesso.")
            
    except Exception as e:
        print(f"AVISO: Falha ao aplicar formatação avançada. Detalhe: {e}")

def _recreate_worksheet(spreadsheet, title, rows, cols):
    try:
        old_sheet = spreadsheet.worksheet(title)
        if old_sheet:
            spreadsheet.del_worksheet(old_sheet)
    except gspread.exceptions.WorksheetNotFound:
        pass
    return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

def reportar_divergencias(lista_divergencias: list, sheet_name: str, client_id: int, start_date: str, end_date: str, recipient_email: str):
    try:
        gspread_client = get_sheets_client()
        template_name = os.getenv("GOOGLE_SHEET_TEMPLATE_NAME")
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

        if not template_name or not folder_id:
            raise ValueError("GOOGLE_SHEET_TEMPLATE_NAME e GOOGLE_DRIVE_FOLDER_ID devem estar no .env")

        try:
            spreadsheet = gspread_client.open(sheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            template_spreadsheet = gspread_client.open(template_name)
            copied_file = gspread_client.copy(template_spreadsheet.id, title=sheet_name, copy_permissions=True)
            drive_service = gspread_client.drive_service
            file_meta = drive_service.files().get(fileId=copied_file.id, fields='parents').execute()
            previous_parents = ",".join(file_meta.get('parents'))
            drive_service.files().update(fileId=copied_file.id, addParents=folder_id, removeParents=previous_parents).execute()
            spreadsheet = gspread_client.open_by_key(copied_file.id)
            if recipient_email:
                spreadsheet.share(recipient_email, perm_type='user', role='writer', notify=False)
            try:
                default_sheet = spreadsheet.worksheet("Página1")
                if default_sheet:
                    spreadsheet.del_worksheet(default_sheet)
            except gspread.exceptions.WorksheetNotFound:
                pass
        
        sheet_title = "Divergências Detalhadas"
        sheet = _recreate_worksheet(spreadsheet, title=sheet_title, rows=len(lista_divergencias) + 100, cols=30)
        sheet_id = sheet.id
        
        if not lista_divergencias:
            return spreadsheet.url

        df = pd.DataFrame(lista_divergencias)
        df.sort_values(by=['id_pedido', 'campo'], ascending=[True, False], inplace=True)

        header = [
            'Pedido', 'Pedido Canal Venda', 'Canal Venda', 'Nota Fiscal', 'Chave Acesso (CT-e)', 'Transportadora', 
            'CEP Origem', 'CEP Destino', 'Cidade Destino', 'Campo Divergente', 'Valor Esperado (Regra)', 
            'Valor Cobrado (Fatura)', 'Diferença', 'Status', 'Margem Aplicada', 'Volumes do Pedido', 
            'Soma Pesos Declarados (kg)', 'Peso Cubado Total (API)', 'Peso Cobrado (API)', 'Dimensões (Volumes)'
        ]
        
        colunas_ordenadas = [
            'id_pedido', 'pedido_canal_venda', 'canal_venda', 'nota_fiscal', 'chave_acesso', 'transportadora',
            'cep_origem', 'cep_destino', 'db_cidade_destino', 'campo', 'valor_banco', 'valor_intelipost', 
            'diferenca_valor', 'status', 'margem_aplicada', 'numero_volume', 'soma_peso_declarado', 
            'api_peso_cubado', 'api_peso_cobrado', 'api_dimensoes'
        ]
        
        for col in colunas_ordenadas:
            if col not in df.columns:
                df[col] = ''
            
        rows_to_add = df[colunas_ordenadas].values.tolist()

        start_date_formatted = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d/%m/%Y')
        end_date_formatted = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d/%m/%Y')
        
        header_data = [
            ["Relatório de Auditoria de Frete Completo"],
            [f"Cliente: {client_id}"],
            [f"Período Analisado: {start_date_formatted} a {end_date_formatted}"],
            [f"Relatório Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"]
        ]
        
        sheet.update('A1:A4', header_data)
        sheet.update('A6', [header])
        if rows_to_add:
            sheet.update('A7', rows_to_add)
        print(f"SUCESSO: {len(rows_to_add)} divergências detalhadas escritas na planilha.")

        all_formatting_requests = []
        num_cols = len(header)
        
        all_formatting_requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': num_cols}}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'horizontalAlignment': 'CENTER', 'textFormat': {'foregroundColor': COLORS['white'], 'fontSize': 16, 'bold': True}}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': num_cols}}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 2, 'endRowIndex': 3, 'startColumnIndex': 0, 'endColumnIndex': num_cols}}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 0, 'endColumnIndex': num_cols}}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background'], 'textFormat': {'bold': True}}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 1, 'endRowIndex': 4}, 'fields': 'userEnteredFormat'}},
        ])
        
        all_formatting_requests.append({'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': 0, 'endColumnIndex': num_cols}, 'fields': 'userEnteredFormat'}})
        
        colunas_para_mesclar = [
            'Pedido', 'Pedido Canal Venda', 'Canal Venda', 'Nota Fiscal', 'Chave Acesso (CT-e)', 'Transportadora',
            'CEP Origem', 'CEP Destino', 'Cidade Destino', 'Volumes do Pedido', 'Soma Pesos Declarados (kg)', 
            'Peso Cubado Total (API)', 'Peso Cobrado (API)', 'Dimensões (Volumes)'
        ]
        
        indices_colunas = [header.index(col) for col in colunas_para_mesclar if col in header]
        start_row_api = 6
        current_group_start = start_row_api
        use_light_gray_background = False
        border_style = {'style': 'SOLID', 'width': 1, 'color': COLORS['gray_border']}

        for i in range(1, len(df)):
            if df.iloc[i]['id_pedido'] != df.iloc[i-1]['id_pedido']:
                group_end_index = start_row_api + i
                if group_end_index > current_group_start:
                    bg_color = COLORS['light_gray_background'] if use_light_gray_background else COLORS['white']
                    all_formatting_requests.append({'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': bg_color, 'horizontalAlignment': 'CENTER'}},'range': {'sheetId': sheet_id, 'startRowIndex': current_group_start, 'endRowIndex': group_end_index}, 'fields': 'userEnteredFormat(backgroundColor,horizontalAlignment)'}})
                    for col_index in indices_colunas:
                        merge_range = {'sheetId': sheet_id, 'startRowIndex': current_group_start, 'endRowIndex': group_end_index, 'startColumnIndex': col_index, 'endColumnIndex': col_index + 1}
                        all_formatting_requests.append({'mergeCells': {'range': merge_range, 'mergeType': 'MERGE_COLUMNS'}})
                        all_formatting_requests.append({'repeatCell': {'cell': {'userEnteredFormat': {'verticalAlignment': 'MIDDLE'}}, 'range': merge_range, 'fields': 'userEnteredFormat.verticalAlignment'}})
                
                all_formatting_requests.append({'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': group_end_index - 1, 'endRowIndex': group_end_index, 'startColumnIndex': 0, 'endColumnIndex': num_cols}, 'bottom': border_style}})
                current_group_start = group_end_index
                use_light_gray_background = not use_light_gray_background
        
        last_group_end_index = len(df) + start_row_api
        if last_group_end_index > current_group_start:
            bg_color = COLORS['light_gray_background'] if use_light_gray_background else COLORS['white']
            all_formatting_requests.append({'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': bg_color, 'horizontalAlignment': 'CENTER'}}, 'range': {'sheetId': sheet_id, 'startRowIndex': current_group_start, 'endRowIndex': last_group_end_index}, 'fields': 'userEnteredFormat(backgroundColor,horizontalAlignment)'}})
            for col_index in indices_colunas:
                merge_range = {'sheetId': sheet_id, 'startRowIndex': current_group_start, 'endRowIndex': last_group_end_index, 'startColumnIndex': col_index, 'endColumnIndex': col_index + 1}
                all_formatting_requests.append({'mergeCells': {'range': merge_range, 'mergeType': 'MERGE_COLUMNS'}})
                all_formatting_requests.append({'repeatCell': {'cell': {'userEnteredFormat': {'verticalAlignment': 'MIDDLE'}}, 'range': merge_range, 'fields': 'userEnteredFormat.verticalAlignment'}})
        
        all_formatting_requests.append({'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': last_group_end_index - 1, 'endRowIndex': last_group_end_index, 'startColumnIndex': 0, 'endColumnIndex': num_cols}, 'bottom': border_style}})

        all_formatting_requests.append({'updateSheetProperties': {'properties': {'sheetId': sheet_id, 'gridProperties': {'frozenRowCount': 6}}, 'fields': 'gridProperties.frozenRowCount'}})
        all_formatting_requests.append({'setBasicFilter': {'filter': {'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': len(rows_to_add) + 6, 'startColumnIndex': 0, 'endColumnIndex': num_cols}}}})
        all_formatting_requests.extend([
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 6, 'startColumnIndex': 0, 'endColumnIndex': num_cols}], 'booleanRule': {'condition': {'type': 'TEXT_CONTAINS', 'values': [{'userEnteredValue': 'superior'}]}, 'format': {'backgroundColor': COLORS['light_green_background']}}}, 'index': 0}},
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 6, 'startColumnIndex': 0, 'endColumnIndex': num_cols}], 'booleanRule': {'condition': {'type': 'TEXT_CONTAINS', 'values': [{'userEnteredValue': 'inferior'}]}, 'format': {'backgroundColor': COLORS['light_red_background']}}}, 'index': 1}}
        ])
        
        colunas_largas = ['Volumes do Pedido', 'Dimensões (Volumes)', 'Pedido Canal Venda', 'Status', 'Margem Aplicada']
        
        indices_colunas_largas = [header.index(col) for col in colunas_largas if col in header]

        for col_idx in indices_colunas_largas:
            all_formatting_requests.append({"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": col_idx, "endIndex": col_idx + 1}, "properties": {"pixelSize": 200}, "fields": "pixelSize"}})
            all_formatting_requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 6, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1}, "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}}, "fields": "userEnteredFormat.wrapStrategy"}})

        all_formatting_requests.append({'autoResizeDimensions': {'dimensions': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': num_cols}}})
        
        _execute_batch_update(spreadsheet, all_formatting_requests)
        
        return spreadsheet.url
        
    except Exception as e:
        print(f"ERRO CRÍTICO AO GERAR RELATÓRIO NO GOOGLE SHEETS. Detalhe: {e}")
        traceback.print_exc()
        return None

def criar_aba_sumario(spreadsheet, df_divergencias, total_pedidos_auditados):
    try:
        if df_divergencias.empty:
            return

        df_custo = df_divergencias[df_divergencias['campo'] == 'Custo'].copy()
        total_divergencias = df_divergencias['id_pedido'].nunique()
        valor_pago_a_menos = abs(df_custo[df_custo['diferenca_valor'] < 0]['diferenca_valor'].sum())
        valor_pago_a_mais = df_custo[df_custo['diferenca_valor'] > 0]['diferenca_valor'].sum()
            
        saldo_final = valor_pago_a_menos - valor_pago_a_mais
        percentual_divergencia = (total_divergencias / total_pedidos_auditados if total_pedidos_auditados > 0 else 0)

        sumario_sheet = _recreate_worksheet(spreadsheet, title="Sumário", rows="200", cols="20")
        sheet_id = sumario_sheet.id
        
        header_data = [["Dashboard de Auditoria de Frete"], [spreadsheet.title.replace("Auditoria Frete - ", "")]]
        sumario_sheet.update('A1', header_data, value_input_option='USER_ENTERED')
        
        kpi_gerais = [
            ["Visão Geral da Auditoria"], ["Total de Pedidos Auditados", total_pedidos_auditados],
            ["Pedidos com Divergência", total_divergencias], ["Percentual com Divergência", f"{percentual_divergencia:.2%}"]
        ]
        sumario_sheet.update('A4', kpi_gerais, value_input_option='USER_ENTERED')
        
        kpi_financeiro = [
            ["🔴 PREJUÍZO CLIENTE (Valor Pago a Mais)"], [valor_pago_a_mais],
            ["Valor a ser contestado/ressarcido."], [], ["SALDO FINAL DA AUDITORIA"], [saldo_final]
        ]
        sumario_sheet.update('D4', kpi_financeiro, value_input_option='USER_ENTERED')
        
        kpi_credito = [
            ["🟢 CRÉDITO TRANSPORTADORA (Valor Pago a Menos)"], [valor_pago_a_menos],
            ["Valor a ser pago/complementado à transportadora."]
        ]
        sumario_sheet.update('F4', kpi_credito, value_input_option='USER_ENTERED')

        if not df_custo.empty:
            df_custo['prejuizo_cliente'] = df_custo['diferenca_valor'].apply(lambda x: x if x > 0 else 0)
            df_custo['credito_transportadora'] = df_custo['diferenca_valor'].apply(lambda x: abs(x) if x < 0 else 0)
            
            sumario_transportadora = df_custo.groupby('transportadora').agg(
                n_divergencias=('id_pedido', 'nunique'),
                prejuizo_cliente=('prejuizo_cliente', 'sum'),
                credito_transportadora=('credito_transportadora', 'sum')
            ).reset_index()
            sumario_transportadora['saldo'] = sumario_transportadora['credito_transportadora'] - sumario_transportadora['prejuizo_cliente']
            sumario_transportadora = sumario_transportadora.sort_values(by='saldo', ascending=True)
            
            table_header = ["Análise Financeira por Transportadora (Divergências de Custo)"]
            table_data = [
                ["Transportadora", "Nº de Pedidos com Divergência de Custo", "Prejuízo Cliente", "Crédito Transportadora", "Saldo"]
            ] + sumario_transportadora.values.tolist()
            sumario_sheet.update('A11', [table_header], value_input_option='USER_ENTERED')
            sumario_sheet.update('A12', table_data, value_input_option='USER_ENTERED')

        requests_body = {'requests': []}
        requests = requests_body['requests']
        currency_format = {'numberFormat': {'type': 'CURRENCY', 'pattern': 'R$ #,##0.00'}}
        border_style = {'style': 'SOLID', 'width': 1, 'color': COLORS['gray_border']}
        
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 7}}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'fontSize': 16, 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 0, 'endRowIndex': 1}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': 7}}},
        ])
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 0, 'endColumnIndex': 2}}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4}, 'fields': 'userEnteredFormat'}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background']}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 7, 'startColumnIndex': 0, 'endColumnIndex': 3}, 'fields': 'userEnteredFormat'}},
        ])
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 3, 'endColumnIndex': 5}}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_red_background']}}, 'fields': 'userEnteredFormat'}},
            {'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'top': border_style, 'bottom': border_style, 'left': border_style, 'right': border_style}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'textFormat': {'fontSize': 14, 'bold': True}, 'horizontalAlignment': 'CENTER', **currency_format}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 3, 'endColumnIndex': 5}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': 3, 'endColumnIndex': 5}}},
        ])
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 4, 'startColumnIndex': 5, 'endColumnIndex': 7}}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_green_background']}}, 'fields': 'userEnteredFormat'}},
            {'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': 3, 'endRowIndex': 7, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'top': border_style, 'bottom': border_style, 'left': border_style, 'right': border_style}},
            {'repeatCell': {'cell': {'userEnteredFormat': {'textFormat': {'fontSize': 14, 'bold': True}, 'horizontalAlignment': 'CENTER', **currency_format}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 4, 'endRowIndex': 5, 'startColumnIndex': 5, 'endColumnIndex': 7}, 'fields': 'userEnteredFormat'}},
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 5, 'endRowIndex': 6, 'startColumnIndex': 5, 'endColumnIndex': 7}}},
        ])
        requests.extend([
            {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 7, 'endRowIndex': 8, 'startColumnIndex': 3, 'endColumnIndex': 7}}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 7, 'endRowIndex': 9}, 'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background']}}, 'fields': 'userEnteredFormat'}},
            {'updateBorders': {'range': {'sheetId': sheet_id, 'startRowIndex': 7, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}, 'top': border_style, 'bottom': border_style, 'left': border_style, 'right': border_style}},
            {'repeatCell': {'range': {'sheetId': sheet_id, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}, 'cell': {'userEnteredFormat': {'textFormat': {'fontSize': 16, 'bold': True}, 'horizontalAlignment': 'CENTER', **currency_format}}, 'fields': 'userEnteredFormat'}},
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}], 'booleanRule': {'condition': {'type': 'NUMBER_GREATER', 'values': [{'userEnteredValue': '0'}]}, 'format': {'textFormat': {'foregroundColor': COLORS['intelipost_vibrant_green']}}}}, 'index': 0}},
            {'addConditionalFormatRule': {'rule': {'ranges': [{'sheetId': sheet_id, 'startRowIndex': 8, 'endRowIndex': 9, 'startColumnIndex': 3, 'endColumnIndex': 7}], 'booleanRule': {'condition': {'type': 'NUMBER_LESS', 'values': [{'userEnteredValue': '0'}]}, 'format': {'textFormat': {'foregroundColor': COLORS['red_text']}}}}, 'index': 1}}
        ])
        if not df_divergencias.empty and not df_custo.empty:
            requests.extend([
                {'mergeCells': {'range': {'sheetId': sheet_id, 'startRowIndex': 10, 'endRowIndex': 11, 'startColumnIndex': 0, 'endColumnIndex': 5}}},
                {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['intelipost_dark_green'], 'textFormat': {'foregroundColor': COLORS['white'], 'bold': True}, 'horizontalAlignment': 'CENTER'}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 10, 'endRowIndex': 11}, 'fields': 'userEnteredFormat'}},
                {'repeatCell': {'cell': {'userEnteredFormat': {'backgroundColor': COLORS['light_gray_background'], 'textFormat': {'bold': True}}}, 'range': {'sheetId': sheet_id, 'startRowIndex': 11, 'endRowIndex': 12}, 'fields': 'userEnteredFormat'}},
                {'repeatCell': {'cell': {'userEnteredFormat': currency_format}, 'range': {'sheetId': sheet_id, 'startRowIndex': 12, 'startColumnIndex': 2, 'endColumnIndex': 5}, 'fields': 'userEnteredFormat(numberFormat)'}}
            ])
        requests.append({'autoResizeDimensions': {'dimensions': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 7}}})
        
        _execute_batch_update(spreadsheet, requests)
        # Removido o print de sucesso daqui, pois já está dentro da _execute_batch_update
        
    except Exception as e:
        print(f"Erro ao criar aba de sumário: {str(e)}")
        traceback.print_exc()