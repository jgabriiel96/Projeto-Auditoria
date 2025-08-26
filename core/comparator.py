# core/comparator.py

def encontrar_divergencias(row):
    """
    Recebe uma linha de DataFrame e retorna uma LISTA de dicionários, 
    um para cada divergência encontrada (custo, peso, etc.).
    """
    divergencias_encontradas = []

    # --- 1. AUDITORIA DE CUSTO ---
    try:
        custo_db = float(row['so_provider_shipping_costs'])
        valor_intelipost = float(row.get('valor_intelipost'))
        config_margem = row['config_margem']
        
        limite_tolerancia = 0.0
        margem_formatada = "N/A"
        if config_margem:
            margem_type = config_margem.get('type')
            if margem_type == 'ABSOLUTE':
                limite_tolerancia = float(config_margem.get('value', 0.0))
                margem_formatada = f"R$ {limite_tolerancia:.2f} (Fixo)"
            elif margem_type == 'PERCENTAGE':
                percentual = float(config_margem.get('value', 0.0))
                limite_tolerancia = round(valor_intelipost * (percentual / 100.0), 2)
                margem_formatada = f"{percentual}% (R$ {limite_tolerancia:.2f})"
            elif margem_type == 'SYSTEM_DEFAULT':
                percentual = 1.0
                limite_tolerancia = round(valor_intelipost * (percentual / 100.0), 2)
                margem_formatada = f"Padrão do Sistema (1.0% = R$ {limite_tolerancia:.2f})"
            elif margem_type == 'DYNAMIC_CHOICE':
                absolute_val = float(config_margem.get('absolute_value', 0.0))
                percentage_val = float(config_margem.get('percentage_value', 0.0))
                limite_absoluto = absolute_val
                limite_percentual = round(valor_intelipost * (percentage_val / 100.0), 2)
                limite_tolerancia = max(limite_absoluto, limite_percentual)
                margem_formatada = (f"Dinâmico (Maior entre R$ {limite_absoluto:.2f} e "
                                    f"{percentage_val}% = R$ {limite_percentual:.2f}) -> "
                                    f"Aplicado: R$ {limite_tolerancia:.2f}")

        diferenca_numerica = round(custo_db - valor_intelipost, 2)

        if abs(diferenca_numerica) > limite_tolerancia:
            status_conferencia = "Custo superior ao da Fatura" if diferenca_numerica > 0 else "Custo inferior ao da Fatura"
            divergencia_custo = {
                'campo': 'Custo', 'valor_banco': custo_db, 'valor_intelipost': valor_intelipost,
                'diferenca_valor': diferenca_numerica, 'status': status_conferencia, 
                'margem_aplicada': margem_formatada
            }
            divergencias_encontradas.append(divergencia_custo)

    except (ValueError, TypeError, KeyError):
        pass

    # --- 2. AUDITORIA DE PESO ---
    try:
        peso_db = float(row.get('db_peso_declarado'))
        peso_api = float(row.get('api_peso_cobrado'))
        
        limite_tolerancia_peso = round(peso_db * 0.05, 3) 
        
        diferenca_peso = round(peso_db - peso_api, 3)

        if abs(diferenca_peso) > limite_tolerancia_peso:
            status_peso = "Peso declarado superior ao cobrado" if diferenca_peso > 0 else "Peso declarado inferior ao cobrado"
            divergencia_peso = {
                'campo': 'Peso (kg)', 'valor_banco': peso_db, 'valor_intelipost': peso_api,
                'diferenca_valor': diferenca_peso, 'status': status_peso, 
                'margem_aplicada': f"5% ({limite_tolerancia_peso} kg)"
            }
            divergencias_encontradas.append(divergencia_peso)

    except (ValueError, TypeError, KeyError):
        pass
        
    # --- ANEXA DADOS DE CONTEXTO A TODAS AS DIVERGÊNCIAS ENCONTRADAS ---
    if divergencias_encontradas:
        dados_contexto = {
            'id_pedido': row.get('so_order_number'),
            'chave_acesso': row.get('chave_cte'),
            'transportadora': row.get('lp_name'),
            'canal_venda': row.get('db_canal_venda'),
            'pedido_canal_venda': row.get('db_pedido_canal_venda'),
            'nota_fiscal': row.get('nota_fiscal'),
            'cep_origem': row.get('cep_origem'),
            'cep_destino': row.get('cep_destino'),
            'db_cidade_destino': row.get('db_cidade_destino'), # <-- CORREÇÃO: Campo adicionado ao contexto
            'db_peso_declarado': row.get('db_peso_declarado'),
            'api_peso_fisico': row.get('api_peso_fisico'),
            'api_peso_cubado': row.get('api_peso_cubado'),
            'api_peso_cobrado': row.get('api_peso_cobrado'),
            'api_dimensoes': row.get('api_dimensoes'),
        }
        for div in divergencias_encontradas:
            div.update(dados_contexto)
        
        return divergencias_encontradas

    return None