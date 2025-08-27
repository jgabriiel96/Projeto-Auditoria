# core/comparator.py

import pandas as pd

def encontrar_divergencias(row: pd.Series) -> list | None:
    """
    Recebe uma linha de DataFrame JÁ AGREGADO POR PEDIDO e retorna uma lista 
    de dicionários para cada divergência (Custo e Peso Total).
    """
    divergencias_encontradas = []

    # --- Dados base para todas as divergências deste PEDIDO ---
    dados_base = {
        'id_pedido': row.get('so_order_number'),
        'pedido_canal_venda': row.get('db_pedido_canal_venda'),
        'canal_venda': row.get('db_canal_venda'),
        'nota_fiscal': row.get('nota_fiscal'),
        'transportadora': row.get('lp_name'),
        'chave_acesso': row.get('chave_cte'),
        'cep_origem': row.get('cep_origem_db'),
        'cep_destino': row.get('cep_destino_db'),
        'db_cidade_destino': row.get('db_cidade_destino'),
        'api_dimensoes': row.get('api_dimensoes'),
        'numero_volume': row.get('numeros_volumes'),
        'soma_peso_declarado': row.get('soma_peso_declarado'),
        'api_peso_cubado': row.get('api_peso_cubado'),
        'api_peso_cobrado': row.get('api_peso_cobrado'),
    }

    # --- 1. AUDITORIA DE CUSTO (Usa a margem configurada) ---
    try:
        custo_db = float(row.get('so_provider_shipping_costs'))
        custo_api = float(row.get('valor_intelipost'))
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
                limite_tolerancia = round(custo_api * (percentual / 100.0), 2)
                margem_formatada = f"{percentual}% (R$ {limite_tolerancia:.2f})"
            elif margem_type == 'SYSTEM_DEFAULT':
                percentual = 1.0
                limite_tolerancia = round(custo_api * (percentual / 100.0), 2)
                margem_formatada = f"Padrão do Sistema (1.0% = R$ {limite_tolerancia:.2f})"
            elif margem_type == 'DYNAMIC_CHOICE':
                absolute_val = float(config_margem.get('absolute_value', 0.0))
                percentage_val = float(config_margem.get('percentage_value', 0.0))
                limite_absoluto = absolute_val
                limite_percentual = round(custo_api * (percentage_val / 100.0), 2)
                limite_tolerancia = max(limite_absoluto, limite_percentual)
                margem_formatada = (f"Dinâmico (Maior entre R$ {limite_absoluto:.2f} e "
                                    f"{percentage_val}% = R$ {limite_percentual:.2f}) -> "
                                    f"Aplicado: R$ {limite_tolerancia:.2f}")

        diferenca_numerica = round(custo_db - custo_api, 2)

        if abs(diferenca_numerica) > limite_tolerancia:
            status_conferencia = "Custo superior ao da Fatura" if diferenca_numerica > 0 else "Custo inferior ao da Fatura"
            div_custo = dados_base.copy()
            div_custo.update({
                'campo': 'Custo',
                'valor_banco': custo_db,
                'valor_intelipost': custo_api,
                'diferenca_valor': diferenca_numerica,
                'status': status_conferencia,
                'margem_aplicada': margem_formatada
            })
            divergencias_encontradas.append(div_custo)
    except (ValueError, TypeError, KeyError):
        pass

    # --- 2. AUDITORIA DE PESO TOTAL (Sem margem) ---
    try:
        soma_peso_declarado = float(row.get('soma_peso_declarado'))
        api_peso_cubado = float(row.get('api_peso_cubado'))
        api_peso_cobrado = float(row.get('api_peso_cobrado'))

        # REGRA: O peso a ser considerado é o MAIOR entre a soma dos declarados e o cubado.
        peso_considerado = max(soma_peso_declarado, api_peso_cubado)
        
        # Comparação direta, sem margem
        if round(peso_considerado, 3) != round(api_peso_cobrado, 3):
            diferenca_peso = round(peso_considerado - api_peso_cobrado, 3)
            status_peso = f"Peso divergente. Considerado ({peso_considerado:.3f} kg) vs Cobrado ({api_peso_cobrado:.3f} kg)"
            
            div_peso = dados_base.copy()
            div_peso.update({
                'campo': 'Peso Total (kg)',
                'valor_banco': peso_considerado, # Valor esperado pela regra de negócio
                'valor_intelipost': api_peso_cobrado, # Valor que a transportadora efetivamente cobrou
                'diferenca_valor': diferenca_peso,
                'status': status_peso,
                'margem_aplicada': 'N/A (Comparação Direta)'
            })
            divergencias_encontradas.append(div_peso)
    except (ValueError, TypeError, KeyError):
        pass
        
    return divergencias_encontradas if divergencias_encontradas else None