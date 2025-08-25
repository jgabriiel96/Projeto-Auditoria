# core/comparator.py

def encontrar_divergencias(row):
    """
    Recebe uma linha de um DataFrame Pandas contendo dados mesclados da API e do banco de dados,
    e retorna um dicionário de divergência se houver uma.
    """
    try:
        custo_db = float(row['so_provider_shipping_costs'])
        valor_intelipost = float(row.get('valor_intelipost'))

    except (ValueError, TypeError):
        return None

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
        if diferenca_numerica > 0:
            status_conferencia = "Custo no pedido superior ao do SEFAZ"
        else:
            status_conferencia = "Custo no pedido inferior ao do SEFAZ"

        return {
            'id_pedido': row['so_order_number'],
            'chave_acesso': row['chave_cte'],
            'margem_aplicada': margem_formatada,
            'transportadora': row['lp_name'],
            'valor_banco': custo_db,
            'valor_intelipost': valor_intelipost,
            'diferenca_valor': diferenca_numerica,
            'status': status_conferencia,
            'campo': 'Custo'
        }

    return None