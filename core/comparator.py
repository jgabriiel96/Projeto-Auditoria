# core/comparator.py

def encontrar_divergencias(pedido_db, valor_intelipost: float | None, chave_cte_externa: str, transportadora_nome: str, config_margem: dict):
    """
    Compara o custo do frete do banco de dados com o custo da Intelipost,
    aplicando uma margem de divergência configurável.

    Args:
        pedido_db (dict): Dicionário contendo dados do pedido do banco de dados.
        valor_intelipost (float | None): Valor do frete obtido da Intelipost.
        chave_cte_externa (str): Chave do CTe para referência.
        transportadora_nome (str): Nome da transportadora.
        config_margem (dict): Dicionário de configuração da margem.
                              Exemplos:
                              {'type': 'ABSOLUTE', 'value': 2.0}
                              {'type': 'PERCENTAGE', 'value': 1.5}
                              {'type': 'SYSTEM_DEFAULT'}
                              {'type': 'DYNAMIC_CHOICE', 'absolute_value': 2.0, 'percentage_value': 1.5}

    Returns:
        dict | None: Um dicionário com os detalhes da divergência, ou None se não houver.
    """
    limite_tolerancia = 0.0
    margem_formatada = "N/A"  # Valor padrão para o relatório

    if config_margem and valor_intelipost is not None:
        margem_type = config_margem.get('type')

        if margem_type == 'ABSOLUTE':
            limite_tolerancia = float(config_margem.get('value', 0.0))
            margem_formatada = f"R$ {limite_tolerancia:.2f} (Fixo)"

        elif margem_type == 'PERCENTAGE':
            percentual = float(config_margem.get('value', 0.0))
            limite_tolerancia = round(valor_intelipost * (percentual / 100.0), 2)
            margem_formatada = f"{percentual}% (R$ {limite_tolerancia:.2f})"

        # NOVA OPÇÃO: Padrão do Sistema (1%)
        elif margem_type == 'SYSTEM_DEFAULT':
            percentual = 1.0
            limite_tolerancia = round(valor_intelipost * (percentual / 100.0), 2)
            margem_formatada = f"Padrão do Sistema (1.0% = R$ {limite_tolerancia:.2f})"

        # NOVA OPÇÃO: Escolha Dinâmica (o maior entre valor fixo e percentual)
        elif margem_type == 'DYNAMIC_CHOICE':
            absolute_val = float(config_margem.get('absolute_value', 0.0))
            percentage_val = float(config_margem.get('percentage_value', 0.0))

            limite_absoluto = absolute_val
            limite_percentual = round(valor_intelipost * (percentage_val / 100.0), 2)

            # A lógica de negócio aqui é usar a maior tolerância para dar mais flexibilidade
            limite_tolerancia = max(limite_absoluto, limite_percentual)
            
            margem_formatada = (f"Dinâmico (Maior entre R$ {limite_absoluto:.2f} e "
                              f"{percentage_val}% = R$ {limite_percentual:.2f}) -> "
                              f"Aplicado: R$ {limite_tolerancia:.2f}")

    divergencias = {
        'id_pedido': pedido_db['so_order_number'],
        'chave_acesso': chave_cte_externa,
        'margem_aplicada': margem_formatada,
        'transportadora': transportadora_nome
    }
    houve_divergencia = False
    custo_db = float(pedido_db['so_provider_shipping_costs'])

    if valor_intelipost is not None:
        diferenca_numerica = round(custo_db - valor_intelipost, 2)

        if abs(diferenca_numerica) > limite_tolerancia:
            if diferenca_numerica > 0:
                status_conferencia = "Custo no pedido superior ao do SEFAZ"
            else:
                status_conferencia = "Custo no pedido inferior ao do SEFAZ"

            divergencias['valor_banco'] = custo_db
            divergencias['valor_intelipost'] = valor_intelipost
            divergencias['diferenca_valor'] = diferenca_numerica
            divergencias['status'] = status_conferencia
            divergencias['campo'] = 'Custo'
            houve_divergencia = True

    return divergencias if houve_divergencia else None