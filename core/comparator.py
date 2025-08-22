# core/comparator.py (Versão Final V2.1)

def encontrar_divergencias(pedido_db, valor_intelipost: float | None, chave_cte_externa: str, transportadora_nome: str, config_margem: dict):
    # V2 - A margem de tolerância hardcoded foi removida.
    
    # V2 - Cálculo do limite de tolerância com base na configuração da API
    limite_tolerancia = 0.0
    margem_formatada = "N/A" # Valor para o relatório
    
    if config_margem and valor_intelipost is not None:
        if config_margem.get('type') == 'ABSOLUTE':
            limite_tolerancia = float(config_margem.get('value', 0.0))
            margem_formatada = f"R$ {limite_tolerancia:.2f} (Fixo)"
        elif config_margem.get('type') == 'PERCENTAGE':
            percentual = float(config_margem.get('value', 0.0))
            # O cálculo do percentual é feito sobre o valor da Intelipost como referência
            limite_tolerancia = round(valor_intelipost * (percentual / 100.0), 2)
            margem_formatada = f"{percentual}% (R$ {limite_tolerancia:.2f})"

    divergencias = {
        'id_pedido': pedido_db['so_order_number'],
        'chave_acesso': chave_cte_externa,
        # V2 - Reporta a margem que foi de fato aplicada na auditoria
        'margem_aplicada': margem_formatada,
        'transportadora': transportadora_nome
    }
    houve_divergencia = False
    custo_db = float(pedido_db['so_provider_shipping_costs'])
    
    if valor_intelipost is not None:
        diferenca_numerica = round(custo_db - valor_intelipost, 2)
        
        # V2 - A comparação agora usa o limite de tolerância calculado dinamicamente
        if abs(diferenca_numerica) > limite_tolerancia:
            if diferenca_numerica > 0:
                status_conferencia = "Custo no pedido superior ao da Intelipost"
            else:
                status_conferencia = "Custo no pedido inferior ao da Intelipost"
            
            divergencias['valor_banco'] = custo_db
            divergencias['valor_intelipost'] = valor_intelipost
            divergencias['diferenca_valor'] = diferenca_numerica
            divergencias['status'] = status_conferencia
            divergencias['campo'] = 'Custo'
            houve_divergencia = True
            
    return divergencias if houve_divergencia else None