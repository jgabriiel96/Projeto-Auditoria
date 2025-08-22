# core/comparator.py

def encontrar_divergencias(pedido_db, valor_intelipost: float | None, chave_cte_externa: str, transportadora_nome: str):
    margem_de_tolerancia = 0.05
    divergencias = {
        'id_pedido': pedido_db['so_order_number'],
        'chave_acesso': chave_cte_externa,
        'margem_aplicada': margem_de_tolerancia,
        'transportadora': transportadora_nome
    }
    houve_divergencia = False
    custo_db = float(pedido_db['so_provider_shipping_costs'])
    
    if valor_intelipost is not None:
        diferenca_numerica = round(custo_db - valor_intelipost, 2)
        
        if abs(diferenca_numerica) > margem_de_tolerancia:
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