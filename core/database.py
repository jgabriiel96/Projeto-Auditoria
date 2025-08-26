# core/database.py

import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def criar_conexao():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("INFO: Conexão com o banco de dados estabelecida com sucesso.")
        return conn
    except Exception as e:
        print(f"ERRO CRÍTICO: Não foi possível conectar ao banco de dados. Detalhe: {e}")
        return None

def obter_dados_de_pedidos_especificos(client_id: int, lista_order_numbers: list) -> pd.DataFrame:
    """
    Busca no banco de dados as informações para uma lista específica de números de pedido.
    """
    if not lista_order_numbers:
        return pd.DataFrame()

    query = r"""
        SELECT
            so.so_order_number AS so_order_number,
            so.so_sales_channel AS db_canal_venda,
            
            COALESCE(
                NULLIF(so.so_sales_order_number, ''),
                substring(so.so_external_order_numbers::text FROM '"sales"\s*=>\s*"([^"]+)"')
            ) AS db_pedido_canal_venda,
            
            sovi.sovi_invoice_number AS nota_fiscal_db,
            sovi.sovi_invoice_total_value AS db_valor_total_nf,
            lp.lp_name AS lp_name,
            ec.ec_shipping_zip_code AS cep_destino_db,
            ec.ec_shipping_city AS db_cidade_destino,
            wa.wa_zip_code AS cep_origem_db,
            so.so_provider_shipping_costs AS so_provider_shipping_costs,
            sov.sov_weight AS db_peso_declarado,
            
            CONCAT(sov.sov_length, 'x', sov.sov_width, 'x', sov.sov_height) AS db_dimensoes

        FROM
            esprinter_data.shipment_order so
        INNER JOIN esprinter_data.shipment_order_volume sov ON
            sov.sov_shipment_order_id = so.so_id
        INNER JOIN esprinter_data.shipment_order_volume_invoice sovi ON
            sovi.sovi_shipment_order_volume_id = sov.sov_id
        INNER JOIN esprinter_data.delivery_method dm ON
            dm.dm_id = so.so_delivery_method_id
        INNER JOIN esprinter_data.logistic_provider lp ON
            lp.lp_id = dm.dm_logistic_provider_id
        INNER JOIN esprinter_data.warehouse_address wa ON
            wa.wa_id = so.so_warehouse_address_id
        INNER JOIN esprinter_data.end_customer ec ON
            ec.ec_id = so.so_end_customer_id
        WHERE
            so.so_client_id = %s
            AND so.so_order_number IN %s;
    """
    params = (client_id, tuple(lista_order_numbers))
    conn = criar_conexao()
    if not conn: return pd.DataFrame()
    try:
        print(f"INFO (DB): Buscando dados de {len(lista_order_numbers)} pedidos específicos no banco de dados.")
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        print(f"ERRO (DB): Falha ao obter pedidos específicos do banco de dados. Detalhe: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()