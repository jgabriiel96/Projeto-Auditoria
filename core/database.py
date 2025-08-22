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

def obter_pedidos_para_auditoria(client_id: int, data_inicio: str, data_fim: str, lista_ids_transportadoras: list):
    query = """
        SELECT
            so.so_order_number,
            so.so_provider_shipping_costs,
            lp.lp_name
        FROM
            esprinter_data.shipment_order so
        INNER JOIN esprinter_data.delivery_method dm 
            ON dm.dm_id = so.so_delivery_method_id
        INNER JOIN esprinter_data.logistic_provider lp 
            ON lp.lp_id = dm.dm_logistic_provider_id
        WHERE
            so.so_client_id = %s
            AND so.so_created BETWEEN %s AND %s
            AND lp.lp_id IN %s;
    """
    params = (client_id, data_inicio, data_fim, tuple(lista_ids_transportadoras))
    conn = criar_conexao()
    if not conn: return pd.DataFrame()
    try:
        print(f"INFO: Buscando pedidos para o cliente {client_id} e {len(lista_ids_transportadoras)} transportadoras.")
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        print(f"ERRO: Falha ao obter pedidos do banco de dados. Detalhe: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()