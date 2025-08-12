from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()

def conectar_pg():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        cursor_factory=RealDictCursor,
        connect_timeout=180  # Aumentando o timeout da conexão para 3 minutos
    )

@app.get("/dados")
def obter_dados(limit: int = 100, offset: int = 0):
    try:
        conn = conectar_pg()
        cursor = conn.cursor()

        # Definindo o timeout para execução da consulta para 2 minutos
        cursor.execute("SET statement_timeout = '120000';")  # Timeout de 2 minutos (120000ms)

        # Query simplificada
        query = f"""
        SELECT *
        FROM tb_fundo
        LIMIT {limit} OFFSET {offset};  -- Paginação
        """

        cursor.execute(query)
        dados = cursor.fetchall()
        cursor.close()
        conn.close()
        return dados

    except Exception as e:
        return {"erro": str(e)}
