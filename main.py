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
        cursor_factory=RealDictCursor
    )

@app.get("/dados")
def obter_dados():
    try:
        conn = conectar_pg()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tb_fundo")  # Edite aqui
        dados = cursor.fetchall()
        cursor.close()
        conn.close()
        return dados
    except Exception as e:
        return {"erro": str(e)}
