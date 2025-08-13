from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

app = FastAPI()

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

def init_pool() -> SimpleConnectionPool:
    """Inicializa o pool de conexões com PostgreSQL."""
    return SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        cursor_factory=RealDictCursor
    )

@app.on_event("startup")
def startup_event() -> None:
    """Cria o pool de conexões quando a aplicação inicia."""
    app.state.pool = init_pool()

@app.on_event("shutdown")
def shutdown_event() -> None:
    """Fecha todas as conexões quando a aplicação para."""
    pool = getattr(app.state, "pool", None)
    if pool:
        pool.closeall()

@app.get("/dados")
def obter_dados():
    try:
        # Obter uma conexão do pool
        conn = app.state.pool.getconn()

        try:
            # Query SQL com múltiplas linhas e formatação melhorada
            query = """
                SELECT
                    CASE
                        WHEN u.nm_unidade = 'Campos' THEN 'Itaperuna Muriae'
                        ELSE u.nm_unidade
                    END AS nm_unidade,
                    i.id AS codigo_integrante,
                    i.nm_integrante,
                    CASE
                        WHEN f.is_fundo_assessoria_pura_convertido IS TRUE THEN f.dt_conversao_ass_pura
                        WHEN f.is_fundo_assessoria_pura_convertido IS FALSE THEN i.dt_cadastro
                    END AS dt_cadastro_integrante,
                    f.id AS id_fundo,
                    '' AS nm_fundo,
                    c.nm_curso AS curso_fundo,
                    '' AS tp_servico,
                    CASE
                        WHEN (
                            f.dt_contrato IS NULL
                            OR f.dt_contrato > f.dt_cadastro
                        ) THEN f.dt_cadastro
                        WHEN f.dt_contrato IS NOT NULL THEN f.dt_contrato
                    END AS dt_contrato,
                    f.dt_cadastro AS dt_cadastro_fundo,
                    '' AS total_lancamentos,
                    fc.vl_plano AS vl_plano,
                    '' AS cadastrado_por,
                    CASE
                        WHEN us.cpf IS NULL THEN us.nome
                        ELSE NULL
                    END AS indicado_por,
                    CASE
                        WHEN us.fl_consultor_comercial IS TRUE THEN 'Sim'
                        WHEN us.fl_consultor_comercial IS FALSE THEN 'Não'
                    END AS consultor_comercial,
                    it.nm_instituicao,
                    '' AS fl_ativo,
                    '' AS tipo_cliente
                FROM
                    tb_fundo f
                    JOIN tb_unidade u ON f.unidade_id = u.id
                    JOIN tb_integrante i ON i.fundo_id = f.id
                    LEFT JOIN tb_fundo_cota fc ON fc.cota_id = i.cota_id
                        AND i.fundo_id = fc.fundo_id
                    JOIN tb_curso c ON c.id = f.curso_id
                    LEFT JOIN tb_usuario us ON us.id = i.id_usuario_indicacao
                    LEFT JOIN tb_instituicao it ON f.instituicao_id = it.id
                WHERE
                    u.categoria = '2' -- FRANQUIA VIVA EVENTOS
                    AND f.tipocliente_id IN (15, 17) -- FUNDO DE FORMATURA, PRÉ EVENTO
                    AND i.dt_cadastro >= '2019-01-01'
                    AND f.is_fundo_teste IS FALSE
                    AND i.nu_status NOT IN (11, 9, 8, 13, 14) -- INTEGRANTE JUNTADO, INTEGRANTE FUNDO, CADASTRO ERRADO, TEMPORÁRIO, MIGRAÇÃO DE FUNDOS
                    AND f.is_assessoria_pura IS FALSE
                    AND (
                        i.dt_cadastro <= '2024-03-08'
                        OR (
                            i.dt_cadastro > '2024-03-08'
                            AND i.id NOT IN (
                                SELECT
                                    i2.id
                                FROM
                                    tb_integrante i2
                                WHERE
                                    i2.forma_adesao = 6
                                    AND i2.dt_cadastro > '2024-03-08'
                            )
                        )
                    )
                ORDER BY
                    i.dt_cadastro
            """

            # Executando a query
            with conn.cursor() as cursor:
                cursor.execute(query)
                dados = cursor.fetchall()

            return dados

        finally:
            # Devolve a conexão ao pool
            app.state.pool.putconn(conn)

    except Exception as e:
        # Retorna erro, caso haja
        return {"erro": str(e)}
