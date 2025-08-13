from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente (útil para testes locais)
load_dotenv()

# --- MUDANÇA 1: INICIALIZAÇÃO DO POOL ---
# O pool de conexões é criado aqui, de forma global e mais segura.
pool = None
try:
    pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        cursor_factory=RealDictCursor
    )
except psycopg2.OperationalError as e:
    # Se falhar aqui (ex: firewall, credenciais erradas), o erro aparecerá nos logs da Vercel.
    print(f"ERRO CRÍTICO: Falha ao inicializar o pool de conexões. {e}")


app = FastAPI()


@app.get("/")
def health_check():
    """Endpoint para verificar se a API está no ar."""
    return {"status": "ok"}


# --- MUDANÇA 2: ESTRUTURA DO ENDPOINT ---
@app.get("/dados")
def obter_dados():
    # Verifica se o pool de conexões foi criado com sucesso.
    if not pool:
        raise HTTPException(
            status_code=503,
            detail="Serviço indisponível: pool de conexões com o banco de dados não foi inicializado."
        )

    conn = None
    try:
        # Pega uma conexão do pool.
        conn = pool.getconn()
        with conn.cursor() as cursor:
            # Sua query SQL original foi mantida aqui.
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
            cursor.execute(query)
            dados = cursor.fetchall()
        
        return dados

    except Exception as e:
        # Retorna um erro HTTP 500 claro se algo der errado.
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
        
    finally:
        # Garante que a conexão SEMPRE seja devolvida ao pool.
        if conn:
            pool.putconn(conn)
