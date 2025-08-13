from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()


def init_pool() -> SimpleConnectionPool:
    """Initialize PostgreSQL connection pool."""
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
    """Create the connection pool when the application starts."""
    app.state.pool = init_pool()


@app.on_event("shutdown")
def shutdown_event() -> None:
    """Close all connections when the application stops."""
    pool = getattr(app.state, "pool", None)
    if pool:
        pool.closeall()

@app.get("/dados")
def obter_dados():
    try:
        conn = app.state.pool.getconn()
        try:
            # Query com múltiplas linhas usando triple-quoted string
            query = """
        SELECT
	case
when u.nm_unidade = 'Campos' then 'Itaperuna Muriae'
else u.nm_unidade
end as nm_unidade,
	f.id AS id_fundo,
	f.nm_fundo,
	f.dt_baile AS dt_baile,
	CASE
		WHEN f.tp_servico = '1' THEN 'Pacote'
		WHEN f.tp_servico = '2' THEN 'Assessoria'
		WHEN f.tp_servico = '3' THEN 'Super Integrada'
	END AS tp_servico,
	CASE
		WHEN f.situacao = 1 THEN 'Não mapeado'
		WHEN f.situacao = 2 THEN 'Mapeado'
		WHEN f.situacao = 3 THEN 'Em negociação'
		WHEN f.situacao = 4 THEN 'Concorrente'
		WHEN f.situacao = 5 THEN 'Comum'
		WHEN f.situacao = 6 THEN 'Juntando'
		WHEN f.situacao = 7 THEN 'Junção'
		WHEN f.situacao = 8 THEN 'Unificando'
		WHEN f.situacao = 9 THEN 'Unificado'
		WHEN f.situacao = 10 THEN 'Rescindindo'
		WHEN f.situacao = 11 THEN 'Rescindido'
		WHEN f.situacao = 12 THEN 'Realizado'
		WHEN f.situacao = 13 THEN 'Desistente'
		WHEN f.situacao = 14 THEN 'Pendente'
	END AS situacao_fundo,
	CASE
		WHEN (
			f.dt_contrato IS NULL
			OR f.dt_contrato > f.dt_cadastro
		) THEN f.dt_cadastro
		WHEN f.dt_contrato IS NOT NULL THEN f.dt_contrato
	END AS dt_contrato_fundo,
    f.dt_cadastro AS dt_cadastro,
	c.nm_curso AS curso_fundo,
	COUNT(i.id) AS integrantes_ativos,
	f.num_alunos_turma AS tat_inicial,
	f.integrantes_previstos_contrato AS mac_inicial,
	f.vl_orcamento_contrato AS maf_inicial,
	f.tat_replanejado,
	f.mac_replanejado,
	f.maf_replanejado,
	CASE 
	WHEN us.enabled IS true THEN us.nome 
   ELSE null 
   END AS consultor_relacionamento,
    CASE
	WHEN f.tipocliente_id = '7' THEN 'EMPRESARIAL'
	WHEN f.tipocliente_id = '14' THEN 'FRANQUIAS'
	WHEN f.tipocliente_id = '15' THEN 'FUNDO DE FORMATURA'
	WHEN f.tipocliente_id = '16' THEN 'OUTROS'
	END AS tipo_cliente_fundo,
	fc.id AS id_juncao
FROM
	tb_fundo f
	JOIN tb_unidade u ON u.id = f.unidade_id
	LEFT JOIN tb_integrante i ON i.fundo_id = f.id
	JOIN tb_curso c ON c.id = f.curso_id
	LEFT JOIN tb_usuario us ON us.id = f.consultorplanejamento_id
	LEFT JOIN tb_grupo_fundos_correlatos fc ON f.id_grupo_fundos_correlatos = fc.id

WHERE
	f.fl_ativo IS true
	AND (i.fl_ativo IS true or i.fl_ativo is null )
	AND u.categoria = '2' -- FRANQUIA VIVA EVENTOS
	AND f.tipocliente_id = '15' -- FUNDO DE FORMATURA
	AND COALESCE(f.is_fundo_teste, 'False') = 'False'
	AND (i.nu_status NOT IN (11, 9, 8, 13) or i.nu_status is null ) -- INTEGRANTE REATIVAOD ,INTEGRANTE JUNTADO, INTEGRANTE FUNDO, CADASTRO ERRADO, TEMPORÁRIO, MIGRAÇÃO DE FUNDOS
GROUP BY
	u.id,
	f.id,
	c.id,
	us.nome,
	fc.id,
	us.enabled
ORDER BY
	u.nm_unidade
        """

            with conn.cursor() as cursor:
                cursor.execute(query)
                dados = cursor.fetchall()
            return dados
        finally:
            app.state.pool.putconn(conn)
    except Exception as e:
        return {"erro": str(e)}
