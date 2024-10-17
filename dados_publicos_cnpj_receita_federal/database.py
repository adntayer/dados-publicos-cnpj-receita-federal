import contextlib
from typing import Generator

import duckdb
from duckdb import DuckDBPyConnection

from dados_publicos_cnpj_receita_federal import SetupLogger

_log = SetupLogger('database')


@contextlib.contextmanager
def connect_db(db_uri=None) -> Generator[DuckDBPyConnection, None, None]:
    """
    Um gerenciador de contexto que conecta ao banco de dados DuckDB e cede a conexão para uso.

    Esta função gerencia o ciclo de vida de uma conexão com o banco de dados DuckDB, garantindo que a conexão seja
    corretamente estabelecida no início e fechada ao final, mesmo em caso de exceção. Ela cede a conexão ativa do banco
    de dados (`db`) para uso dentro do bloco `with`, permitindo a execução segura de consultas no banco de dados.

    Se nenhum `db_uri` for fornecido, ele usará o URI padrão especificado nas configurações.

    Parâmetros:
    ----------
    db_uri : str, opcional
        O URI para o banco de dados DuckDB ao qual se conectar. Se não for fornecido, o URI será carregado da configuração
        `DB_URI`.

    Cede:
    ------
    DuckDBPyConnection
        Um objeto de conexão ativa com o banco de dados DuckDB, que pode ser usado para executar consultas.

    Exemplo:
    --------
    com connect_db('caminho_para_o_banco.duckdb') como db:
        resultado = db.execute("SELECT * FROM tabela").fetchall()

    # Se nenhum URI for fornecido, a função usará o URI padrão:
    com connect_db() como db:
        resultado = db.execute("SELECT * FROM tabela").fetchall()
    """
    if not db_uri:
        from dados_publicos_cnpj_receita_federal.settings import DB_URI

        db_uri = DB_URI

    try:
        _log.info('connect_db | conectando ao DuckDB')
        db = duckdb.connect(db_uri)
        yield db
    except Exception as e:
        _log.error(f'connect_db | erro durante a conexão ou execução no banco de dados: {e}')
        raise
    finally:
        _log.info('connect_db | fechando a conexão com o DuckDB')
        db.close()
        _log.info('connect_db | conexão fechada')
