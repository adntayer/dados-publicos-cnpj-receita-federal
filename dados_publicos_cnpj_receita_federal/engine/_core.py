import os

import duckdb

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db
from dados_publicos_cnpj_receita_federal.settings import DB_URI
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW


_log = SetupLogger('src.engine._core')


def load_data_to_duckdb(db_uri, path, dict_column_types, table_name, safra, sep=';', header='false'):
    """
    Carrega dados de arquivos CSV em uma tabela do DuckDB, com transformações de tipo.

    Esta função lê os dados CSV de um caminho especificado, transforma os tipos de dados de acordo com um dicionário
    de tipos de coluna fornecido, e carrega os dados em uma tabela do DuckDB. Ela cria uma tabela temporária para
    ler os dados, realiza as transformações de tipo necessárias e depois insere os dados na tabela de destino.
    Além disso, adiciona uma coluna `safra` à tabela e a atualiza com o valor fornecido.

    Parâmetros:
    ----------
    db_uri : str
        O URI para conectar ao banco de dados DuckDB.

    path : str
        O caminho para os arquivos CSV a serem carregados. Pode incluir padrões de coringa para carregar vários arquivos.

    dict_column_types : dict
        Um dicionário que mapeia os nomes das colunas para seus tipos de dados de destino (exemplo: `{'coluna1': 'INTEGER', 'coluna2': 'DOUBLE'}`).

    table_name : str
        O nome da tabela de destino onde os dados serão carregados.

    safra : str
        Um valor de string que será adicionado como uma coluna à tabela (coluna `safra`).

    sep : str, opcional
        O delimitador usado nos arquivos CSV (padrão é `';'`).

    header : str, opcional
        Especifica se os arquivos CSV contêm uma linha de cabeçalho.
        Defina como `'true'` se a primeira linha contiver os nomes das colunas (padrão é `'false'`).

    Lança:
    ------
    Exceção
        Se houver um problema durante o processo de carregamento, como a impossibilidade de ler os arquivos ou tipos de dados incorretos.

    Exemplo:
    --------
    load_data_to_duckdb(
        db_uri='caminho_do_banco.duckdb',
        path='/dados/arquivos_csv/*.csv',
        dict_column_types={'col1': 'INTEGER', 'col2': 'DOUBLE'},
        table_name='minha_tabela',
        safra='2024-10'
    )
    """
    with connect_db(db_uri=db_uri) as db:
        list_columns_names = list(dict_column_types.keys())
        _log.info(f'load_data_to_duckdb | {table_name} | carregando para a tabela temporária -> todos os arquivos em {path=}')
        try:
            db.execute(
                f"""
                    SET progress_bar_time = 1;
                    DROP TABLE IF EXISTS temp_table;
                    CREATE TABLE temp_table AS
                    SELECT * FROM
                        read_csv_auto(
                                '{path}',
                                sep='{sep}',
                                header = {header},
                                quote='"',
                                union_by_name=true,
                                names = {list_columns_names}
                            )
                """,
            )
        except duckdb.duckdb.IOException:
            msg = f"load_data_to_duckdb | Verifique se 'safra' existe: {path}"
            _log.critical(msg)
            raise Exception(msg)

        _log.info(f'load_data_to_duckdb | {table_name} | criando e carregando a tabela final (tipos transformados)')
        columns_definitions = ',\n'.join([f'{name} {dtype}' for name, dtype in dict_column_types.items()])
        select_statements = []
        for name, dtype in dict_column_types.items():
            if dtype == 'DOUBLE':
                select_statements.append(f"REPLACE(CAST({name} AS VARCHAR), ',', '.')::DOUBLE AS {name}")
            elif dtype == 'INTEGER':
                select_statements.append(f"CAST(REPLACE(CAST({name} AS VARCHAR), ',', '') AS INTEGER) AS {name}")
            else:
                select_statements.append(f'CAST({name} AS VARCHAR) AS {name}')

        select_clause = ', '.join(select_statements)

        db.execute(
            f"""
                SET progress_bar_time = 1;
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name} (
                    {columns_definitions}
                );
                INSERT INTO {table_name}
                SELECT
                    {select_clause}
                FROM temp_table;
            """,
        )

        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS safra;
                ALTER TABLE {table_name} ADD COLUMN safra VARCHAR;
                UPDATE {table_name}
                SET safra = '{safra}';
                """,
        )

        db.execute('DROP TABLE IF EXISTS temp_table;')
        _log.info(f'load_data_to_duckdb | {table_name} | tabela carregada')

        process_mapping(db_uri, safra)


def process_mapping(db_uri, safra):
    """
    Processa e carrega os dados de mapeamento no banco de dados DuckDB.

    Esta função verifica se as tabelas de mapeamento específicas existem no banco de dados (como `qualificacoes_socios`,
    `pais`, `naturezas_juridicas`, `municipios`, `motivos`, e `cnaes`). Se uma tabela não existir, a função carrega
    os dados CSV correspondentes para o banco de dados. Para algumas tabelas como `cnaes`, os dados são recuperados
    de uma fonte externa e processados antes de serem carregados no banco de dados.

    Parâmetros:
    ----------
    db_uri : str
        O URI para conectar ao banco de dados DuckDB.

    safra : str
        O identificador do lote ou período de dados a ser processado. Isso é usado para construir caminhos
        para os arquivos CSV que precisam ser carregados.

    Exemplo:
    --------
    process_mapping(db_uri='caminho_do_banco.duckdb', safra='2024-10')
    """
    with connect_db(db_uri=db_uri) as db:
        if not check_table_exists(db_uri=DB_URI, table_name='qualificacoes_socios'):
            _log.info('process_mapping | indo para qualificacoes_socios')
            path = os.path.join(PATH_FOLDER_RAW, safra, 'unzip', '*.QUALSCSV')
            db.execute(
                f"""
                    SET progress_bar_time = 1;
                    DROP TABLE IF EXISTS qualificacoes_socios;
                    CREATE TABLE qualificacoes_socios AS
                    SELECT * FROM
                        read_csv_auto(
                                '{path}',
                                sep=';',
                                header = false,
                                quote='"',
                                union_by_name=true,
                                names = ['codigo', 'descricao']
                            )
                """,
            )

        if not check_table_exists(db_uri=DB_URI, table_name='pais'):
            _log.info('process_mapping | indo para pais')
            path = os.path.join(PATH_FOLDER_RAW, safra, 'unzip', '*.PAISCSV')
            db.execute(
                f"""
                    SET progress_bar_time = 1;
                    DROP TABLE IF EXISTS pais;
                    CREATE TABLE pais AS
                    SELECT * FROM
                        read_csv_auto(
                                '{path}',
                                sep=';',
                                header = false,
                                quote='"',
                                union_by_name=true,
                                names = ['codigo', 'pais']
                            )
                """,
            )

        if not check_table_exists(db_uri=DB_URI, table_name='naturezas_juridicas'):
            _log.info('process_mapping | indo para natureza juridica')
            path = os.path.join(PATH_FOLDER_RAW, safra, 'unzip', '*.NATJUCSV')
            db.execute(
                f"""
                    SET progress_bar_time = 1;
                    DROP TABLE IF EXISTS naturezas_juridicas;
                    CREATE TABLE naturezas_juridicas AS
                    SELECT * FROM
                        read_csv_auto(
                                '{path}',
                                sep=';',
                                header = false,
                                quote='"',
                                union_by_name=true,
                                names = ['codigo', 'natureza_juridica']
                            )
                """,
            )

        if not check_table_exists(db_uri=DB_URI, table_name='municipios'):
            _log.info('process_mapping | indo para municipios')
            path = os.path.join(PATH_FOLDER_RAW, safra, 'unzip', '*.MUNICCSV')
            db.execute(
                f"""
                    SET progress_bar_time = 1;
                    DROP TABLE IF EXISTS municipios;
                    CREATE TABLE municipios AS
                    SELECT * FROM
                        read_csv_auto(
                                '{path}',
                                sep=';',
                                header = false,
                                quote='"',
                                union_by_name=true,
                                names = ['codigo', 'municipio']
                            )
                """,
            )

        if not check_table_exists(db_uri=DB_URI, table_name='motivos'):
            _log.info('process_mapping | indo para motivos')
            path = os.path.join(PATH_FOLDER_RAW, safra, 'unzip', '*.MOTICSV')
            db.execute(
                f"""
                    SET progress_bar_time = 1;
                    DROP TABLE IF EXISTS motivos;
                    CREATE TABLE motivos AS
                    SELECT * FROM
                        read_csv_auto(
                                '{path}',
                                sep=';',
                                header = false,
                                quote='"',
                                union_by_name=true,
                                names = ['codigo', 'motivo']
                            )
                """,
            )

        if not check_table_exists(db_uri=DB_URI, table_name='cnaes'):
            _log.info('process_mapping | indo para cnaes')
            db.execute(
                """
                    SET progress_bar_time = 1;
                    DROP TABLE IF EXISTS cnaes;
                    CREATE TABLE cnaes AS
                    SELECT * FROM
                        read_csv_auto(
                                'https://servicodados.ibge.gov.br/api/v2/cnae/classes',
                                sep=',',
                                header = true,
                                quote='"',
                                union_by_name=true
                            )
                """,
            )


def check_table_exists(db_uri, table_name):
    """
    Verifica se uma tabela existe no banco de dados.

    Parâmetros:
    ----------
    db_uri : str
        O URI para conectar ao banco de dados DuckDB.

    table_name : str
        O nome da tabela a ser verificada.

    Retorna:
    --------
    bool
        Retorna True se a tabela existir, caso contrário False.
    """
    with connect_db(db_uri=db_uri) as db:
        result = db.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchall()
        return result[0][0] > 0
