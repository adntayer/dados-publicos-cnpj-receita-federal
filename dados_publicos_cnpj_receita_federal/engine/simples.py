import os
import time

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db
from dados_publicos_cnpj_receita_federal.engine._core import load_data_to_duckdb
from dados_publicos_cnpj_receita_federal.settings import DB_URI
from dados_publicos_cnpj_receita_federal.settings import FOLDER_UNZIP
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_SIMPLES

_log = SetupLogger('engine.simples')


def processar_simples(safra):
    """
    Processa e carrega os dados relacionados ao regime tributário 'Simples Nacional' para o banco de dados DuckDB.

    Esta função realiza os seguintes passos:
    1. Carrega os dados do arquivo CSV contendo informações sobre as empresas optantes pelo regime 'Simples Nacional'.
    2. Transforma os dados, incluindo:
        - Preenchimento da coluna 'cnpj_basico' para 8 caracteres.
        - Padronização das colunas 'opcao_pelo_simples' e 'opcao_pelo_mei' para os valores 'SIM', 'NÃO' ou 'OUTROS'.
        - Formatação das colunas 'data_opcao_pelo_simples', 'data_exclusao_opcao_pelo_simples', 'data_opcao_pelo_mei' e 'data_exclusao_opcao_pelo_mei' para o formato de data padrão 'YYYY-MM-DD', ou configurando-as como NULL quando aplicável.
    3. Registra o progresso e o tempo de execução de cada etapa.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote ou período de dados a ser processado. A função procurará os arquivos CSV
        na pasta correspondente ao safra informado.

    Exemplo:
    --------
    processar_simples('2024-10')
    """
    start = time.time()
    table_name = TABLE_NAME_SIMPLES

    _log.info(f'{table_name=} | carregando para o DuckDB')
    dict_column_types = {
        'cnpj_basico': 'VARCHAR',
        'opcao_pelo_simples': 'VARCHAR',
        'data_opcao_pelo_simples': 'VARCHAR',
        'data_exclusao_opcao_pelo_simples': 'VARCHAR',
        'opcao_pelo_mei': 'VARCHAR',
        'data_opcao_pelo_mei': 'VARCHAR',
        'data_exclusao_opcao_pelo_mei': 'VARCHAR',
    }
    path = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, '*SIMPLES.CSV*')
    load_data_to_duckdb(db_uri=DB_URI, path=path, dict_column_types=dict_column_types, table_name=table_name, safra=safra)
    _log.info(f'{table_name=} | TRANSFORMAÇÃO')

    with connect_db(db_uri=DB_URI) as db:
        _log.info(f'{table_name=} | definindo cnpj_basico com 8 caracteres')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET cnpj_basico = LPAD(cnpj_basico, 8, '0');
                """,
        )

        _log.info(f'{table_name=} | ajustando opcao_pelo_simples')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET opcao_pelo_simples =
                    CASE opcao_pelo_simples
                        WHEN 'S' THEN 'SIM'
                        WHEN 'N' THEN 'NÃO'
                        ELSE 'OUTROS'
                    END;
                """,
        )

        _log.info(f'{table_name=} | ajustando data_opcao_pelo_simples')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_opcao_pelo_simples = CASE
                    WHEN data_opcao_pelo_simples = '00000000' THEN NULL
                    WHEN data_opcao_pelo_simples != '00000000' THEN SUBSTR(data_opcao_pelo_simples, 1, 4) || '-' ||
                                                                    SUBSTR(data_opcao_pelo_simples, 5, 2) || '-' ||
                                                                    SUBSTR(data_opcao_pelo_simples, 7, 2)
                    ELSE data_opcao_pelo_simples
                END
                """,
        )
        _log.info(f'{table_name=} | ajustando data_exclusao_opcao_pelo_simples')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_exclusao_opcao_pelo_simples = CASE
                    WHEN data_exclusao_opcao_pelo_simples = '00000000' THEN NULL
                    WHEN data_exclusao_opcao_pelo_simples != '00000000' THEN SUBSTR(data_exclusao_opcao_pelo_simples, 1, 4) || '-' ||
                                                                             SUBSTR(data_exclusao_opcao_pelo_simples, 5, 2) || '-' ||
                                                                             SUBSTR(data_exclusao_opcao_pelo_simples, 7, 2)
                    ELSE data_exclusao_opcao_pelo_simples
                END
                """,
        )

        _log.info(f'{table_name=} | ajustando opcao_pelo_mei')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET opcao_pelo_mei =
                    CASE opcao_pelo_mei
                        WHEN 'S' THEN 'SIM'
                        WHEN 'N' THEN 'NÃO'
                        ELSE 'OUTROS'
                    END;
                """,
        )
        _log.info(f'{table_name=} | ajustando data_opcao_pelo_mei')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_opcao_pelo_mei = CASE
                    WHEN data_opcao_pelo_mei = '00000000' THEN NULL
                    WHEN data_opcao_pelo_mei != '00000000' THEN SUBSTR(data_opcao_pelo_mei, 1, 4) || '-' ||
                                                                SUBSTR(data_opcao_pelo_mei, 5, 2) || '-' ||
                                                                SUBSTR(data_opcao_pelo_mei, 7, 2)
                    ELSE data_opcao_pelo_mei
                END
                """,
        )
        _log.info(f'{table_name=} | ajustando data_exclusao_opcao_pelo_mei')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_exclusao_opcao_pelo_mei = CASE
                    WHEN data_exclusao_opcao_pelo_mei = '00000000' THEN NULL
                    WHEN data_exclusao_opcao_pelo_mei != '00000000' THEN SUBSTR(data_exclusao_opcao_pelo_mei, 1, 4) || '-' ||
                                                                         SUBSTR(data_exclusao_opcao_pelo_mei, 5, 2) || '-' ||
                                                                         SUBSTR(data_exclusao_opcao_pelo_mei, 7, 2)
                    ELSE data_exclusao_opcao_pelo_mei
                END
                """,
        )

        end = time.time()
        row_count = db.sql(f'SELECT COUNT(*) FROM  {table_name}').fetchone()[0]
        db.sql(f'select * from {table_name}').show()
        _log.info(f'{table_name=} | tabela {table_name} carregada em {end - start:.1f} segundos com {row_count:_} linhas')
