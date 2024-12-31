import os
import time

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db
from dados_publicos_cnpj_receita_federal.engine._core import load_data_to_duckdb
from dados_publicos_cnpj_receita_federal.settings import DB_URI
from dados_publicos_cnpj_receita_federal.settings import FOLDER_UNZIP
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_REGIME_TRIBUTARIO

_log = SetupLogger('engine.regime_tributario')


def processar_regime_tributario(safra):
    """
    Processa e carrega os dados do regime tributário para o banco de dados DuckDB.

    Esta função realiza os seguintes passos:
    1. Carrega os dados de vários arquivos CSV (representando diferentes regimes tributários) em tabelas temporárias separadas:
        - Lucro Presumido
        - Lucro Arbitrado
        - Lucro Real
        - Imunes
    2. Combina os dados dessas tabelas individuais em uma única tabela `regime_tributario` usando uma consulta `UNION ALL`.
    3. Limpa os dados removendo a pontuação (pontos, barras e hífens) da coluna `cnpj`.
    4. Registra o progresso e o tempo de execução de cada etapa.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote ou período de dados a ser processado. A função procurará os arquivos CSV
        na pasta correspondente ao safra informado.

    Exemplo:
    --------
    processar_regime_tributario('2024-10')
    """
    start = time.time()
    table_name = TABLE_NAME_REGIME_TRIBUTARIO

    _log.info(f'{table_name=} | carregando para o DuckDB')
    dict_column_types = {
        'ano': 'VARCHAR',
        'cnpj': 'VARCHAR',
        'cnpj_da_scp': 'VARCHAR',
        'forma_de_tributacao': 'VARCHAR',
        'quantidade_de_escrituracoes': 'INTEGER',
    }
    path_lucro_presumido = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, 'Lucro Presumido*')
    load_data_to_duckdb(db_uri=DB_URI, path=path_lucro_presumido, dict_column_types=dict_column_types, table_name=table_name + '_lucro_presumido', safra=safra)

    path_lucro_arbitrado = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, 'Lucro Arbitrado*')
    load_data_to_duckdb(db_uri=DB_URI, path=path_lucro_arbitrado, dict_column_types=dict_column_types, table_name=table_name + '_lucro_arbitrado', safra=safra, sep=',', header='true')

    path_lucro_real = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, 'Lucro Real*')
    load_data_to_duckdb(db_uri=DB_URI, path=path_lucro_real, dict_column_types=dict_column_types, table_name=table_name + '_lucro_real', safra=safra, sep=',', header='true')

    path_imune = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, 'Imunes*')
    load_data_to_duckdb(db_uri=DB_URI, path=path_imune, dict_column_types=dict_column_types, table_name=table_name + '_imune', safra=safra)

    _log.info(f'{table_name=} | TRANSFORMAÇÃO')

    with connect_db(db_uri=DB_URI) as db:
        _log.info(f'{table_name=} | unindo todas as tabelas de regimes')
        db.execute(
            f"""
            SET progress_bar_time = 1;
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS
            SELECT * FROM regime_tributario_lucro_presumido
            UNION ALL
            SELECT * FROM regime_tributario_lucro_arbitrado
            UNION ALL
            SELECT * FROM regime_tributario_lucro_real
            UNION ALL
            SELECT * FROM regime_tributario_imune;
            DROP TABLE regime_tributario_lucro_presumido;
            DROP TABLE regime_tributario_imune;
            DROP TABLE regime_tributario_lucro_arbitrado;
            DROP TABLE regime_tributario_lucro_real;
            """,
        )
        _log.info(f'{table_name=} | limpando cnpj')
        db.execute(
            f"""
                    SET progress_bar_time = 1;
                    UPDATE {table_name}
                    SET cnpj = REPLACE(REPLACE(REPLACE(cnpj, '.', ''), '/', ''), '-', '');
                    """,
        )

        end = time.time()
        row_count = db.sql(f'SELECT COUNT(*) FROM  {table_name}').fetchone()[0]
        db.sql(f'select * from {table_name}').show()
        _log.info(f'{table_name=} | tabela {table_name} carregada em {end - start:.1f} segundos com {row_count:_} linhas')
