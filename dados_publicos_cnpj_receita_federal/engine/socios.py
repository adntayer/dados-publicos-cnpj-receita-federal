import os
import time

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db
from dados_publicos_cnpj_receita_federal.engine._core import load_data_to_duckdb
from dados_publicos_cnpj_receita_federal.settings import DB_URI
from dados_publicos_cnpj_receita_federal.settings import FOLDER_UNZIP
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_SOCIOS

_log = SetupLogger('engine.socios')


def processar_socios(safra):
    """
    Processa e carrega dados relacionados aos sócios na base de dados DuckDB.

    Esta função realiza os seguintes passos:
    1. Carrega os dados a partir do arquivo CSV contendo informações sobre os sócios.
    2. Transforma os dados, incluindo:
        - Preenchimento da coluna 'cnpj_basico' para 8 caracteres.
        - Mapeamento da coluna 'identificador_socio' para valores legíveis ('PESSOA JURIDICA', 'PESSOA FISICA', 'ESTRANGEIRO', 'OUTROS').
        - Adição de descrições para 'qualificacao_socio' e 'qualificacao_representante_legal' com base nos códigos, realizando uma junção com a tabela `qualificacoes_socios`.
        - Formatação da coluna 'data_entrada_sociedade' para o formato padrão de data 'YYYY-MM-DD', ou atribuição de NULL quando aplicável.
        - Mapeamento da coluna 'faixa_etaria_socio_codigo' para faixas etárias legíveis e atualização com os valores correspondentes.
    3. Registra o progresso e o tempo de execução de cada etapa.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote ou período de dados a ser processado. A função procurará arquivos CSV na
        pasta correspondente ao identificador da safra fornecida.

    Exemplo:
    --------
    processar_socios('2024-10')
    """

    start = time.time()
    table_name = TABLE_NAME_SOCIOS

    _log.info(f'{table_name=} | carregando para o DuckDB')
    dict_column_types = {
        'cnpj_basico': 'VARCHAR',
        'identificador_socio': 'VARCHAR',
        'nome_razao_social_socio': 'VARCHAR',
        'documento_socio': 'VARCHAR',
        'qualificacao_socio_codigo': 'VARCHAR',
        'data_entrada_sociedade': 'VARCHAR',
        'pais': 'VARCHAR',
        'documento_representante_legal': 'VARCHAR',
        'representante_legal': 'VARCHAR',
        'qualificacao_representante_legal_codigo': 'VARCHAR',
        'faixa_etaria_socio_codigo': 'VARCHAR',
    }
    path = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, '*.SOCIOCSV')
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
        _log.info(f'{table_name=} | definindo identificador_socio')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET identificador_socio =
                    CASE identificador_socio
                        WHEN '1' THEN 'PESSOA JURIDICA'
                        WHEN '2' THEN 'PESSOA FISICA'
                        WHEN '3' THEN 'ESTRANGEIRO'
                        ELSE 'OUTROS'
                    END;
                """,
        )

        _log.info(f'{table_name=} | definindo qualificacao_socio')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS qualificacao_socio;
                ALTER TABLE {table_name} ADD COLUMN qualificacao_socio VARCHAR;
                UPDATE {table_name}
                SET qualificacao_socio = (
                    SELECT descricao
                    FROM qualificacoes_socios
                    WHERE qualificacoes_socios.codigo = {table_name}.qualificacao_socio_codigo
                    );
                """,
        )

        _log.info(f'{table_name=} | definindo data_entrada_sociedade')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_entrada_sociedade = CASE
                    WHEN data_entrada_sociedade = '00000000' THEN NULL
                    WHEN data_entrada_sociedade != '00000000' THEN SUBSTR(data_entrada_sociedade, 1, 4) || '-' ||
                                                                   SUBSTR(data_entrada_sociedade, 5, 2) || '-' ||
                                                                   SUBSTR(data_entrada_sociedade, 7, 2)
                    ELSE data_entrada_sociedade
                END
                """,
        )

        _log.info(f'{table_name=} | definindo qualificacao_representante_legal')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS qualificacao_representante_legal;
                ALTER TABLE {table_name} ADD COLUMN qualificacao_representante_legal VARCHAR;
                UPDATE {table_name}
                SET qualificacao_representante_legal = (
                    SELECT descricao
                    FROM qualificacoes_socios
                    WHERE qualificacoes_socios.codigo = {table_name}.qualificacao_representante_legal_codigo
                    );
                """,
        )

        _log.info(f'{table_name=} | definindo faixa_etaria')
        dict_faixa_etaria_desc = {
            '01': '0 a 12 anos',
            '02': '13 a 20 anos',
            '03': '21 a 30 anos',
            '04': '31 a 40 anos',
            '05': '41 a 50 anos',
            '06': '51 a 60 anos',
            '07': '61 a 70 anos',
            '08': '71 a 80 anos',
            '09': 'Maiores de 80 anos',
            '00': 'Não se aplica',
        }
        case_statement = '\n'.join(f"WHEN '{key}' THEN '{value}'" for key, value in dict_faixa_etaria_desc.items())
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS faixa_etaria_socio;
                ALTER TABLE {table_name} ADD COLUMN faixa_etaria_socio VARCHAR;
                UPDATE {table_name} SET faixa_etaria_socio_codigo = LPAD(faixa_etaria_socio_codigo, 2, '0');
                UPDATE {table_name}
                SET faixa_etaria_socio = CASE faixa_etaria_socio_codigo
                    {case_statement}
                    ELSE null
                END;
                """,
        )

        end = time.time()
        row_count = db.sql(f'SELECT COUNT(*) FROM  {table_name}').fetchone()[0]
        db.sql(f'select * from {table_name}').show()
        _log.info(f'{table_name=} | tabela {table_name} carregada em {end - start:.1f} segundos com {row_count:_} linhas')
