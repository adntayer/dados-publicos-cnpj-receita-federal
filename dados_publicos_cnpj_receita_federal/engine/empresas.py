import os
import time

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db
from dados_publicos_cnpj_receita_federal.engine._core import load_data_to_duckdb
from dados_publicos_cnpj_receita_federal.settings import DB_URI
from dados_publicos_cnpj_receita_federal.settings import FOLDER_UNZIP
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_EMPRESAS

_log = SetupLogger('engine.empresas')


def processar_empresas(safra):
    """
    Processa e carrega os dados relacionados a empresas para o banco de dados DuckDB.

    Esta função realiza os seguintes passos:
    1. Carrega os dados brutos de arquivos CSV para uma tabela temporária no DuckDB.
    2. Aplica transformações nos dados, como:
        - Preencher o `cnpj_basico` para garantir que tenha 8 caracteres.
        - Adicionar descrições à coluna `porte` com base em códigos predefinidos.
        - Adicionar descrições às colunas `natureza_juridica` e `qualificacao_responsavel` unindo com outras tabelas de referência.
    3. Registra o progresso e o tempo de execução de cada etapa.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote ou período de dados a ser processado. A função procurará os arquivos CSV
        na pasta correspondente ao safra informado.

    Exemplo:
    --------
    processar_empresas('2024-10')
    """
    start = time.time()
    table_name = TABLE_NAME_EMPRESAS

    _log.info(f'{table_name=} | carregando para o DuckDB')
    dict_column_types = {
        'cnpj_basico': 'VARCHAR',
        'razao_social': 'VARCHAR',
        'codigo_natureza_juridica': 'VARCHAR',
        'codigo_qualificacao_responsavel': 'VARCHAR',
        'capital_social': 'DOUBLE',
        'porte': 'VARCHAR',
        'ente_federativo_responsavel': 'VARCHAR',
    }
    path = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, '*.EMPRECSV')
    load_data_to_duckdb(db_uri=DB_URI, path=path, dict_column_types=dict_column_types, table_name=table_name, safra=safra)
    _log.info(f'{table_name=} | TRANSFORMAÇÃO')

    with connect_db(db_uri=DB_URI) as db:
        _log.info(f'{table_name=} | ajustando cnpj_basico para 8 caracteres')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET cnpj_basico = LPAD(cnpj_basico, 8, '0');
                """,
        )

        _log.info(f'{table_name=} | ajustando porte')
        dict_situation_desc = {
            '01': 'NÃO INFORMADO',
            '02': 'MICRO EMPRESA',
            '03': 'EMPRESA DE PEQUENO PORTE',
            '05': 'DEMAIS',
        }
        case_statement = '\n'.join(f"WHEN '{key}' THEN '{value}'" for key, value in dict_situation_desc.items())
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS porte_desc;
                ALTER TABLE {table_name} ADD COLUMN porte_desc VARCHAR;
                UPDATE {table_name}
                SET porte_desc = CASE porte
                    {case_statement}
                    ELSE null
                END;
                """,
        )

        _log.info(f'{table_name=} | ajustando descrição da natureza_juridica')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS natureza_juridica;
                ALTER TABLE {table_name} ADD COLUMN natureza_juridica VARCHAR;
                UPDATE {table_name}
                SET natureza_juridica = (
                    SELECT natureza_juridica
                    FROM naturezas_juridicas
                    WHERE naturezas_juridicas.codigo = {table_name}.codigo_natureza_juridica
                    );
                """,
        )

        _log.info(f'{table_name=} | ajustando descrição da qualificacao_responsavel')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS qualificacao_responsavel;
                ALTER TABLE {table_name} ADD COLUMN qualificacao_responsavel VARCHAR;
                UPDATE {table_name}
                SET qualificacao_responsavel = (
                    SELECT descricao
                    FROM qualificacoes_socios
                    WHERE qualificacoes_socios.codigo = {table_name}.codigo_qualificacao_responsavel
                    );
                """,
        )

        end = time.time()
        row_count = db.sql(f'SELECT COUNT(*) FROM  {table_name}').fetchone()[0]
        db.sql(f'select * from {table_name}').show()
        _log.info(f'{table_name=} | tabela {table_name} carregada em {end - start:.1f} segundos com {row_count:_} linhas')
