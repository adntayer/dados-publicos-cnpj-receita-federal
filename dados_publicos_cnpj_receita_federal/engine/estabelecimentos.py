import os
import time

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db
from dados_publicos_cnpj_receita_federal.engine._core import load_data_to_duckdb
from dados_publicos_cnpj_receita_federal.settings import DB_URI
from dados_publicos_cnpj_receita_federal.settings import FOLDER_UNZIP
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_ESTABELECIMENTOS

_log = SetupLogger('engine.estabelecimentos')


def processar_estabelecimentos(safra):
    """
    Processa e carrega os dados relacionados a estabelecimentos para o banco de dados DuckDB.

    Esta função realiza os seguintes passos:
    1. Carrega os dados brutos de arquivos CSV para uma tabela temporária no DuckDB.
    2. Aplica transformações nos dados, como:
        - Preencher as colunas `cnpj_basico`, `cnpj_ordem` e `cnpj_dv` para garantir os comprimentos corretos.
        - Criar uma coluna consolidada `cnpj` combinando `cnpj_basico`, `cnpj_ordem` e `cnpj_dv`.
        - Reformatar as colunas de data (`data_situacao_cadastral`, `data_inicio_atividade`, `data_situacao_especial`) para o formato `YYYY-MM-DD`.
        - Adicionar descrições nas colunas `matriz_filial`, `situacao_cadastral`, `situacao_cadastral_motivo` e `municipio` unindo com outras tabelas de referência.
    3. Registra o progresso e o tempo de execução de cada etapa.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote ou período de dados a ser processado. A função procurará os arquivos CSV
        na pasta correspondente ao safra informado.

    Exemplo:
    --------
    processar_estabelecimentos('2024-10')
    """
    start = time.time()
    table_name = TABLE_NAME_ESTABELECIMENTOS

    _log.info(f'{table_name=} | carregando para o DuckDB')
    dict_column_types = {
        'cnpj_basico': 'VARCHAR',
        'cnpj_ordem': 'VARCHAR',
        'cnpj_dv': 'VARCHAR',
        'matriz_filial': 'VARCHAR',
        'nome_fantasia': 'VARCHAR',
        'situacao_cadastral': 'VARCHAR',
        'data_situacao_cadastral': 'VARCHAR',
        'situacao_cadastral_motivo_codigo': 'VARCHAR',
        'nome_na_cidade_no_exterior': 'VARCHAR',
        'pais': 'VARCHAR',
        'data_inicio_atividade': 'VARCHAR',
        'cnae_principal': 'VARCHAR',
        'cnae_secundarios': 'VARCHAR',
        'tipo_de_logradouro': 'VARCHAR',
        'logradouro': 'VARCHAR',
        'numero': 'VARCHAR',
        'complemento': 'VARCHAR',
        'bairro': 'VARCHAR',
        'cep': 'VARCHAR',
        'uf': 'VARCHAR',
        'municipio_codigo': 'VARCHAR',
        'tel1_dd': 'VARCHAR',
        'tel1': 'VARCHAR',
        'tel2_dd': 'VARCHAR',
        'tel2': 'VARCHAR',
        'fax_dd': 'VARCHAR',
        'fax': 'VARCHAR',
        'email': 'VARCHAR',
        'situacao_especial': 'VARCHAR',
        'data_situacao_especial': 'VARCHAR',
    }
    path = os.path.join(PATH_FOLDER_RAW, safra, FOLDER_UNZIP, '*.ESTABELE')
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

        _log.info(f'{table_name=} | ajustando cnpj_ordem para 4 caracteres')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET cnpj_ordem = LPAD(cnpj_ordem, 4, '0');
                """,
        )

        _log.info(f'{table_name=} | ajustando cnpj_dv para 2 caracteres')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET cnpj_dv = LPAD(cnpj_dv, 4, '0');
                """,
        )

        _log.info(f'{table_name=} | criando coluna cnpj')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS cnpj;
                ALTER TABLE {table_name} ADD COLUMN cnpj VARCHAR;
                UPDATE {table_name}
                SET cnpj = cnpj_basico || cnpj_ordem || cnpj_dv
                """,
        )

        _log.info(f'{table_name=} | ajustando data_situacao_cadastral')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_situacao_cadastral = SUBSTR(data_situacao_cadastral, 1, 4) || '-' ||
                                              SUBSTR(data_situacao_cadastral, 5, 2) || '-' ||
                                              SUBSTR(data_situacao_cadastral, 7, 2);
                """,
        )

        _log.info(f'{table_name=} | ajustando data_inicio_atividade')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_inicio_atividade = SUBSTR(data_inicio_atividade, 1, 4) || '-' ||
                                            SUBSTR(data_inicio_atividade, 5, 2) || '-' ||
                                            SUBSTR(data_inicio_atividade, 7, 2);
                """,
        )

        _log.info(f'{table_name=} | ajustando data_situacao_especial')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET data_situacao_especial = SUBSTR(data_situacao_especial, 1, 4) || '-' ||
                                             SUBSTR(data_situacao_especial, 5, 2) || '-' ||
                                             SUBSTR(data_situacao_especial, 7, 2);
                """,
        )

        _log.info(f'{table_name=} | ajustando descrição de matriz_filial')
        dict_situacao_cadastral_descricao = {
            '1': 'MATRIZ',
            '2': 'FILIAL',
        }
        case_statement = '\n'.join(f"WHEN '{key}' THEN '{value}'" for key, value in dict_situacao_cadastral_descricao.items())
        db.sql(
            f"""
                SET progress_bar_time = 1;
                UPDATE {table_name}
                SET matriz_filial = CASE matriz_filial
                    {case_statement}
                    ELSE null
                END;
                """,
        )

        _log.info(f'{table_name=} | ajustando descrição da situacao_cadastral')
        dict_situacao_cadastral_descricao = {
            '01': 'NULA',
            '02': 'ATIVA',
            '03': 'SUSPENSA',
            '04': 'INAPTA',
            '08': 'BAIXADA',
        }
        case_statement = '\n'.join(f"WHEN '{key}' THEN '{value}'" for key, value in dict_situacao_cadastral_descricao.items())
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS situacao_cadastral_descricao;
                ALTER TABLE {table_name} ADD COLUMN situacao_cadastral_descricao VARCHAR;
                UPDATE {table_name}
                SET situacao_cadastral_descricao = CASE situacao_cadastral
                    {case_statement}
                    ELSE null
                END;
                """,
        )

        _log.info(f'{table_name=} | ajustando descrição do situacao_cadastral_motivo')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS situacao_cadastral_motivo;
                ALTER TABLE {table_name} ADD COLUMN situacao_cadastral_motivo VARCHAR;
                UPDATE {table_name}
                SET situacao_cadastral_motivo = (
                    SELECT motivo
                    FROM motivos
                    WHERE motivos.codigo = {table_name}.situacao_cadastral_motivo_codigo
                    );
                """,
        )

        _log.info(f'{table_name=} | ajustando descrição do municipio')
        db.sql(
            f"""
                SET progress_bar_time = 1;
                ALTER TABLE {table_name} DROP COLUMN IF EXISTS municipio;
                ALTER TABLE {table_name} ADD COLUMN municipio VARCHAR;
                UPDATE {table_name}
                SET municipio = (
                    SELECT municipio
                    FROM municipios
                    WHERE municipios.codigo = {table_name}.municipio_codigo
                    );
                """,
        )

        end = time.time()
        row_count = db.sql(f'SELECT COUNT(*) FROM  {table_name}').fetchone()[0]
        db.sql(f'select * from {table_name}').show()
        _log.info(f'{table_name=} | tabela {table_name} carregada em {end - start:.1f} segundos com {row_count:_} linhas')
