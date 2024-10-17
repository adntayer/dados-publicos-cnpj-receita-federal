import os

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db
from dados_publicos_cnpj_receita_federal.settings import DB_URI
from dados_publicos_cnpj_receita_federal.settings import FOLDER_UNLOAD
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_EMPRESAS
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_ESTABELECIMENTOS
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_REGIME_TRIBUTARIO
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_SIMPLES
from dados_publicos_cnpj_receita_federal.settings import TABLE_NAME_SOCIOS

_log = SetupLogger('io.unload')


def unload_safra(safra, unload_file_format='parquet', threads=4, chunk_size=2_000_000, export_path=None):
    """
    Descarrega dados das tabelas especificadas do banco de dados para arquivos em um formato especificado.

    Esta função descarrega dados de uma lista predefinida de tabelas para arquivos no formato `parquet` (padrão),
    ou outros formatos se implementados. Os dados são divididos em blocos para processamento eficiente e exportados
    para um caminho padrão ou um caminho de exportação personalizado.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote ou período de dados a ser descarregado.

    unload_file_format : str, opcional, padrão='parquet'
        O formato de arquivo para exportação dos dados. Formatos suportados: 'parquet'.
        Outros formatos irão gerar um erro de `NotImplementedError`.

    threads : int, opcional, padrão=4
        O número de threads a serem usadas para descarregar dados simultaneamente. Esse parâmetro é usado no comando
        COPY para descarregar dados do banco de dados.

    chunk_size : int, opcional, padrão=2_000_000
        O número de linhas por bloco a ser descarregado de cada tabela. Os dados serão exportados em múltiplos blocos
        se o número total de linhas exceder esse valor.

    export_path : str ou None, opcional, padrão=None
        O caminho personalizado para exportação dos dados descarregados. Se None, os dados serão exportados para o diretório
        padrão sob a pasta correspondente à safra fornecida. Se um caminho for especificado, a função criará os diretórios
        necessários sob o caminho fornecido.

    Raises:
    ------
    NotImplementedError
        Se um formato de arquivo não suportado for especificado para descarregamento (atualmente apenas 'parquet' é suportado).

    Exemplo:
    --------
    unload_safra('2024-10', unload_file_format='parquet', threads=8, chunk_size=1000000, export_path='/caminho/para/exportar')
    """
    list_tbls = [
        'cnaes',
        TABLE_NAME_REGIME_TRIBUTARIO,
        TABLE_NAME_SIMPLES,
        TABLE_NAME_SOCIOS,
        TABLE_NAME_EMPRESAS,
        TABLE_NAME_ESTABELECIMENTOS,
    ]
    PATH_FOLDER_RAW_SAFRA = os.path.join(PATH_FOLDER_RAW, safra)
    if not export_path:
        _log.info('unload | caminho de exportação não disponível... exportando para o caminho padrão')
        path_unload = os.path.join(PATH_FOLDER_RAW_SAFRA, FOLDER_UNLOAD)
        os.makedirs(path_unload, exist_ok=True)
    else:
        _log.info(f'unload | criando pasta para {export_path=}')
        path_unload = os.path.join(export_path, safra)
        os.makedirs(path_unload, exist_ok=True)

    with connect_db(db_uri=DB_URI) as db:
        for tbl in list_tbls:
            _log.info(f"unload | descarregando {tbl=} para '{unload_file_format}'")
            if unload_file_format == 'parquet':
                PATH_FOLDER_UNLOAD_PARQUET = os.path.join(path_unload, 'format_parquet')
                os.makedirs(PATH_FOLDER_UNLOAD_PARQUET, exist_ok=True)
                PATH_FOLDER_UNLOAD_PARQUET_TBL = os.path.join(PATH_FOLDER_UNLOAD_PARQUET, tbl)
                os.makedirs(PATH_FOLDER_UNLOAD_PARQUET_TBL, exist_ok=True)

                total_rows = db.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
                rounds = max(total_rows // chunk_size, 1)
                _log.info(f'unload | {tbl=} com {total_rows:_} linhas em {rounds} blocos')
                for offset in range(0, total_rows, chunk_size):
                    file_number = offset // chunk_size
                    output_file = f'{PATH_FOLDER_UNLOAD_PARQUET_TBL}/{tbl}_{file_number}.parquet'

                    _log.info(f'unload | [{tbl=}, {unload_file_format}] | bloco {file_number} de {rounds} ({file_number/rounds:.1%}) -> para o arquivo {output_file}')
                    db.execute(
                        f"""
                                    SET progress_bar_time = 1;
                                    SET threads = {threads};
                                    COPY (SELECT * FROM {tbl} LIMIT {chunk_size} OFFSET {offset}) TO '{output_file}'
                                    (FORMAT PARQUET, COMPRESSION ZSTD);
                                """,
                    )
            else:
                msg = f'unload | {unload_file_format=} não implementado'
                _log.info(msg)
                raise NotImplementedError(msg)
