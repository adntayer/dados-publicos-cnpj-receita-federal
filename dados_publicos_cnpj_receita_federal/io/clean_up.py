import shutil

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW

_log = SetupLogger('io.clean_up')


def clean():
    """
    Esta função deleta a pasta de dados de modo a limpar o 'ambiente'.

    Exemplo:
    --------
    clean()
    """
    _log.info(f'clean | Deletando pasta {PATH_FOLDER_RAW}')
    shutil.rmtree(PATH_FOLDER_RAW)
    _log.info('clean | Pasta deletada')
