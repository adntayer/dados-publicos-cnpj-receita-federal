import os
import zipfile

from tqdm import tqdm

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.settings import FOLDER_UNZIP
from dados_publicos_cnpj_receita_federal.settings import FOLDER_ZIP
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW

_log = SetupLogger('io.unzip')


def unzip_safra(safra):
    """
    Descompacta todos os arquivos no lote 'safra' especificado da pasta de arquivos .zip para o diretório de descompactação.

    Esta função procura todos os arquivos `.zip` na pasta da safra especificada, descompacta-os e salva os
    arquivos extraídos em um diretório de descompactação designado. Se nenhum arquivo .zip for encontrado,
    a função registra uma mensagem e retorna.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote de dados ou período a ser processado. A função procurará arquivos .zip na
        pasta correspondente à safra fornecida.

    Exemplo:
    --------
    unzip_safra('2024-10')
    """
    _log.info(f"unzip |'{safra=}' | Listando arquivos")
    PATH_FOLDER_RAW_SAFRA = os.path.join(PATH_FOLDER_RAW, safra)
    PATH_FOLDER_RAW_SAFRA_ZIP = os.path.join(PATH_FOLDER_RAW_SAFRA, FOLDER_ZIP)
    PATH_FOLDER_RAW_SAFRA_UNZIP = os.path.join(PATH_FOLDER_RAW_SAFRA, FOLDER_UNZIP)
    os.makedirs(PATH_FOLDER_RAW_SAFRA_UNZIP, exist_ok=True)

    _log.info(f"unzip |'{safra=}' | Iniciando a descompactação da safra")
    list_files = os.listdir(PATH_FOLDER_RAW_SAFRA_ZIP)

    if not list_files:
        _log.info(f"unzip |'{safra=}' | Nenhum arquivo .zip encontrado para a safra")
        return

    with tqdm(total=len(list_files), desc='Arquivos descompactados', leave=False) as bar:
        for file in sorted(list_files):
            unzip_file(file_path=os.path.join(PATH_FOLDER_RAW_SAFRA_ZIP, file), path_folder_unzip=PATH_FOLDER_RAW_SAFRA_UNZIP)
            bar.update(1)

    _log.info(f"unzip |'{safra=}' | Descompactação concluída")


def unzip_file(file_path, path_folder_unzip):
    """
    Extrai o conteúdo de um arquivo ZIP e o salva em um diretório especificado.

    Esta função abre um arquivo ZIP e extrai seu conteúdo. Ela grava cada arquivo do arquivo ZIP no
    diretório de destino, exibindo uma barra de progresso para acompanhar o progresso da extração. A função
    trata o conteúdo de cada arquivo lendo-o em partes e gravando-o no diretório de destino.

    Parâmetros:
    ----------
    file_path : str
        O caminho completo para o arquivo ZIP a ser extraído.

    path_folder_unzip : str
        O diretório de destino onde os arquivos extraídos serão salvos.

    Exemplo:
    --------
    unzip_file('/caminho/para/arquivo.zip', '/caminho/para/destino')
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        total_size = sum(zip_ref.getinfo(name).file_size for name in zip_ref.namelist())
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f'{os.path.basename(file_path):>25}', leave=False) as pbar:
            for file_name in zip_ref.namelist():
                file_target = os.path.join(path_folder_unzip, file_name)
                with open(file_target, 'w', encoding='UTF-8') as outfile:
                    member_fd = zip_ref.open(file_name)
                    while True:
                        x = member_fd.read(n=1024 * 1024 * 2).decode('ISO-8859-1')
                        if not x:
                            break
                        outfile.write(x)
                        pbar.update(len(x))


if __name__ == '__main__':
    unzip_safra('2024-11')
