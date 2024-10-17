import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.settings import FOLDER_ZIP
from dados_publicos_cnpj_receita_federal.settings import PATH_FOLDER_RAW

_log = SetupLogger('io.downloader')


def download_safra(safra):
    """
    Baixa os arquivos de dados para uma "safra" (lote/período) específica do portal de dados abertos da Receita Federal.

    Esta função recupera todos os arquivos .zip disponíveis para uma determinada 'safra' (período) de duas fontes principais:
    1. Os dados principais do CNPJ para a safra especificada.
    2. Os dados do regime tributário, também associados à safra.

    Ela cria as pastas locais necessárias para armazenar os arquivos .zip baixados e os baixa em série,
    garantindo que apenas os arquivos ausentes sejam baixados.

    Parâmetros:
    ----------
    safra : str
        O identificador do lote/período dos dados a serem baixados. Ele corresponde ao ano ou período
        de interesse, que é adicionado à URL base para buscar os links dos dados.

    Raises:
    ------
    Exceção
        Se nenhum link de download for encontrado para a safra especificada, uma exceção será levantada.

    Exemplo:
    --------
    download_safra('2024-10')
    """
    now = datetime.now()
    url_core = 'https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/'
    url_safra = f'{url_core}/{safra}'
    PATH_FOLDER_RAW_SAFRA = os.path.join(PATH_FOLDER_RAW, safra)
    os.makedirs(PATH_FOLDER_RAW_SAFRA, exist_ok=True)
    PATH_FOLDER_RAW_SAFRA_ZIP = os.path.join(PATH_FOLDER_RAW_SAFRA, FOLDER_ZIP)
    os.makedirs(PATH_FOLDER_RAW_SAFRA_ZIP, exist_ok=True)

    _log.info(f"download | '{safra=}' | Obtendo links para a safra {safra}")
    response = requests.get(url_safra)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    list_links_core = sorted([f"{url_safra}/{link.get('href')}" for link in links if link.get('href').endswith('.zip')])

    url_tax_regime = 'https://arquivos.receitafederal.gov.br/cnpj/regime_tributario'
    response_tax_regime = requests.get(url_tax_regime)
    soup_tax_regime = BeautifulSoup(response_tax_regime.text, 'html.parser')
    links_tax_regime = soup_tax_regime.find_all('a')
    list_links_tax_regime = sorted([f"{url_tax_regime}/{link.get('href')}" for link in links_tax_regime if link.get('href').endswith('.zip')])

    list_links = list_links_core + list_links_tax_regime

    if not list_links_core:
        msg = f"download | '{safra=}' | Nenhum link encontrado para {safra=} em '{now:%Y-%m-%d %H:%M:%S}'. Verifique aqui {url_core}"
        _log.info(msg)
        raise Exception(msg)

    _log.info(f"download | '{safra=}' | Iniciando o download da safra")
    with tqdm(total=len(list_links), desc='Arquivos baixados', leave=False) as bar:
        for url in sorted(list_links):
            if need_download(url=url, path=PATH_FOLDER_RAW_SAFRA_ZIP):
                download_file(url=url, path=PATH_FOLDER_RAW_SAFRA_ZIP)
            bar.update(1)

    _log.info('Download completo')


def need_download(url, path):
    """
    Verifica se um arquivo precisa ser baixado com base no seu tamanho e se já existe localmente.

    Esta função compara o tamanho do arquivo no servidor (obtido por meio de uma requisição HTTP HEAD) com o
    tamanho do arquivo local (se existir). Se o arquivo local estiver presente e com o mesmo tamanho do arquivo no servidor,
    retorna `False`, indicando que o arquivo não precisa ser baixado. Caso contrário, retorna `True`, indicando que o arquivo deve ser baixado.

    Parâmetros:
    ----------
    url : str
        A URL do arquivo a ser verificado para download. Esta URL é usada para obter o tamanho do arquivo no servidor.

    path : str
        O caminho do diretório local onde o arquivo será salvo. A função verifica se o arquivo já existe
        neste local e compara o tamanho com o tamanho do arquivo no servidor.

    Retorna:
    -------
    bool
        Retorna `True` se o arquivo precisar ser baixado (ou se não existir localmente ou se o tamanho for diferente do arquivo no servidor).
        Retorna `False` se o arquivo local existir e o tamanho for o mesmo do arquivo no servidor.

    Exemplo:
    --------
    precisa_download('https://.../arquivo.zip', '/caminho/local')
    """
    filename = os.path.split(url)[1]
    response = requests.head(url)
    headers = response.headers
    content_length = int(headers.get('Content-Length'))
    local_file_path = os.path.join(path, filename)

    need = True
    if content_length:
        if os.path.exists(local_file_path):
            local_file_size = os.path.getsize(local_file_path)

            if content_length == local_file_size:
                need = False
    return need


def download_file(url, path):
    """
    Baixa um arquivo de uma URL especificada e o salva no sistema de arquivos local.

    Esta função recupera um arquivo da URL fornecida e o salva no diretório local especificado.
    O arquivo é transmitido em partes para lidar com arquivos grandes de maneira eficiente,
    mostrando uma barra de progresso usando o `tqdm` para acompanhar o progresso do download.

    Parâmetros:
    ----------
    url : str
        A URL do arquivo a ser baixado. O arquivo será recuperado por meio de uma requisição HTTP GET.

    path : str
        O diretório local onde o arquivo baixado será salvo. O arquivo será nomeado com base
        no nome do arquivo extraído da URL.

    Raises:
    ------
    HTTPError
        Se a requisição HTTP para o arquivo falhar (por exemplo, se o arquivo não for encontrado ou houver um erro no servidor),
        será levantada uma `HTTPError`.

    Exemplo:
    --------
    download_file('https://.../arquivo.zip', '/diretorio/local/arquivo.zip')
    """
    filename = os.path.split(url)[1]
    local_file_path = os.path.join(path, filename)

    response = requests.get(url, stream=True, timeout=40)
    response.raise_for_status()

    with open(local_file_path, 'wb') as f:
        total_size = int(response.headers.get('Content-Length', 0))
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename, leave=False) as bar:
            for data in response.iter_content(chunk_size=1024):
                f.write(data)
                bar.update(len(data))
