from bs4 import BeautifulSoup

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.io.requests_config import create_custom_session

_log = SetupLogger('io.safra_atual')


def safra_atual():
    safra = list_safras()[-1]
    _log.info(f'safra_atual | {safra=}')
    return safra


def list_safras():
    url = 'https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/'

    session = create_custom_session()
    response = session.get(url)
    list_safras = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('/'):
                folder_name = href.rstrip('/')
                if '-' in folder_name:
                    list_safras.append(folder_name)

    return sorted(list_safras)


if __name__ == '__main__':
    safra_atual()
