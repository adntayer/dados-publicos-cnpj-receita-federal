from dados_publicos_cnpj_receita_federal.engine import processar_empresas
from dados_publicos_cnpj_receita_federal.engine import processar_estabelecimentos
from dados_publicos_cnpj_receita_federal.engine import processar_regime_tributario
from dados_publicos_cnpj_receita_federal.engine import processar_simples
from dados_publicos_cnpj_receita_federal.engine import processar_socios
from dados_publicos_cnpj_receita_federal.io import download_safra
from dados_publicos_cnpj_receita_federal.io import safra_atual
from dados_publicos_cnpj_receita_federal.io import unload_safra
from dados_publicos_cnpj_receita_federal.io import unzip_safra


def main():
    safra = safra_atual()
    # ou safra = '2024-11'

    download_safra(safra=safra)
    unzip_safra(safra=safra)

    processar_empresas(safra=safra)
    processar_estabelecimentos(safra=safra)
    processar_regime_tributario(safra=safra)
    processar_simples(safra=safra)
    processar_socios(safra=safra)

    unload_safra(safra=safra)


if __name__ == '__main__':
    main()
