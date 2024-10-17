![Python](https://img.shields.io/badge/python-3.9-blue.svg)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)
![DuckDB](https://img.shields.io/badge/-DuckDB-FFF000?style=flat&logo=duckdb&logoColor=white)

[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)

# Dados Publicos CNPJ Receita Federal

## Descrição

Este projeto tem como objetivo extrair informações do Cadastro Nacional da Pessoa Jurídica (CNPJ) disponibilizadas pela Receita Federal do Brasil nesse [link](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj). Através da utilização de técnicas de engenharia de dados, o projeto busca processar grandes volumes de dados de maneira eficiente e escalável.

O Layout das bases de dados foi obtido através desse [link](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

## Resumo

O Cadastro Nacional da Pessoa Jurídica (CNPJ) é um banco de dados gerenciado pela Secretaria Especial da Receita Federal do Brasil (RFB), que armazena informações cadastrais das pessoas jurídicas e outras entidades de interesse das administrações tributárias da União, dos Estados, do Distrito Federal e dos Municípios.

A periodicidade de atualização dos dados é mensal.

## Como Executar o Projeto

```bash
pip install git+https://github.com/adntayer/dados-publicos-cnpj-receita-federal.git@lastest
```

ou

```bash
pip install git+https://github.com/adntayer/dados-publicos-cnpj-receita-federal.git@<commit_hash>
```

### Executar toda a pipeline

```python
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
```

### Para visualizar o banco de dados

```python
from dados_publicos_cnpj_receita_federal.database import connect_db

with connect_db() as db:
    db.query("show tables")

# ┌───────────────────────────────────┐
# │               name                │
# │              varchar              │
# ├───────────────────────────────────┤
# │ cnaes                             │
# │ empresas                          │
# │ estabelecimentos                  │
# │ motivos                           │
# │ municipios                        │
# │ naturezas_juridicas               │
# │ pais                              │
# │ qualificacoes_socios              │
# │ regime_tributario                 │
# │ regime_tributario_lucro_presumido │
# │ simples                           │
# │ socios                            │
# ├───────────────────────────────────┤
# │              12 rows              │
# └───────────────────────────────────┘
```

### Para limpar

```python
from dados_publicos_cnpj_receita_federal.io import clean

clean()
```


## Tecnologias Utilizadas

- Python
- [DuckDB](https://duckdb.org/)

## Metodologia

A metodologia adotada para a extração e processamento dos dados envolve as seguintes etapas:

### 1. Download

Foi implementado um sistema para realizar o download dos arquivos disponíveis no site da Receita Federal em série para que todos os downloads sejam baixados corretamente.

### 2. Descompactação (unzip)

Para fazer o unzip, utilizou-se um processamento em série para descompactar os arquivos baixados, garantindo que o processo de extração seja ágil e eficiente.

### 3. Carregamento de Dados em DuckDB

Uma vez descompactados, os arquivos `.csv` são carregados em um banco de dados local utilizando [DuckDB](https://duckdb.org/).

### 4. Transformação de Dados

Alem de carregar os dados em um banco local, algumas transoformações em algumas colunas específicas, como a natureza jurídica, convertendo códigos em descrições mais legíveis, foi realizada de modo a para facilitar a análise futura (_exemplo: 2046 - > 'Sociedade Anônima Aberta'_).

### 5. Exportação dos Dados (unload)

Apos os dados carregados e transformados, o 'unload' da base pode ser realizado de modo a fornecer arquivos para consultas e análises subsequentes com a possbilidade de carregamento em bancos de dados de produçao (ex: AWS Redshift) ou até mesmo ingestão dos arquivos em um Data Lake.

# Problemas & Features

Se você encontrar algum bug ou tiver uma nova sugestão de feature, sinta-se à vontade para abrir uma nova issue no repositório. Isso ajudará a manter o projeto organizado e permitirá que outras pessoas contribuam para as melhorias.

Veja o arquivo [CONTRIBUTION](CONTRIBUTION.md)

# Licença

Este projeto está licenciado sob a [Licença MIT](LICENSE). Ao contribuir para este projeto, você concorda com os termos dessa licença.
