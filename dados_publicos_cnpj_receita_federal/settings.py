import os

FOLDER_ROOT = os.getcwd()
PATH_FOLDER_RAW = os.path.join(FOLDER_ROOT, '.data_dados_publicos_cnpj_receita_federal')
DB_URI = os.path.join(FOLDER_ROOT, '.data_dados_publicos_cnpj_receita_federal', 'db.duckdb')

os.makedirs(PATH_FOLDER_RAW, exist_ok=True)

FOLDER_ZIP = 'zip'
FOLDER_UNZIP = 'unzip'
FOLDER_UNLOAD = 'unload'

TABLE_NAME_EMPRESAS = 'empresas'
TABLE_NAME_ESTABELECIMENTOS = 'estabelecimentos'
TABLE_NAME_SIMPLES = 'simples'
TABLE_NAME_SOCIOS = 'socios'
TABLE_NAME_REGIME_TRIBUTARIO = 'regime_tributario'
