from setuptools import find_packages
from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('dev.txt') as f:
    requirements_dev = f.read().splitlines()

setup(
    name='dados_publicos_cnpj_receita_federal',
    version='0.1.0',
    packages=find_packages(),
    author='adntayer',
    description='Este projeto tem como objetivo extrair informações do Cadastro Nacional da Pessoa Jurídica (CNPJ) disponibilizadas pela Receita Federal do Brasil',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/adntayer/dados-publicos-cnpj-receita-federal',
    install_requires=requirements,
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    extras_require={
        'dev': requirements_dev,
    },
)
