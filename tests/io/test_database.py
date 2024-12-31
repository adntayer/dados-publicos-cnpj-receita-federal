import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from dados_publicos_cnpj_receita_federal import SetupLogger
from dados_publicos_cnpj_receita_federal.database import connect_db


@patch('duckdb.connect')
@patch.object(SetupLogger, 'info')
@patch.object(SetupLogger, 'error')
@patch('dados_publicos_cnpj_receita_federal.settings.DB_URI', None)  # Mock DB_URI to be None
def test_connect_db_success(mock_error, mock_info, mock_connect):
    mock_db = MagicMock()
    mock_connect.return_value = mock_db

    with connect_db() as db:
        mock_connect.assert_called_once_with(None)  # Now this should match
        assert db == mock_db
        mock_info.assert_any_call('connect_db | conectando ao DuckDB')

    mock_info.assert_any_call('connect_db | fechando a conexão com o DuckDB')
    mock_info.assert_any_call('connect_db | conexão fechada')
    mock_db.close.assert_called_once()


@patch('duckdb.connect')
@patch.object(SetupLogger, 'info')
def test_connect_db_default_uri(mock_info, mock_connect):
    mock_db = MagicMock()
    mock_connect.return_value = mock_db
    expected_db_uri = 'default_uri_from_settings'

    with patch('dados_publicos_cnpj_receita_federal.settings.DB_URI', expected_db_uri):
        with connect_db():
            mock_connect.assert_called_once_with(expected_db_uri)

    mock_info.assert_any_call('connect_db | fechando a conexão com o DuckDB')
    mock_info.assert_any_call('connect_db | conexão fechada')


@patch('duckdb.connect')
@patch.object(SetupLogger, 'info')
@patch.object(SetupLogger, 'error')
def test_connect_db_exception_handling(mock_error, mock_info, mock_connect):
    mock_connect.side_effect = Exception('Connection failed')

    with patch('dados_publicos_cnpj_receita_federal.settings.DB_URI', 'dummy_uri'):
        with pytest.raises(Exception):
            with connect_db():
                pass

    mock_error.assert_called_with('connect_db | erro durante a conexão ou execução no banco de dados: Connection failed')


@patch('duckdb.connect')
@patch.object(SetupLogger, 'info')
@patch.object(SetupLogger, 'error')
def test_connect_db_with_uri(mock_error, mock_info, mock_connect):
    mock_db = MagicMock()
    mock_connect.return_value = mock_db
    test_db_uri = 'my_test_db_uri'

    with connect_db(test_db_uri) as db:
        mock_connect.assert_called_once_with(test_db_uri)
        assert db == mock_db
        mock_info.assert_any_call('connect_db | conectando ao DuckDB')

    mock_info.assert_any_call('connect_db | fechando a conexão com o DuckDB')
    mock_info.assert_any_call('connect_db | conexão fechada')


if __name__ == '__main__':
    unittest.main()
