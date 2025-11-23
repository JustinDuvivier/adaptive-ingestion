import pytest
from unittest.mock import MagicMock, patch
from src.database.setup import DatabaseSetup


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def setup(mock_db):
    return DatabaseSetup(mock_db)


def test_init(mock_db):
    setup = DatabaseSetup(mock_db)
    assert setup.db == mock_db


def test_table_exists_returns_true(setup, mock_db):
    mock_db.execute_query.return_value = [(True,)]

    result = setup._table_exists('some_table')

    mock_db.execute_query.assert_called_once()

    call_args = mock_db.execute_query.call_args[0]

    sql = call_args[0]
    params = call_args[1]

    assert "SELECT EXISTS" in sql
    assert "information_schema.tables" in sql
    assert "table_schema = 'public'" in sql
    assert "table_name = %s" in sql
    assert params == ('some_table',)
    assert result == True

def test_table_exists_returns_false(setup, mock_db):
    mock_db.execute_query.return_value = [(False,)]

    result = setup._table_exists('nonexistent')

    assert result == False


def test_constraint_exists(setup, mock_db):
    mock_db.execute_query.return_value = [(True,)]

    result = setup._constraint_exists('table_name', 'constraint_name')

    mock_db.execute_query.assert_called_once()
    assert result == True


def test_setup_pgvector_success(setup, mock_db):
    mock_db.execute_update.return_value = None

    setup._setup_pgvector()

    mock_db.execute_update.assert_called_once_with(
        "CREATE EXTENSION IF NOT EXISTS vector"
    )


def test_setup_pgvector_already_exists(setup, mock_db):
    mock_db.execute_update.side_effect = Exception("extension \"vector\" already exists")

    setup._setup_pgvector()

    mock_db.execute_update.assert_called_once()


def test_create_analysis_registry_when_not_exists(setup, mock_db):
    mock_db.execute_query.return_value = [(False,)]

    setup._create_analysis_registry()

    mock_db.execute_query.assert_called_once()
    assert mock_db.execute_update.called
    create_call = mock_db.execute_update.call_args[0][0]
    assert "CREATE TABLE analysis_registry" in create_call


def test_create_analysis_registry_when_exists(setup, mock_db):
    mock_db.execute_query.return_value = [(True,)]

    setup._create_analysis_registry()

    mock_db.execute_query.assert_called_once()
    mock_db.execute_update.assert_not_called()


def test_add_foreign_keys(setup, mock_db):

    mock_db.execute_query.return_value = [(False,)]

    setup._add_foreign_keys()

    assert mock_db.execute_query.call_count == 3

    assert mock_db.execute_update.call_count == 3

    calls = [call[0][0] for call in mock_db.execute_update.call_args_list]
    assert any("ALTER TABLE insights_cache" in call for call in calls)
    assert any("ADD CONSTRAINT fk_insights_analysis" in call for call in calls)


def test_create_indexes(setup, mock_db):
    mock_db.execute_update.return_value = None

    setup._create_indexes()

    assert mock_db.execute_update.call_count >= 9

    calls = [call[0][0] for call in mock_db.execute_update.call_args_list]
    assert any("idx_analysis_registry_session" in call for call in calls)
    assert any("idx_embeddings_vector" in call for call in calls)


@patch('builtins.input', return_value='no')
def test_drop_all_tables_cancelled(mock_input, setup, mock_db):

    setup._drop_all_tables()

    mock_db.execute_update.assert_not_called()


@patch('builtins.input', return_value='yes')
def test_drop_all_tables_confirmed(mock_input, setup, mock_db):
    setup._drop_all_tables()

    assert mock_db.execute_update.call_count == 5  # 5 tables

    calls = [call[0][0] for call in mock_db.execute_update.call_args_list]
    assert all("DROP TABLE IF EXISTS" in call for call in calls)


def test_verify_setup_all_tables_exist(setup, mock_db):

    mock_db.execute_query.side_effect = [
        [(True,)],
        [(5,)],
        [(True,)],
        [(10,)],
        [(True,)],
        [(0,)],
        [(True,)],
        [(0,)],
        [(True,)],
        [(0,)],
        [(True,)]
    ]

    result = setup.verify_setup()

    assert result == True


def test_verify_setup_missing_table(setup, mock_db):
    mock_db.execute_query.side_effect = [
        [(False,)],
        [(True,)],
        [(0,)],
        [(True,)],
        [(0,)],
        [(True,)],
        [(0,)],
        [(True,)],
        [(0,)],
        [(True,)]
    ]

    result = setup.verify_setup()

    assert result == False


def test_setup_all_calls_methods_in_order(setup, mock_db):
    setup._drop_all_tables = MagicMock()
    setup._setup_pgvector = MagicMock()
    setup._create_analysis_registry = MagicMock()
    setup._create_stg_rejects = MagicMock()
    setup._create_insights_cache = MagicMock()
    setup._create_query_history = MagicMock()
    setup._create_embeddings = MagicMock()
    setup._add_foreign_keys = MagicMock()
    setup._create_indexes = MagicMock()

    setup.setup_all(drop_existing=False)

    setup._setup_pgvector.assert_called_once()
    setup._create_analysis_registry.assert_called_once()
    setup._create_stg_rejects.assert_called_once()
    setup._create_insights_cache.assert_called_once()
    setup._create_query_history.assert_called_once()
    setup._create_embeddings.assert_called_once()
    setup._add_foreign_keys.assert_called_once()
    setup._create_indexes.assert_called_once()

    setup._drop_all_tables.assert_not_called()


def test_setup_all_with_drop_existing(setup, mock_db):
    setup._drop_all_tables = MagicMock()
    setup._setup_pgvector = MagicMock()
    setup._create_analysis_registry = MagicMock()
    setup._create_stg_rejects = MagicMock()
    setup._create_insights_cache = MagicMock()
    setup._create_query_history = MagicMock()
    setup._create_embeddings = MagicMock()
    setup._add_foreign_keys = MagicMock()
    setup._create_indexes = MagicMock()

    setup.setup_all(drop_existing=True)

    setup._drop_all_tables.assert_called_once()