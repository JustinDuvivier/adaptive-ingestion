import pytest
from unittest.mock import MagicMock, patch, call
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from src.database.connection import DatabaseConnection


@pytest.fixture
def mock_pool():
    mock = MagicMock(spec=ThreadedConnectionPool)
    mock.minconn = 1
    mock.maxconn = 10
    return mock


@pytest.fixture
@patch('src.database.connection.ThreadedConnectionPool')
def db_connection(mock_pool_class):
    mock_pool_instance = MagicMock()
    mock_pool_class.return_value = mock_pool_instance

    db = DatabaseConnection(
        host='localhost',
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

    return db


def test_init_creates_pool():
    with patch('src.database.connection.ThreadedConnectionPool') as mock_pool:
        db = DatabaseConnection(
            host='localhost',
            dbname='test_db',
            user='test_user',
            password='test_pass'
        )

        mock_pool.assert_called_once_with(
            1, 10,
            host='localhost',
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        assert db.pool == mock_pool.return_value


def test_init_with_kwargs():
    with patch('src.database.connection.ThreadedConnectionPool') as mock_pool:
        db = DatabaseConnection(
            host='localhost',
            dbname='test_db',
            user='test_user',
            password='test_pass',
            port=5433,
            connect_timeout=30
        )

        mock_pool.assert_called_once_with(
            1, 10,
            host='localhost',
            database='test_db',
            user='test_user',
            password='test_pass',
            port=5433,
            connect_timeout=30
        )


def test_get_connection(db_connection):
    mock_conn = MagicMock()
    db_connection.pool.getconn.return_value = mock_conn

    conn = db_connection.get_connection()

    db_connection.pool.getconn.assert_called_once()
    assert conn == mock_conn


def test_return_connection(db_connection):
    mock_conn = MagicMock()

    db_connection.return_connection(mock_conn)

    db_connection.pool.putconn.assert_called_once_with(mock_conn)


def test_execute_query_success(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [('result1',), ('result2',)]

    db_connection.pool.getconn.return_value = mock_conn

    result = db_connection.execute_query(
        "SELECT * FROM users WHERE id = %s",
        (1,)
    )

    db_connection.pool.getconn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM users WHERE id = %s",
        (1,)
    )
    mock_cursor.fetchall.assert_called_once()
    mock_cursor.close.assert_called_once()
    db_connection.pool.putconn.assert_called_once_with(mock_conn)

    assert result == [('result1',), ('result2',)]


def test_execute_query_pool_error(db_connection):
    db_connection.pool.getconn.side_effect = psycopg2.pool.PoolError("Pool exhausted")

    with pytest.raises(Exception) as exc_info:
        db_connection.execute_query("SELECT 1")

    assert "pool exhausted" in str(exc_info.value).lower()


def test_execute_query_operational_error(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg2.OperationalError("Connection lost")

    db_connection.pool.getconn.return_value = mock_conn

    with pytest.raises(Exception) as exc_info:
        db_connection.execute_query("SELECT 1")

    assert "connection failed" in str(exc_info.value).lower()
    db_connection.pool.putconn.assert_called_once_with(mock_conn)


def test_execute_update_success(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 5

    db_connection.pool.getconn.return_value = mock_conn

    result = db_connection.execute_update(
        "UPDATE users SET active = true WHERE id = %s",
        (1,)
    )

    mock_cursor.execute.assert_called_once_with(
        "UPDATE users SET active = true WHERE id = %s",
        (1,)
    )
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    assert result == 5


def test_execute_update_integrity_error(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg2.IntegrityError("Duplicate key")

    db_connection.pool.getconn.return_value = mock_conn

    with pytest.raises(Exception) as exc_info:
        db_connection.execute_update("INSERT INTO users VALUES (%s)", (1,))

    assert "constraint violation" in str(exc_info.value).lower()
    mock_conn.rollback.assert_called_once()


def test_execute_many_success(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 3

    db_connection.pool.getconn.return_value = mock_conn

    params_list = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
    result = db_connection.execute_many(
        "INSERT INTO users (id, name) VALUES (%s, %s)",
        params_list
    )

    mock_cursor.executemany.assert_called_once_with(
        "INSERT INTO users (id, name) VALUES (%s, %s)",
        params_list
    )
    mock_conn.commit.assert_called_once()
    assert result == 3


def test_execute_many_empty_list(db_connection):
    with pytest.raises(Exception) as exc_info:
        db_connection.execute_many("INSERT INTO users VALUES (%s)", [])

    assert "params_list cannot be empty" in str(exc_info.value)
    db_connection.pool.getconn.assert_not_called()


def test_execute_many_data_error(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.executemany.side_effect = psycopg2.DataError("Invalid data type")

    db_connection.pool.getconn.return_value = mock_conn

    with pytest.raises(Exception) as exc_info:
        db_connection.execute_many(
            "INSERT INTO users VALUES (%s)",
            [('invalid',)]
        )

    assert "invalid data" in str(exc_info.value).lower()
    mock_conn.rollback.assert_called()


def test_close(db_connection):
    db_connection.close()

    db_connection.pool.closeall.assert_called_once()


def test_connection_returned_on_exception(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("Unexpected error")

    db_connection.pool.getconn.return_value = mock_conn

    with pytest.raises(Exception):
        db_connection.execute_query("SELECT 1")

    db_connection.pool.putconn.assert_called_once_with(mock_conn)


def test_get_connection_and_cursor_helper(db_connection):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    db_connection.pool.getconn.return_value = mock_conn

    conn, cursor = db_connection._get_connection_and_cursor()

    assert conn == mock_conn
    assert cursor == mock_cursor
    db_connection.pool.getconn.assert_called_once()
    mock_conn.cursor.assert_called_once()


def test_get_connection_and_cursor_pool_error(db_connection):
    db_connection.pool.getconn.side_effect = psycopg2.pool.PoolError("Pool exhausted")

    with pytest.raises(Exception) as exc_info:
        db_connection._get_connection_and_cursor()

    assert "pool exhausted" in str(exc_info.value).lower()