import psycopg2
from psycopg2.pool import ThreadedConnectionPool


class DatabaseConnection:
    def __init__(self,host,dbname,user,password,**kwargs):
        self.pool = ThreadedConnectionPool(
            1,
            10,
            host = host,
            database = dbname,
            user = user,
            password = password,
            **kwargs
        )

    def get_connection(self):
        return self.pool.getconn()

    def return_connection(self, conn):
        self.pool.putconn(conn)

    def _get_connection_and_cursor(self):
        try:
            connection = self.pool.getconn()
            cursor = connection.cursor()
            return connection,cursor
        except psycopg2.pool.PoolError:
            raise Exception('Connection pool exhausted')
        except psycopg2.OperationalError:
            raise Exception('Database initial connection failed, try again')


    def execute_query(self,query,params = None):
        connection = None
        try:
            connection,cursor = self._get_connection_and_cursor()
            cursor.execute(query,params)
            result = cursor.fetchall()
            cursor.close()
            return result
        except psycopg2.pool.PoolError:
            raise Exception('Connection pool exhausted')
        except psycopg2.OperationalError:
            raise Exception('Database connection failed during query, try again')
        except psycopg2.ProgrammingError as e:
            raise Exception(f'Query error: {str(e)}')
        except psycopg2.IntegrityError as e:
            raise Exception(f'Data Constraint violation: {str(e)}')
        except Exception as e:
            raise Exception(f'Unexpected database error: {str(e)}')
        finally:
            if connection:
                self.pool.putconn(connection)

    def execute_update(self,query,params = None):
        connection = None
        try:
            connection,cursor = self._get_connection_and_cursor()
            cursor.execute(query,params)
            connection.commit()
            affected_rows = cursor.rowcount
            cursor.close()
            return affected_rows
        except psycopg2.pool.PoolError:
            raise Exception('Connection pool exhausted')
        except psycopg2.OperationalError:
            if connection:
                connection.rollback()
            raise Exception('Database connection failed during update, try again')
        except psycopg2.IntegrityError as e:
            if connection:
                connection.rollback()
            raise Exception(f'Data constraint violation: {str(e)}')
        except psycopg2.DataError as e:
            if connection:
                connection.rollback()
            raise Exception(f'Invalid data: {str(e)}')
        except psycopg2.ProgrammingError as e:
            if connection:
                connection.rollback()
            raise Exception(f'Query error: {str(e)}')
        finally:
            if connection:
                self.pool.putconn(connection)

    def execute_many(self,query,params_list):
        connection = None
        try:
            if not params_list:
                raise ValueError("params_list cannot be empty")
            connection,cursor = self._get_connection_and_cursor()
            cursor.executemany(query,params_list)
            connection.commit()
            affected = cursor.rowcount
            cursor.close()
            return affected
        except psycopg2.pool.PoolError:
            raise Exception("Connection pool exhausted")
        except psycopg2.OperationalError as e:
            if connection:
                connection.rollback()
            raise Exception(f'Database connection failed: {str(e)}')
        except psycopg2.IntegrityError as e:
            if connection:
                connection.rollback()
            raise Exception(f'Batch operation has failed - constraint violation: {str(e)}')
        except psycopg2.DataError as e:
            if connection:
                connection.rollback()
            raise Exception(f'Batch operation has failed - invalid data: {str(e)}')
        except psycopg2.ProgrammingError as e:
            if connection:
                connection.rollback()
            raise Exception(f'Query error: {str(e)}')
        except (TypeError,ValueError) as e:
            if connection:
                connection.rollback()
            raise Exception(f'Invalid parameters format: {str(e)}')
        finally:
            if connection:
                self.pool.putconn(connection)

    def close(self):
        if self.pool:
            self.pool.closeall()



