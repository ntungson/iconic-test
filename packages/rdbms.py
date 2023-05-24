import logging
from collections import namedtuple
from pathlib import Path

import psycopg2
import pydantic

logger = logging.getLogger(__name__)

QueryResult = namedtuple("QueryResult", ["rows", "columns"])


class DBConfig(pydantic.BaseSettings):
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    dbname: str = "public"


class DBConnection:
    """
    Class to create a connection to a database
    """

    def __init__(self, config: DBConfig):
        self._config = config
        self._conn = self._get_connection()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._conn.close()

    def _get_connection(self):
        """Establish a connection to the database"""
        try:
            return psycopg2.connect(**self._config.dict())
        except psycopg2.OperationalError as ex:
            logger.error("Failed to connect to %s: %s", self._config.host, ex)
            raise

    def commit(self):
        """Commit changes"""
        self._conn.commit()

    def execute(self, query: str):
        """Execute a SQL query"""
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
        except (psycopg2.ProgrammingError, psycopg2.OperationalError, psycopg2.DatabaseError):
            logger.error(f"Query failed:\n{query}")
            raise
        else:
            logger.debug(f"Query succeeded:\n{query}")
        return cursor

    def execute_psql_copy(self, copy_query, file):
        """Execute a copy query on a Postgres database"""
        cursor = self._conn.cursor()
        try:
            cursor.copy_expert(sql=copy_query, file=file)
        except (
            psycopg2.ProgrammingError,
            psycopg2.OperationalError,
            psycopg2.DatabaseError,
        ):
            logger.error(f"Copy query failed: \n{copy_query}")
            raise
        else:
            logger.debug(f"Copy query succeeded: \n{copy_query}")
        return cursor

    def upsert_csv_psql_table(self, csv_file_path: Path, table: str, upsert_query: str, encoding: str = "utf-8"):
        """Upsert data to from a csv file to a psql table"""
        create_tmp_table_query = f"""
CREATE TEMP TABLE tmp_table
(LIKE {table} INCLUDING DEFAULTS);
"""

        copy_query = """
COPY tmp_table ({columns}) FROM STDIN
WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');
"""

        try:
            with csv_file_path.open(encoding=encoding) as f:
                # Create a temp table
                self.execute(create_tmp_table_query)

                # Copy the data from the csv file to the temp table
                copy_query = copy_query.format(columns=f.readline().strip())
                f.seek(0)
                self.execute_psql_copy(copy_query, f)

                # Update data using upsert
                cursor = self.execute(upsert_query)
                logger.info(f"Upsert table {table}, affected rows: {cursor.rowcount}")
        except FileNotFoundError:
            logger.error(f"Could not find this file to load: {csv_file_path}")
            raise
