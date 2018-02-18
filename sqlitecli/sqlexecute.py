import os
import logging
import sqlite3
import pymysql
import sqlparse
from .packages import special
from pymysql.constants import FIELD_TYPE
from pymysql.converters import (convert_mysql_timestamp, convert_datetime,
                                convert_timedelta, convert_date, conversions,
                                decoders)

_logger = logging.getLogger(__name__)

FIELD_TYPES = decoders.copy()
FIELD_TYPES.update({
    FIELD_TYPE.NULL: type(None)
})

class SQLExecute(object):

    databases_query = '''
        PRAGMA database_list
    '''

    tables_query = '''
        SELECT name
        FROM sqlite_master
        WHERE type IN ('table','view')
        AND name NOT LIKE 'sqlite_%'
        ORDER BY 1
    '''

    table_columns_query = '''
        SELECT name, sql
        FROM sqlite_master
        WHERE type IN ('table','view')
        AND name NOT LIKE 'sqlite_%'
        ORDER BY 1
    '''

    def __init__(self, filename):
        if filename:
            self.filename = os.path.expanduser(os.path.abspath(filename))
        else:
            self.filename = ':memory:'
        self.dbname = 'dummy'
        self.connect()

    def connect(self):
        conn = sqlite3.connect(self.filename)
        if hasattr(self, 'conn'):
            self.conn.close()
        self.conn = conn

    def run(self, statement):
        """Execute the sql in the database and return the results. The results
        are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        """

        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield (None, None, None, None)

        # Split the sql into separate queries and run each one.
        # Unless it's saving a favorite query, in which case we
        # want to save them all together.
        if statement.startswith('\\fs'):
            components = [statement]
        else:
            components = sqlparse.split(statement)

        for sql in components:
            # Remove spaces, eol and semi-colons.
            sql = sql.rstrip(';')

            cur = self.conn.cursor()
            try:   # Special command
                _logger.debug('Trying a dbspecial command. sql: %r', sql)
                for result in special.execute(cur, sql):
                    yield result
            except special.CommandNotFound:  # Regular SQL
                _logger.debug('Regular sql statement. sql: %r', sql)
                cur.execute(sql)
                yield self.get_result(cur)

    def get_result(self, cursor):
        """Get the current result's data from the cursor."""
        title = headers = None

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT or SHOW.
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            status = '{0} row{1} in set'
        else:
            _logger.debug('No rows in result.')
            status = 'Query OK, {0} row{1} affected'
        status = status.format(cursor.rowcount,
                               '' if cursor.rowcount == 1 else 's')

        return (title, cursor if cursor.description else None, headers, status)

    def tables(self):
        """Yields table names"""
        for row in self.conn.execute(self.tables_query):
            yield row

    def table_columns(self):
        """Yields (table column) pairs"""
        for table, sql in self.conn.execute(self.table_columns_query):
            for col in self._get_cols(sql):
                yield (table, col)

    def _get_cols(self, sql):
        index = sql.index('(')
        return [col.split()[0] for col in sql[index + 1: len(sql) - 1].split(', ')]

    def databases(self):
        for row in self.conn.execute(self.databases_query):
            yield row[1]
