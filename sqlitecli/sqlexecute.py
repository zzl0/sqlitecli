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

    databases_query = '''SHOW DATABASES'''

    tables_query = '''
    SELECT name
    FROM sqlite_master
    WHERE type IN ('table','view')
    AND name NOT LIKE 'sqlite_%'
    ORDER BY 1
    '''

    version_query = '''SELECT @@VERSION'''

    version_comment_query = '''SELECT @@VERSION_COMMENT'''
    version_comment_query_mysql4 = '''SHOW VARIABLES LIKE "version_comment"'''

    show_candidates_query = '''SELECT name from mysql.help_topic WHERE name like "SHOW %"'''

    users_query = '''SELECT CONCAT("'", user, "'@'",host,"'") FROM mysql.user'''

    functions_query = '''SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE="FUNCTION" AND ROUTINE_SCHEMA = "%s"'''

    table_columns_query = '''select TABLE_NAME, COLUMN_NAME from information_schema.columns
                                    where table_schema = '%s'
                                    order by table_name,ordinal_position'''

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
        """Yields column names"""
        for c in ['aaa', 'bbb']:
            yield ('tbl1', c)

    def databases(self):
        for n in ['main']:
            yield n

    def functions(self):
        """Yields tuples of (schema_name, function_name)"""
        return []

    def show_candidates(self):
        return []

    def users(self):
        return []

    def server_type(self):
        return 'sqlite'
