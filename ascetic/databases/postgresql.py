import collections
from ascetic.databases.base import Database
from ascetic.utils import cached_property


@Database.register('postgresql')
class PostgreSQLDatabase(Database):

    @cached_property
    def psycopg2(self):
        import psycopg2
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
        return psycopg2

    def connection_factory(self, **kwargs):
        return self.psycopg2.connect(**kwargs)

    def last_insert_id(self, cursor):
        cursor.execute("SELECT lastval()")
        return cursor.fetchone()[0]

    def read_pk(self, db_table):
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        cursor = self.execute("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
        FROM   pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
               AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %s::regclass
               AND i.indisprimary
        ORDER BY a.attnum;
        """, [db_table])
        return tuple(i[0] for i in cursor.fetchall())

    def describe_table(self, db_table):
        cursor = self.execute("""
            SELECT column_name, ordinal_position, data_type, is_nullable, column_default, character_maximum_length
            FROM   information_schema.columns
            WHERE  table_name = %s
            ORDER BY ordinal_position;
        """, [db_table])
        schema = collections.OrderedDict()
        for row in cursor.fetchall():
            col = {
                'column': row[0],
                'position': row[1],
                'data_type': row[2],
                'null': row[3].upper() == 'YES',
                # 'default': row[4],
                'max_length': row[5],
            }
            schema[col['column']] = col
        return schema

    def set_autocommit(self, autocommit):
        # The server-side autocommit setting was removed and reimplemented in client applications and languages.
        # Server-side autocommit was causing too many problems with languages and applications that wanted
        # to control their own autocommit behavior, so autocommit was removed from the server and added to individual
        # client APIs as appropriate.
        # https://www.postgresql.org/docs/7.4/static/release-7-4.html
        # self.execute("SET AUTOCOMMIT = {}".format('ON' if autocommit else 'OFF'))
        self.connection.set_session(autocommit=autocommit)
        super(PostgreSQLDatabase, self).set_autocommit(autocommit)
