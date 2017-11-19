import collections
from sqlbuilder.smartsql.dialects import mysql
from ascetic.databases.base import Database


@Database.register('mysql')
class MySQLDatabase(Database):

    compile = mysql.compile

    def connection_factory(self, **kwargs):
        import MySQLdb
        return MySQLdb.connect(**kwargs)

    def read_pk(self, db_table):
        cursor = self.execute("""
            SELECT COLUMN_NAME
            FROM   information_schema.KEY_COLUMN_USAGE
            WHERE  TABLE_SCHEMA = SCHEMA()
            AND    CONSTRAINT_NAME = 'PRIMARY'
            AND    TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION;
        """, [db_table])
        return tuple(i[0] for i in cursor.fetchall())

    def describe_table(self, db_table):
        cursor = self.execute("""
            SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
            FROM   information_schema.COLUMNS
            WHERE  TABLE_SCHEMA = SCHEMA()
            AND    TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION;
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
        self.execute("SET autocommit={}".format(int(autocommit)))
        # self.connection.autocommit(autocommit)
        super(MySQLDatabase, self).set_autocommit(autocommit)
