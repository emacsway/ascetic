from sqlbuilder.smartsql.dialects import sqlite
from ascetic.databases.base import Database


@Database.register('sqlite3')
class SqliteDatabase(Database):

    placeholder = '?'
    compile = sqlite.compile

    def connection_factory(self, **kwargs):
        import sqlite3
        return sqlite3.connect(**kwargs)
