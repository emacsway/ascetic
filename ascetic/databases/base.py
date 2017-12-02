from __future__ import absolute_import
import logging, time, weakref
from functools import wraps
from sqlbuilder import smartsql
from ascetic import interfaces, observable, settings, utils

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

PLACEHOLDER = '%s'

utils.resolve(settings.LOGGER_INIT)(settings)


class Database(interfaces.IDatabase):

    _engines = {}
    placeholder = '%s'
    compile = smartsql.compile
    connection = None

    def __init__(self, alias, engine, initial_sql, always_reconnect=False, debug=False, **kwargs):
        self.alias = alias
        self.engine = engine
        self.debug = debug
        self.initial_sql = initial_sql
        self.always_reconnect = always_reconnect
        self._conf = kwargs
        self._logger = logging.getLogger('.'.join((__name__, self.alias)))
        if self.debug:
            self._execute = self.log_sql(self._execute)
        observable.observe(self)

    def connection_factory(self, **kwargs):
        raise NotImplementedError

    def _ensure_connected(self):
        self.connection = self.connection_factory(**self._conf)
        self.observed().notify('connect')
        if self.initial_sql:
            self.connection.cursor().execute(self.initial_sql)
        return self

    def log_sql(self, f):
        alias = self.alias
        logger = self._logger

        @wraps(f)
        def wrapper(sql, params=()):
            start = time.time()
            try:
                return f(sql, params)
            except Exception as e:
                logger.exception(e)
                raise
            finally:
                stop = time.time()
                duration = stop - start
                logger.debug(
                    '%s - (%.4f) %s; args=%s' % (alias, duration, sql, params),
                    extra={'alias': alias, 'duration': duration, 'sql': sql, 'params': params}
                )
                # import traceback; traceback.print_stack()
        return wrapper

    def _execute(self, sql, params=()):
        cursor = self.cursor()
        try:
            cursor.execute(sql, params)
        except Exception:
            if self.always_reconnect or self.transaction.can_reconnect():
                self._ensure_connected()
                cursor = self.cursor()
                cursor.execute(sql, params)
            else:
                raise
        return cursor

    def execute(self, sql, params=()):
        if not isinstance(sql, string_types):
            sql, params = self.compile(sql)
        sql = sql.rstrip("; \t\n\r")
        if self.placeholder != PLACEHOLDER:
            sql = self._sql_replace(sql, PLACEHOLDER, self.placeholder)
        cursor = self._execute(sql, params)
        return cursor

    @staticmethod
    def _sql_replace(self, statement, old, new):
        tokens = statement.split("'")
        for i in range(0, len(tokens), 2):
            tokens[i] = tokens[i].replace(old, new)
        return "'".join(tokens)

    def cursor(self):
        if not self.connection:
            self._ensure_connected()
        return self.connection.cursor()

    def last_insert_id(self, cursor):
        return cursor.lastrowid

    def begin(self):
        self.execute("BEGIN")
        self.observed().notify('begin')

    def commit(self):
        self.connection.commit()
        self.observed().notify('commit')

    def rollback(self):
        self.connection.rollback()
        self.observed().notify('rollback')

    def begin_savepoint(self, name):
        self.execute("SAVEPOINT %s", name)
        self.observed().notify('begin_savepoint', name)

    def commit_savepoint(self, name):
        self.execute("RELEASE SAVEPOINT %s", name)
        self.observed().notify('commit_savepoint', name)

    def rollback_savepoint(self, name):
        self.execute("ROLLBACK TO SAVEPOINT %s", name)
        self.observed().notify('rollback_savepoint', name)

    def set_autocommit(self, autocommit):
        pass

    def read_pk(self, db_table):
        return tuple()

    def read_fields(self, db_table):
        schema = self.describe_table(db_table)
        q = self.execute('SELECT * FROM {0} LIMIT 1'.format(self.qn(db_table)))
        # See cursor.description http://www.python.org/dev/peps/pep-0249/
        result = []
        for row in q.description:
            column = row[0]
            field_description = schema.get(column) or {}
            field_description.update({'column': column, 'type_code': row[1]})
            result.append(field_description)
        return result

    def describe_table(self, db_table):
        return {}

    def qn(self, name):
        return self.compile(smartsql.Name(name))[0]

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    @classmethod
    def register(cls, engine):
        def _deco(engine_cls):
            cls._engines[engine] = engine_cls
            return engine_cls
        return _deco

    @classmethod
    def factory(cls, **kwargs):

        try:
            database_factory = cls._engines[kwargs['engine']]
        except KeyError:
            database_factory = utils.resolve(kwargs['engine'])

        database = database_factory(**kwargs)

        if 'django_alias' in kwargs:
            database.connection_factory = django_connection_factory

        from ascetic.identity_maps import IdentityMap
        database.identity_map = IdentityMap(weakref.ref(database))

        from ascetic.transaction import TransactionManager
        database.transaction = TransactionManager(weakref.ref(database), kwargs.pop('autocommit', False))
        return database


def django_connection_factory(django_alias, **kwargs):
    # self.django_conn.ensure_connection()
    from django.db import connections
    django_connection = connections[django_alias]
    if not django_connection.connection:
        django_connection.cursor()
    return django_connection.connection
