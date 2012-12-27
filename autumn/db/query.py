from __future__ import absolute_import, unicode_literals
import copy
from autumn.db.connection import connections
from autumn.db.query_base import Query as QueryBase, OPERATORS, PLACEHOLDER, LOOKUP_SEP

try:
    str = unicode  # Python 2.* compatible
    str_types = ()
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

DIALECTS = {
    'sqlite3': 'sqlite',
    'mysql': 'mysql',
    'postgresql': 'postgres',
    'postgresql_psycopg2': 'postgres',
    'postgis': 'postgres',
    'oracle': 'oracle',
}


class Query(QueryBase):
    '''
    Gives quick access to database by setting attributes (query conditions, et
    cetera), or by the sql methods.

    Instance Methods
    ----------------

    Creating a Query object requires a Model class at the bare minimum. The
    doesn't run until results are pulled using a slice, ``list()`` or iterated
    over.

    For example::

        q = Query(model=MyModel)

    This sets up a basic query without conditions. We can set conditions using
    the ``filter`` method::

        q.filter(name='John', age=30)
        q.filter('name = %s AND age=%s', 'John', 30)

    We can also chain the ``filter`` method::

        q.filter(name='John').filter(age=30)

    In both cases the ``WHERE`` clause will become::

        WHERE `name` = 'John' AND `age` = 30

    Support JOIN:

        Author.get().table_as('a').join(
            'INNER JOIN', Book.get().table_as('b').filter('a.id = b.author_id')
        ).filter(a__id__in=(3,5)).order_by('-a.id')
        or:
        Author.get().table_as('a').join(
            'INNER JOIN', Book.get().table_as('b').filter(a__id=q.f('b.author_id'))
        ).filter(a__id__in=(3,5)).order_by('-a.id')

    You can also order using ``order_by`` to sort the results::

        # The second arg is optional and will default to ``ASC``
        q.order_by('column', 'DESC')

    You can limit result sets by slicing the Query instance as if it were a
    list. Query is smart enough to translate that into the proper ``LIMIT``
    clause when the query hasn't yet been run::

        q = Query(model=MyModel).filter(name='John')[:10]   # LIMIT 0, 10
        q = Query(model=MyModel).filter(name='John')[10:20] # LIMIT 10, 10
        q = Query(model=MyModel).filter(name='John')[0]    # LIMIT 0, 1

    Simple iteration::

        for obj in Query(model=MyModel).filter(name='John'):
            # Do something here

    Counting results is easy with the ``count`` method. If used on a ``Query``
    instance that has not yet retrieve results, it will perform a ``SELECT
    COUNT(*)`` instead of a ``SELECT *``. ``count`` returns an integer::

        count = Query(model=MyModel).filter=(name='John').count()

    Query(model=MyModel).raw(sql, *params) uses ``raw`` SQL.

    Class Methods
    -------------

    ``Query.raw_sql(sql, params)`` returns a database cursor. Usage::

        query = 'SELECT * FROM `users` WHERE id = ?'
        params = (1,) # params must be a tuple or list

        # Now we have the database cursor to use as we wish
        cursor = Query.raw_sql(query, params)

    '''
    def _set_table(self, table=None, alias=None, **kwargs):
        from autumn.models import Model
        if kwargs:
            alias, table = kwargs.items()[0]
        if issubclass(table, Model):
            self._model = table
            self._table = type(self)().n(table.Meta.table)
            if not self.using:
                self.using = table.using
        elif isinstance(table, string_types):
            self._model = None
            self._table = type(self)().n(table)
        else:
            raise Exception('Table slould be instance of Model or str.')
        if alias:
            self._table = self._table.as_(alias)
        return self

    def _f_in_model(self, f):
        if not self._model:
            return True
        if f in self._model.Meta.fields:
            return True
        return False

    def count(self):
        self = super(Query, self).count()
        return type(self).raw_sql(self.render(), self.params(), self.using).fetchone()[0]

    def dialect(self):
        engine = type(self).get_db(self.using).engine
        return DIALECTS.get(engine, engine)

    def get_data(self):
        if self._cache is None:
            self._cache = list(self.iterator())
        return self._cache

    def iterator(self):
        cursor = self.execute_query()
        fields = [f[0] for f in cursor.description]
        for row in cursor.fetchall():
            data = dict(list(zip(fields, row)))
            if self._model:
                # obj = self._model(*row)
                obj = self._model(**data)
                obj._new_record = False
                yield obj
            else:
                yield data

    def execute_query(self):
        return type(self).raw_sql(self.render(), self.params(), self.using)

    @classmethod
    def get_db(cls, using=None):
        if not using:
            using = getattr(cls, 'using', 'default')
        return connections[using]

    @classmethod
    def get_cursor(cls, using=None):
        return cls.get_db(using).cursor()

    @classmethod
    def raw_sql(cls, sql, params=(), using=None):
        db = cls.get_db(using)
        if db.debug:
            print(sql, params)
        cursor = cls.get_cursor(using)
        if db.placeholder != PLACEHOLDER:
            sql = sql.replace(PLACEHOLDER, db.placeholder)
        try:
            cursor.execute(sql, params)
            if db.ctx.b_commit:
                db.conn.commit()
        except BaseException as ex:
            if db.debug:
                print("raw_sql: exception: ", ex)
                print("sql:", sql)
                print("params:", params)
            raise
        return cursor

    @classmethod
    def raw_sqlscript(cls, sql, using=None):
        db = cls.get_db(using)
        cursor = cls.get_cursor(using)
        try:
            cursor.executescript(sql)
            if db.ctx.b_commit:
                db.conn.commit()
        except BaseException as ex:
            if db.debug:
                print("raw_sqlscript: exception: ", ex)
                print("sql:", sql)
            raise
        return cursor

    # begin() and commit() for SQL transaction control
    # This has only been tested with SQLite3 with default isolation level.
    # http://www.python.org/doc/2.5/lib/sqlite3-Controlling-Transactions.html
    @classmethod
    def begin(cls, using=None):
        """
        begin() and commit() let you explicitly specify an SQL transaction.
        Be sure to call commit() after you call begin().
        """
        cls.get_db(using).ctx.b_commit = False

    @classmethod
    def commit(cls, using=None):
        """
        begin() and commit() let you explicitly specify an SQL transaction.
        Be sure to call commit() after you call begin().
        """
        cursor = None
        try:
            cls.get_db(using).conn.commit()
        finally:
            cls.get_db(using).ctx.b_commit = True
        return cursor

Q = Query
q = Query()
