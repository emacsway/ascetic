from __future__ import absolute_import, unicode_literals
from autumn.db import escape
from autumn.db.connection import connections

try:
    str = unicode  # Python 2.* compatible
    str_types = ()
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class Query(object):
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
        
    We can also chain the ``filter`` method::
    
        q.filter(name='John').filter(age=30)
        
    In both cases the ``WHERE`` clause will become::
    
        WHERE `name` = 'John' AND `age` = 30
    
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
        
    Query(model=MyModel).raw(sql, params) uses ``raw`` SQL.
            
    Class Methods
    -------------
    
    ``Query.raw_sql(sql, params)`` returns a database cursor. Usage::
    
        query = 'SELECT * FROM `users` WHERE id = ?'
        params = (1,) # params must be a tuple or list
        
        # Now we have the database cursor to use as we wish
        cursor = Query.raw_swl(query, params)
    
    '''
    
    def __init__(self, query_type='SELECT *', conditions={}, model=None, using=None):
        from autumn.model import Model
        self._type = query_type
        self._conditions = conditions
        self._order = ''
        self._limit = ()
        self._cache = None
        self._sql = None
        self._params = []
        if model and not issubclass(model, Model):
            raise Exception('Query objects must be created with a model class.')
        self._model = model
        if using:
            self.using = using
        elif model:
            self.using = model.using
        self.placeholder = connections[self.using].conn.placeholder

    def __getitem__(self, k):
        if self._cache != None:
            return self._cache[k]
        
        if isinstance(k, integer_types):
            self._limit = (k,1)
            lst = self.get_data()
            if not lst:
                return None
            return lst[0]
        elif isinstance(k, slice):
            if k.start is not None:
                assert k.stop is not None, "Limit must be set when an offset is present"
                assert k.stop >= k.start, "Limit must be greater than or equal to offset"
                self._limit = k.start, (k.stop - k.start)
            elif k.stop is not None:
                self._limit = 0, k.stop
        
        return self.get_data()
        
    def __len__(self):
        return self.count()
        return len(self.get_data())
        
    def __iter__(self):
        return iter(self.get_data())
        
    def __repr__(self):
        return repr(self.get_data())

    def raw(self, sql, params=None):
        self._sql = sql,
        self._params = params or []
        return self

    def count(self):
        return Query.raw_sql(
            "SELECT COUNT(1) as c FROM ({0}) as t".format(
                self.query_template(limit=False)
            ),
            self.extract_params(), self.using
        ).fetchone()[0]
        
    def filter(self, **kwargs):
        self._conditions.update(kwargs)
        return self
        
    def order_by(self, field, direction='ASC'):
        self._order = 'ORDER BY {0} {1}'.format(escape(field), direction)
        return self
        
    def extract_condition_keys(self):
        if len(self._conditions):
            return 'WHERE {0}'.format(
                ' AND '.join(
                    "{0}={1}".format(escape(k), self.placeholder)
                    for k in self._conditions
                )
            )
        
    def extract_params(self):
        if self._sql:
            return self._params
        return list(self._conditions.values())
        
    def query_template(self, limit=True):
        if self._sql:
            return '{0} {1}'.format(
                self._sql,
                limit and self.extract_limit() or ''
            )
        return '{0} FROM {1} {2} {3} {4}'.format(
            self._type,
            self._model.Meta.table_safe,
            self.extract_condition_keys() or '',
            self._order,
            limit and self.extract_limit() or '',
        )
        
    def extract_limit(self):
        if len(self._limit):
            return 'LIMIT {0}'.format(', '.join(str(l) for l in self._limit))
        
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
        return Query.raw_sql(self.query_template(), self.extract_params(), self.using)
        
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
            print (sql, params)
        cursor = cls.get_cursor(using)
        try:
            cursor.execute(sql, params)
            if db.conn.b_commit:
                db.conn.connection.commit()
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
            if db.conn.b_commit:
                db.conn.connection.commit()
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
        cls.get_db(using).conn.b_commit = False

    @classmethod
    def commit(cls, using=None):
        """
        begin() and commit() let you explicitly specify an SQL transaction.
        Be sure to call commit() after you call begin().
        """
        cursor = None
        try:
            cls.get_db(using).conn.connection.commit()
        finally:
            cls.get_db(using).conn.b_commit = True
        return cursor
