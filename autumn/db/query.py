from __future__ import absolute_import, unicode_literals
import re
import copy
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
        q.filter('name = %s AND age=%s', 'John', 30)
        
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
    
    def __init__(self, query_type='SELECT *', model=None, using=None):
        from autumn.models import Model
        self._type = query_type
        self._from = None
        self._conditions = []
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
        self.placeholder = connections[self.using].placeholder
        if self._model:
            self._from = self._model.Meta.table_safe

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
            else:
                return self.clone()
        
        return self
        
    def __len__(self):
        return self.count()
        #return len(self.get_data())
        
    def __iter__(self):
        return iter(self.get_data())
        
    def __repr__(self):
        return repr(self.get_data())

    def clone(self):
        return copy.deepcopy(self)

    def raw(self, sql, params=None):
        self._sql = sql
        self._params = params or []
        return self

    def count(self):
        return Query.raw_sql(
            "SELECT COUNT(1) as c FROM ({0}) as t".format(
                self.query_template()
            ),
            self.params(), self.using
        ).fetchone()[0]

    def from_(self, expr):
        self._from = expr
        return self

    def filter(self, *args, **kwargs):
        connector = kwargs.pop('_connector', 'AND')
        if args:
            self._conditions.append((connector, args[0], args[1:]))
        for k, v in kwargs.items():
            self._conditions.append((connector, k, v))
        return self

    def or_filter(self, *args, **kwargs):
        kwargs['_connector'] = 'OR'
        return self.filter(*args, **kwargs)

    def order_by(self, field, direction='ASC'):
        self._order = 'ORDER BY {0} {1}'.format(escape(field), direction)
        return self
        
    def render_conditions(self, completed=True):
        parts = []
        for connector, expr, params in self._conditions:
            if isinstance(expr, Query):  # Nested conditions
                expr = expr.render_conditions(completed=False)
            if not re.search(r'[ =!<>]', expr, re.S):
                expr = "{0} = {1}".format(escape(expr), self.placeholder)
            if parts:
                expr = '{0} ({1}) '.format(connector, expr)
            parts.append(expr)
        if parts and completed:
            parts.insert(0, 'WHERE')
        return ' '.join(parts)
        
    def params(self):
        if self._sql:
            return self._params
        result = []
        for connector, expr, params in self._conditions:
            if isinstance(expr, Query):  # Nested conditions
                result.append(expr.params())
            else:
                result.append(params)
        return result
        
    def query_template(self, limit=True):
        if self._sql:
            return '{0} {1}'.format(
                self._sql,
                limit and self.extract_limit() or ''
            )
        return '{0} FROM {1} {2} {3} {4}'.format(
            self._type,
            self._model.Meta.table_safe,
            self.render_conditions() or '',
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
        return Query.raw_sql(self.query_template(), self.params(), self.using)
        
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
        if db.placeholder != '%s':
            sql = sql.replace('%s', db.placeholder)
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
