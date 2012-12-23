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


PLACEHOLDER = '%s'
LOOKUP_SEP = '__'
OPERATORS = {
    'eq': '{field} = {val}',
    'neq': '{field} != {val}',
    'lt': '{field} < {val}',
    'lte': '{field} <= {val}',
    'gt': '{field} > {val}',
    'gte': '{field} >= {val}',
    'in': '{field} IN {val}',
    'not_in': '{field} IN {val}',
    'exact': '{field} LIKE {val}',
    'iexact': 'LOWER({field}) LIKE LOWER({val})',
    'startswith': '{field} LIKE CONCAT({val}, "%")',
    'istartswith': 'LOWER({field}) LIKE LOWER(CONCAT({val}, "%"))',
    'endswith': '{field} LIKE CONCAT("%", {val})',
    'iendswith': 'LOWER({field}) LIKE LOWER(CONCAT("%", {val}))',
    'contains': '{field} LIKE CONCAT("%", {val}, "%")',
    'icontains': 'LOWER({field}) LIKE LOWER(CONCAT("%", {val}, "%"))',
}
DIALECTS = {
    'sqlite3': 'sqlite',
    'mysql': 'mysql',
    'postgresql': 'postgres',
    'postgresql_psycopg2': 'postgres',
    'postgis': 'postgres',
    'oracle': 'oracle',
}


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

    def __init__(self, fields=None, model=None, using=None):
        from autumn.models import Model
        self._distinct = False
        self._fields = fields or ['*']
        self._from = None
        self._conditions = []
        self._order_by = []
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

    def dialect(self):
        engine = Query.get_db(self.using).engine
        return DIALECTS.get(engine, engine)

    def get_operator(self, key):
        ops = copy.copy(OPERATORS)
        ops.update(getattr(Query.get_db(self.using), 'operators', {}))
        return ops.get(key, None)

    def raw(self, sql, params=None):
        self._sql = sql
        self._params = params or []
        return self

    def count(self):
        return Query.raw_sql(
            "SELECT COUNT(1) as c FROM ({0}) as t".format(
                self.render(order_by=False)
            ),
            self.params(), self.using
        ).fetchone()[0]

    def distinct(self, val=None):
        if val is not None:
            self = self.clone()
            self._distinct = val
            return self
        return self._distinct

    def from_(self, expr):
        self = self.clone()
        self._from = expr
        return self

    def _add_condition(self, conditions, connector, inversion, *args, **kwargs):
        if args:
            conditions.append((connector, inversion, args[0], list(args[1:])))
        for k, v in kwargs.items():
            conditions.append((connector, inversion, k, [v,]))
        return self

    def filter(self, *args, **kwargs):
        self = self.clone()
        return self._add_condition(self._conditions, 'AND', False, *args, **kwargs)

    def or_filter(self, *args, **kwargs):
        self = self.clone()
        return self._add_condition(self._conditions, 'OR', False, *args, **kwargs)
        return self

    def exclude(self, *args, **kwargs):
        self = self.clone()
        return self._add_condition(self._conditions, 'AND', True, *args, **kwargs)

    def or_exclude(self, *args, **kwargs):
        self = self.clone()
        return self._add_condition(self._conditions, 'OR', True, *args, **kwargs)

    def order_by(self, *fields, **kwargs):
        self = self.clone()
        for field in fields:
            direction = 'desc' in kwargs and 'DESC' or 'ASC'
            if field[0] == '-':
                direction = 'DESC'
                field = field[1:]
            self._order_by.append([field, direction])
        return self

    def _render_conditions(self, conditions):
        parts = []
        for connector, inversion, expr, params in conditions:
            if isinstance(expr, Query):  # Nested conditions
                expr.using = self.using
                expr = expr.render()
            if not re.search(r'[ =!<>]', expr, re.S):
                tpl = self.get_operator(isinstance(params[0], (list, tuple)) and 'in' or 'eq')
                if LOOKUP_SEP in expr:
                    expr_parts = expr.split(LOOKUP_SEP)
                    if self.get_operator(expr_parts[-1]) is not None:  # TODO: check for expr_parts[-1] is not field name
                        tpl = self.get_operator(expr_parts.pop())
                    expr = '.'.join(map(escape, expr_parts))
                else:
                    expr = escape(expr)
                expr = tpl.replace('%', '%%').format(field=expr, val=PLACEHOLDER)
            if inversion:
                expr = '{0} ({1}) '.format('NOT', expr)
            if parts:
                expr = '{0} ({1}) '.format(connector, expr)

            expr_parts = expr.split(PLACEHOLDER)
            expr_plhs = [PLACEHOLDER, ] *  (len(expr_parts) - 1) + ['', ]
            for i, param in enumerate(params[:]):
                if isinstance(param, (list, tuple)):
                    expr_plhs[i] = '({0})'.format(', '.join([PLACEHOLDER, ] * len(param)))
                if isinstance(param, Query):  # SubQuery
                    param.using = self.using
                    expr_plhs[i] = '({0})'.format(param.render())
            expr_final = []
            for pair in zip(expr_parts, expr_plhs):
                expr_final += pair
            expr = ''.join(expr_final)

            parts.append(expr)
        return ' '.join(parts)

    def render_conditions(self):
        return self._render_conditions(self._conditions)

    def render_order_by(self):
        return ', '.join([' '.join(i) for i in self._order_by])
        
    def render_limit(self):
        if len(self._limit):
            return ', '.join(str(i) for i in self._limit)
        
    def render(self, order_by=True, limit=True):
        if self._sql:
            parts = [self._sql]
            if limit and self._limit:
                parts += ['LIMIT', self.render_limit()]
            return ' '.join(parts)

        elif self._from:
            parts = []
            parts += ['SELECT']
            if self._distinct:
                parts += ['DISTINCT']
            parts += [', '.join(self._fields)]
            parts += ['FROM', self._from]
            if self._conditions:
                parts += ['WHERE', self.render_conditions()]
            if order_by and self._order_by:
                parts += ['ORDER BY', self.render_order_by()]
            if limit and self._limit:
                parts += ['LIMIT', self.render_limit()]
            return ' '.join(parts)

        elif self._conditions:
            return self.render_conditions()
        
    def params(self):
        if self._sql:
            return self._params
        result = []
        for connector, inversion, expr, params in self._conditions:
            if isinstance(expr, Query):  # Nested conditions
                result += expr.params()
            else:
                for i, param in enumerate(params[:]):
                    if isinstance(param, Query):  # SubQuery
                        params[i: i + 1] = param.params()
                    elif isinstance(param, (list, tuple)):
                        params[i: i + 1] = param
                result += params
        return result
        
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
        return Query.raw_sql(self.render(), self.params(), self.using)
        
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
