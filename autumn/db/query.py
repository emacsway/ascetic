from __future__ import absolute_import, unicode_literals
import re
import copy
from autumn.db import quote_name
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
    'nin': '{field} NOT IN {val}',
    'exact': '{field} LIKE {val}',
    'iexact': 'LOWER({field}) LIKE LOWER({val})',
    'startswith': '{field} LIKE CONCAT({val}, "%")',
    'istartswith': 'LOWER({field}) LIKE LOWER(CONCAT({val}, "%"))',
    'endswith': '{field} LIKE CONCAT("%", {val})',
    'iendswith': 'LOWER({field}) LIKE LOWER(CONCAT("%", {val}))',
    'contains': '{field} LIKE CONCAT("%", {val}, "%")',
    'icontains': 'LOWER({field}) LIKE LOWER(CONCAT("%", {val}, "%"))',
    # TODO: 'range': '{field} BETWEEN {val0} AND {val1}',
    # TODO: 'isnull': '{field} IS{inv} NULL',
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

    Support JOIN:

        Author.get().table_as('a').join(
            'INNER JOIN', Book.get().table_as('b').filter('a.id = b.author_id')
        ).filter(a__id__in=(3,5)).order_by('-a.id')
        or:
        Author.get().table_as('a').join(
            'INNER JOIN', Book.get().table_as('b').filter(a__id=n('b.author_id'))
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

    def __init__(self, fields=None, model=None, using=None):
        self._model = None
        self._distinct = False
        self._fields = fields or ['*']
        self._table = None
        self._alias = None
        self._join_type = None
        self._join_tables = []
        self._conditions = []
        self._order_by = []
        self._group_by = []
        self._having = None
        self._limit = ()
        self._cache = None
        self._name = None
        self._sql = None
        self._params = []
        self.using = using
        if model:
            self._set_table(model)

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

    def quote_name(self, name):
        return quote_name(name, self.using)

    def qn(self, name):  # just a short alias
        return self.quote_name(name)

    def clone(self):
        return copy.deepcopy(self)

    def reset(self):
        return Query(model=self._model, using=self.using)

    def dialect(self):
        engine = Query.get_db(self.using).engine
        return DIALECTS.get(engine, engine)

    def get_operator(self, key):
        ops = copy.copy(OPERATORS)
        ops.update(getattr(Query.get_db(self.using), 'operators', {}))
        return ops.get(key, None)

    def raw(self, sql, *params):
        if isinstance(sql, Query):
            return sql
        self = self.reset()
        self._sql = sql
        self._params = params or []
        return self

    def name(self, name):
        if isinstance(name, Query):
            return name
        self = self.reset()
        self._name = name
        return self

    def n(self, name):  # just a short alias
        return self.name(name)

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

    def fields(self, *args, **kwargs):
        if args:
            self = self.clone()
            args = list(args)
            if kwargs.get('reset', None) is True:
                del kwargs['reset']
                self._fields = []
            if isinstance(args[0], (list, tuple)):
                self._fields = map(self.name, args.pop(0))
            self._fields += map(self.name, args)
            for k, v in kwargs.items():
                self._fields.append(self.name(v).as_(k))
            return self
        return self._fields

    def as_(self, alias=None):
        if alias is not None:
            self = self.clone()
            self._alias = alias
            return self
        return self._alias

    def _set_table(self, table=None, alias=None, **kwargs):
        from autumn.models import Model
        if kwargs:
            alias, table = kwargs.items()[0]
        if issubclass(table, Model):
            self._model = table
            self._table = Query().name(table.Meta.table)
            if not self.using:
                self.using = table.using
        elif isinstance(table, string_types):
            self._model = None
            self._table = Query().name(table)
        else:
            raise Exception('Table slould be instance of Model or str.')
        if alias:
            self._table = self._table.as_(alias)
        return self

    def table(self, table=None, alias=None, **kwargs):
        self = self.clone()
        return self._set_table(table, alias, **kwargs)

    def table_as(self, alias):
        self = self.clone()
        self._table = self._table.as_(alias)
        return self

    def join(self, join_type, expr):
        self = self.clone()
        expr._join_type = join_type
        self._join_tables.append(expr)
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

    def group_by(self, *args, **kwargs):
        if args:
            self = self.clone()
            args = list(args)
            if 'reset' in kwargs:
                self._group_by = []
            if isinstance(args[0], (list, tuple)):
                self._group_by = args.pop(0)
            self._group_by += args
            return self
        return self._group_by

    def having(self, expr=None):
        """Having. expr should be an instanse of Query."""
        if expr is not None:
            self = self.clone()
            self._having = expr
            return self
        return self._having

    def order_by(self, *fields, **kwargs):
        self = self.clone()
        for field in fields:
            direction = 'desc' in kwargs and 'DESC' or 'ASC'
            if field[0] == '-':
                direction = 'DESC'
                field = field[1:]
            self._order_by.append([field, direction])
        return self

    def flatten_expr(self, expr, params):
        expr_parts = expr.split(PLACEHOLDER)
        expr_plhs = [PLACEHOLDER, ] *  (len(expr_parts) - 1) + ['', ]
        for i, param in enumerate(params[:]):
            if isinstance(param, (list, tuple)):
                expr_plhs[i] = '({0})'.format(', '.join([PLACEHOLDER, ] * len(param)))
            if isinstance(param, Query):  # SubQuery
                expr_plhs[i] = self.chrender(param)
        expr_final = []
        for pair in zip(expr_parts, expr_plhs):
            expr_final += pair
        expr = ''.join(expr_final)
        return expr

    def flatten_params(self, params):
        flat = []
        for param in params:
            if isinstance(param, Query):  # SubQuery
                flat += self.chparams(param)
            elif isinstance(param, (list, tuple)):
                flat += param
            else:
                flat.append(param)
        return flat

    @property
    def top_parent(self):
        current = self
        while getattr(curent, 'parent', None) is not None:
            curent = curent.parent
        return current

    def chrender(self, expr, parentheses=True):
        """Renders child"""
        if isinstance(expr, Query):
            expr.using = self.using
            expr.parent = self
            r = expr.render()
            if parentheses and expr._alias is None and expr._name is None and expr._join_type is None:
                r = '({0})'.format(r)
            return r
        return expr

    def chparams(self, expr):
        """Returns parameters for child"""
        if isinstance(expr, Query):
            expr.using = self.using
            expr.parent = self
            return expr.params()
        return []

    def _render_conditions(self, conditions):
        parts = []
        for connector, inversion, expr, params in conditions:
            if isinstance(expr, Query):  # Nested conditions
                expr = self.chrender(expr)
            if not re.search(r'[ =!<>]', expr, re.S):
                tpl = self.get_operator(isinstance(params[0], (list, tuple)) and 'in' or 'eq')
                if LOOKUP_SEP in expr:
                    expr_parts = expr.split(LOOKUP_SEP)
                    if self.get_operator(expr_parts[-1]) is not None:  # TODO: check for expr_parts[-1] is not field name
                        tpl = self.get_operator(expr_parts.pop())
                    expr = '.'.join(expr_parts)
                expr = tpl.replace('%', '%%').format(field=self.qn(expr), val=PLACEHOLDER)
            if inversion:
                expr = '{0} ({1}) '.format('NOT', expr)
            if parts:
                expr = '{0} ({1}) '.format(connector, expr)
            parts.append(expr)
        return ' '.join(parts)

    def render_conditions(self):
        return self._render_conditions(self._conditions)

    def render_order_by(self):
        return ', '.join([' '.join([self.chrender(i[0]), i[1]]) for i in self._order_by])
        
    def render_limit(self):
        if len(self._limit):
            return ', '.join(str(i) for i in self._limit)
        
    def render(self, order_by=True, limit=True):
        result = None
        if self._name:
            # alias of table can be changed during Query building.
            # Do not add prefix for columns here
            result = self.qn(self._name)

        elif self._sql:
            parts = [self._sql]
            if limit and self._limit:
                parts += ['LIMIT', self.render_limit()]
            result = ' '.join(parts)

        elif self._join_type:
            parts = []
            parts += [self._join_type]
            if self._table is not None:
                parts += [self.chrender(self._table)]
            if self._conditions:
                parts += ['ON', self.render_conditions()]
            if self._join_tables:
                parts += ['({0})'.format(' '.join([i.self.chrender() for i in self._join_tables]))]  # Nestet JOIN
            result = ' '.join(parts)

        elif self._table is not None:
            parts = []
            parts += ['SELECT']
            if self._distinct:
                parts += ['DISTINCT']
            parts += [', '.join(map(self.chrender, self._fields))]
            parts += ['FROM', self.chrender(self._table)]
            parts += [self.chrender(t) for t in self._join_tables]
            if self._conditions:
                parts += ['WHERE', self.render_conditions()]
            if self._group_by:
                parts += ['GROUP BY', ', '.join(map(self.chrender, self._group_by))]
            if self._having is not None:
                parts += ['HAVING', self.chrender(self._having, False)]
            if order_by and self._order_by:
                parts += ['ORDER BY', self.render_order_by()]
            if limit and self._limit:
                parts += ['LIMIT', self.render_limit()]
            result = ' '.join(parts)

        elif self._conditions:
            result = self.render_conditions()

        result = self.flatten_expr(result, self._raw_params())

        if self._alias:
            if ' ' in result:
                result = '({0})'.format(result)
            result = ' '.join([result, 'AS', self.qn(self._alias)])
        return result

    def _raw_params(self):
        result = []
        if self._name:
            result = []

        elif self._sql:
            result = self._params

        elif self._join_type:
            result += self.chparams(self._table)
            for connector, inversion, expr, expr_params in self._conditions:
                result += self.chparams(expr) if isinstance(expr, Query) else expr_params
            for i in self._join_tables:
                result += self.chparams(i)

        elif self._table is not None:
            for i in self._fields:
                result += self.chparams(i)
            result += self.chparams(self._table)
            for i in self._join_tables:
                result += self.chparams(i)
            for connector, inversion, expr, expr_params in self._conditions:
                result += self.chparams(expr) if isinstance(expr, Query) else expr_params
            for i in self._group_by:
                result += self.chparams(i)
            if self._having is not None:
                result += self.chparams(self._having)
            for i in self._order_by:
                result += self.chparams(i[0])

        elif self._conditions:
            for connector, inversion, expr, expr_params in self._conditions:
                    result += self.chparams(expr) if isinstance(expr, Query) else expr_params

        return result

    def params(self):
        return self.flatten_params(self._raw_params())

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


class MetaN(type):
    def __getattr__(cls, key):
        if key[0] == '_':
            raise AttributeError
        return n(key.replace(LOOKUP_SEP, '.'))


class N(MetaN(bytes("NewBase"), (object, ), {})):
    pass

Q = Query
q = Query()
n = q.n
