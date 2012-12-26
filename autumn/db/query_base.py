from __future__ import absolute_import, unicode_literals
import re
import copy

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


class Query(object):
    """Abstract SQL Builder, can be used without ORM."""
    def __init__(self, model=None, using=None):
        self._model = None
        self._distinct = False
        self._fields = []
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
        self.parent = None
        self.using = using
        if model:
            self._set_table(model)

    def __getitem__(self, k):
        if self._cache != None:
            return self._cache[k]

        if isinstance(k, integer_types):
            self._limit = (k, 1)
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

    def get_data(self):
        return []

    def quote_name(self, name):
        return name

    def qn(self, name):  # just a short alias
        return self.quote_name(name)

    def clone(self):
        return copy.deepcopy(self)

    def reset(self):
        return type(self)(model=self._model, using=self.using)

    def dialect(self):
        return 'postgres'

    def get_operator(self, key):
        return OPERATORS.get(key, None)

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
        self = type(self)()
        self._name = name
        return self

    def n(self, name):  # just a short alias
        return self.name(name)

    def count(self):
        return self.reset().raw(
            "SELECT COUNT(1) as c FROM %s as t",
            self.order_by(reset=True)
        )

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
                self._fields = map(self.n, args.pop(0))
            self._fields += map(self.n, args)
            for k, v in kwargs.items():
                self._fields.append(self.n(v).as_(k))
            return self
        return self._fields

    def as_(self, alias=None):
        if alias is not None:
            self = self.clone()
            self._alias = alias
            return self
        return self._alias

    def _set_table(self, table=None, alias=None, **kwargs):
        if kwargs:
            alias, table = kwargs.items()[0]
        if isinstance(table, string_types):
            self._model = None
            self._table = type(self)().n(table)
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
        if args:  # TODO: cotvert string to self.raw() here?
            conditions.append((connector, inversion, args[0], list(args[1:])))
        for k, v in kwargs.items():
            conditions.append((connector, inversion, k, [v, ]))
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
                self._group_by = map(self.n, args.pop(0))
            self._group_by += map(self.n, args)
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
        if kwargs.get('reset', None) is True:
            del kwargs['reset']
            self._order_by = []
        for field in fields:
            direction = 'desc' in kwargs and 'DESC' or 'ASC'
            if field[0] == '-':
                direction = 'DESC'
                field = field[1:]
            self._order_by.append([self.n(field), direction])
        return self

    def flatten_expr(self, expr, params):
        expr_parts = expr.split(PLACEHOLDER)
        expr_plhs = [PLACEHOLDER, ] * (len(expr_parts) - 1) + ['', ]
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
        while current.parent is not None:
            current = current.parent
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

    def chrender_field(self, f):
        if f._name and '.' not in f._name:
            f._name = '.'.join([
                self._table._alias or self._table._name, f._name
            ])
        return self.chrender(f)

    def render(self, order_by=True, limit=True, **kwargs):
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
            fields = map(self.chrender_field, self._fields)
            for t in self._join_tables:
                fields += map(t.chrender_field, t._fields)
            if not fields:
                fields = ['*']
            parts += [', '.join(fields)]
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
            for t in self._join_tables:
                for i in t._fields:
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
