from __future__ import absolute_import, unicode_literals
import re
import copy

try:
    from autumn import settings
except ImportError:
    settings = None

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
    'notin': '{field} NOT IN {val}',
    'exact': '{field} LIKE {val}',
    'iexact': '{field} ILIKE {val}',
    'startswith': '{field} LIKE CONCAT({val}, "%")',
    'istartswith': '{field} ILIKE CONCAT({val}, "%")',
    'endswith': '{field} LIKE CONCAT("%", {val})',
    'iendswith': '{field} ILIKE CONCAT("%", {val})',
    'contains': '{field} LIKE CONCAT("%", {val}, "%")',
    'icontains': '{field} ILIKE CONCAT("%", {val}, "%")',
    'range': '{field} BETWEEN {val} AND {val}',
    'isnull': '{field} IS {val} NULL',
}
OPERATOR_DIALECTS = {
    'sqlite': {
        'exact': '{field} GLOB {val}',
        'iexact': '{field} LIKE {val}',
        'startswith': '{field} GLOB CONCAT({val}, "%")',
        'istartswith': '{field} LIKE CONCAT({val}, "%")',
        'endswith': '{field} GLOB CONCAT("%", {val})',
        'iendswith': '{field} LIKE CONCAT("%", {val})',
        'contains': '{field} GLOB CONCAT("%", {val}, "%")',
        'icontains': '{field} LIKE CONCAT("%", {val}, "%")',
    },
    'mysql': {
        'exact': '{field} LIKE BINARY {val}',
        'iexact': '{field} LIKE {val}',
        'startswith': '{field} LIKE BINARY CONCAT({val}, "%")',
        'istartswith': '{field} LIKE CONCAT({val}, "%")',
        'endswith': '{field} LIKE BINARY CONCAT("%", {val})',
        'iendswith': '{field} LIKE CONCAT("%", {val})',
        'contains': '{field} LIKE BINARY CONCAT("%", {val}, "%")',
        'icontains': '{field} LIKE CONCAT("%", {val}, "%")',
    },
}


class Query(object):
    """Abstract SQL Builder, can be used without ORM.

    You should mark non-field names as qs.n('name_here').
    All strings will be converted to DB field.
    """
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
        self._field = None
        self._name = None
        self._sql = None
        self._params = []
        self._inline = False
        self._dialect = 'postgres'
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

    def qn(self, name):
        """quotes name"""
        return name

    def clone(self):
        return copy.deepcopy(self)

    def reset(self):
        return type(self)(self._model, self.using)

    def inline(self, key=None):
        if key is not None:
            self = self.clone()
            self._inline = key
            return self
        return self._inline

    def dialect(self):
        return self._dialect

    def get_operator(self, key):
        ops = copy.copy(OPERATORS)
        ops.update(OPERATOR_DIALECTS.get(self.dialect(), {}))
        return ops.get(key, None)

    def raw(self, sql, *params):
        if isinstance(sql, Query):
            return sql
        self = self.reset()
        self._sql, self._params = sql, params
        return self

    def n(self, name, attr='_name'):
        """Makes DB name"""
        if isinstance(name, Query):
            return name
        self = type(self)()
        setattr(self, attr, name)
        return self.inline(True)

    def f(self, name):
        """Makes DB field"""
        return self.n(name, '_field')

    def count(self):
        return self.reset().raw(
            "SELECT COUNT(1) as c FROM %s as t", self.order_by(reset=True)
        )

    def distinct(self, val=None):
        if val is not None:
            self = self.clone()
            self._distinct = val
            return self
        return self._distinct

    def fields(self, *args, **kwargs):
        if args:
            self, args = self.clone(), list(args)
            if kwargs.get('reset', None) is True:
                del kwargs['reset']
                self._fields = []
            if isinstance(args[0], (list, tuple)):
                self._fields = map(self.f, args.pop(0))
            self._fields += map(self.f, args)
            for k, v in kwargs.items():
                self._fields.append(self.f(v).as_(k))
            return self
        return self._fields

    def as_(self, alias=None):
        if alias is not None:
            self = self.clone()
            self._alias = alias
            return self.inline(True)
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
        return self.clone()._set_table(table, alias, **kwargs)

    def table_as(self, alias):
        self = self.clone()
        self._table = self._table.as_(alias)
        return self

    def join(self, join_type, expr):
        self = self.clone()
        expr._join_type = join_type
        self._join_tables.append(expr.inline(True))
        return self

    def _add_condition(self, conditions, connector, inversion, *args, **kwargs):
        if args:  # TODO: cotvert string to self.raw() here?
            conditions.append((connector, inversion, args[0], list(args[1:])))
        for k, v in kwargs.items():
            op = k.split(LOOKUP_SEP).pop()
            if op == 'isnull':
                vs = [self.raw('' if v else 'NOT').inline(True)]
            elif op == 'range':
                vs = v
            else:
                vs = [v]
            conditions.append((connector, inversion, k, vs))
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
            self, args = self.clone(), list(args)
            if 'reset' in kwargs:
                self._group_by = []
            if isinstance(args[0], (list, tuple)):
                self._group_by = map(self.f, args.pop(0))
            self._group_by += map(self.f, args)
            return self
        return self._group_by

    def having(self, expr=None):
        """Having. expr should be an instanse of Query."""
        if expr is not None:
            self = self.clone()
            self._having = expr.inline(True)
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
            self._order_by.append([self.f(field), direction])
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

    def chrender(self, expr, inline=False):
        """Renders child"""
        if isinstance(expr, Query):
            expr.parent, expr.using = self, self.using
            r = expr.render()
            if not (inline or expr.inline()):
                r = '({0})'.format(r)
            return r
        return expr

    def chparams(self, expr):
        """Returns parameters for child"""
        if isinstance(expr, Query):
            expr.parent, expr.using = self, self.using
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

    def _f_in_model(self, f):
        return True

    def render_field(self):
        f, cur = self._field, self
        if '.' not in f:
            while cur is not None:
                if cur._table is not None and cur._f_in_model(f):
                    f = '.'.join([cur._table._alias or cur._table._name, f])
                    break
                cur = cur.parent
        if settings:
            result = {'field': f}
            # In signal handler we can obtain Query() instance and take
            # model from it by field prefix (table name or alias).
            # We can localize fieldname and return it to result
            settings.send_signal(signal='field_conversion', sender=self, result=result, field=f)
            f = result['field']
        return self.qn(f)

    def render(self, order_by=True, limit=True, **kwargs):
        result = None
        if self._field:
            result = self.render_field()

        elif self._name:
            result = self.qn(self._name)

        elif self._sql is not None:
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
            fields = map(self.chrender, self._fields)
            for t in self._join_tables:
                fields += map(t.chrender, t._fields)
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
                parts += ['HAVING', self.chrender(self._having)]
            if order_by and self._order_by:
                parts += ['ORDER BY', self.render_order_by()]
            if limit and self._limit:
                parts += ['LIMIT', self.render_limit()]
            result = ' '.join(parts)

        elif self._conditions:
            result = self.render_conditions()

        result = self.flatten_expr(result, self._raw_params())

        if self._alias:
            if ' ' in result or ',' in result:
                result = '({0})'.format(result)
            result = ' '.join([result, 'AS', self.qn(self._alias)])
        return result

    def _raw_params(self):
        result = []
        if self._sql:
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
                    result += t.chparams(i)
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
