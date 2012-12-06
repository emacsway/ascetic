from autumn import settings
from autumn.db.query import Query
from sqlbuilder import smartsql

SMARTSQL_ALIAS = getattr(settings, 'SQLBUILDER_SMARTSQL_ALIAS', 'ss')


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class Facade(object):
    """Facade for smartsql integration"""

    def __init__(self, model):
        """Constructor"""
        self.model = model
        self.table = Table(self.model.Meta.table)
        self.table.facade = self
        self.query_set = QS(self.table).fields(self.get_fields())
        self.query_set.facade = self

    def get_fields(self, prefix=None):
        """Returns field list."""
        if prefix is None:
            prefix = self.table
        result = []
        for f in self.model.Meta.fields:
            result.append(smartsql.Field(f, prefix))
        return result

    @property
    def qs(self):
        """Returns query set."""
        return self.query_set

    @property
    def t(self):
        """Returns table instance."""
        return self.table


class QS(smartsql.QS):
    """Query Set adapted."""

    def __len__(self):
        """Returns length or list."""
        return len(self.execute())

    def count(self):
        """Returns length or list."""
        return len(self.execute())

    def __iter__(self):
        """Returns iterator."""
        return iter(self.execute())

    def iterator(self):
        """Returns iterator."""
        return self.execute().iterator()

    def __getitem__(self, key):
        """Returns sliced self or item."""
        return self.execute()[key]

    def execute(self):
        """Implementation of query execution"""
        return Query(model=self.facade.model).raw(
            smartsql.sqlrepr(self), smartsql.sqlparams(self)
        )

    def result(self):
        """Result"""
        if self._action in ('select', 'count', ):
            return self
        return self.execute()


class Table(smartsql.Table):
    """Table class"""
    pass


@classproperty
def ss(cls):
    if getattr(cls, '_{0}'.format(SMARTSQL_ALIAS), None) is None:
        setattr(cls, '_{0}'.format(SMARTSQL_ALIAS), Facade(cls))
    return getattr(cls, '_{0}'.format(SMARTSQL_ALIAS))

setattr(Model, SMARTSQL_ALIAS, ss)
