from __future__ import absolute_import
from sqlbuilder import smartsql
from ..models import ForeignKey, RelationDescriptor, to_tuple, mapper_registry
from ..utils import cached_property

# Under construction!!! Not testet yet!!!


class MpMapper(object):
    """The simplest Materialized Path realization.

    Strong KISS principle.
    You shoul to create CHAR field with name "tree_path",
    and "parent_id".
    """

    PATH_SEPARATOR = '/'
    KEY_SEPARATOR = ':'
    PATH_DIGITS = 10

    def _do_prepare_model(self, model):
        setattr(model, 'parent', RelationDescriptor(ForeignKey(
            'self',
            field=tuple('parent_{}'.format(k) for k in to_tuple(self.pk)),
            rel_name="children"
        )))

    def _mp_encode(self, value):
        return str(value).replace('&', '&a').replace(self.KEY_SEPARATOR, '&k').replace(self.PATH_SEPARATOR, '&p')

    def _mp_decode(self, value):
        return value.replace('&p', self.PATH_SEPARATOR).replace('&k', self.KEY_SEPARATOR).replace('&a', '&')

    @cached_property
    def mp_root(self):
        return mapper_registry[self.relations['parent'].descriptor_class]

    def save(self, obj):
        """Sets content_type and calls parent method."""
        try:
            old_tree_path = self.get_original_data(obj)['tree_path']
        except (AttributeError, KeyError):
            old_tree_path = None

        super(MpMapper, self).save(obj)

        tree_path = self.KEY_SEPARATOR.join(self._mp_encode(i).zfill(self.PATH_DIGITS) for i in to_tuple(obj.pk))
        if obj.parent:
            tree_path = self.PATH_SEPARATOR.join((self.parent.tree_path, tree_path))

        if old_tree_path != tree_path:
            obj.tree_path = tree_path
            self.update_original_data(obj, tree_path=tree_path)
            self.mp_root.base_query.where(
                self.mp_root.sql_table.pk == obj.pk
            ).update({
                'tree_path': tree_path
            })

            if old_tree_path is not None:
                for obj in self.mp_root.base_query.where(
                    self.mp_root.sql_table.tree_path.startswith(old_tree_path) &
                    self.mp_root.sql_table.pk != obj.pk
                ).iterator():
                    self.mp_root.base_query.where(
                        self.mp_root.sql_table.pk == obj.pk
                    ).update({
                        'tree_path': tree_path + obj.tree_path[len(old_tree_path):]
                    })
        return self

    def get_ancestors_chained(self, obj, root=False, me=False, reverse=True):
        objs = []
        current = obj
        while (current if root else current.parent_id):
            if current != obj or me:
                objs.append(current)
            current = current.parent
        if reverse:
            objs.reverse()
        return objs

    def get_ancestors_by_path(self, obj, root=False, me=False, reverse=True):
        t = self.mp_root.sql_table
        q = self.query.where(
            smartsql.P(obj.tree_path).startswith(t.tree_path)
        )
        if not root:
            q = q.where(t.parent.is_not(None))
        if not me:
            q = q.where(t.pk != obj.pk)
        if reverse:
            q = q.order_by((t.tree_path,))
        else:
            q = q.order_by((t.tree_path.desc(),))
        return q

    def get_ancestors_by_paths(self, obj, root=False, me=False, reverse=True):
        q = self.query
        t = self.mp_root.sql_table
        cond = None
        paths = obj.tree_path.split(self.PATH_SEPARATOR)
        while paths:
            q = (t.tree_path == self.PATH_SEPARATOR.join(paths))
            cond = cond | q if cond is not None else q
            paths.pop()

        q = q.where(cond)

        if not root:
            q = q.where(t.parent.is_not(None))
        if not me:
            q = q.where(t.pk != obj.pk)

        if reverse:
            q = q.order_by(t.tree_path)
        else:
            q = q.order_by(t.tree_path.desc())
        return q

    def get_ancestors(self, obj, root=False, me=False, reverse=True):
        return self.get_ancestors_by_paths(obj, root=root, me=me, reverse=reverse)

    def get_hierarchical_name(self, obj, sep=', ', root=False, me=True, reverse=True, namegetter=unicode):
        """returns children QuerySet instance for given parent_id"""
        return sep.join(map(namegetter, self.get_ancestors(obj, root=root, me=me, reverse=reverse)))

    def get_children(self, obj):
        """Fix for MTI"""
        return self.query.where(self.mp_root.sql_table.parent == obj.pk)

    def _descendants(self, obj):
        r = list(self.get_children(obj))
        for i in r:
            r += i._descendants(obj)
        return r

    def get_descendants_recursive(self, obj, me=False):
        r = []
        if me:
            r.append(obj)
        r += self._descendants(obj)
        return r

    def get_descendants(self, obj, me=False):
        t = self.mp_root.sql_table
        q = self.query.where(t.tree_path.startswith(obj.tree_path))
        if not me:
            q = q.where(t.pk != obj.pk)
        return q


class MpModel(object):

    def get_ancestors(self, root=False, me=False, reverse=True):
        return mapper_registry[self.__class__].get_ancestors(self, root, me, reverse)

    def get_hierarchical_name(self, sep=', ', root=False, me=True, reverse=True, namegetter=unicode):
        return mapper_registry[self.__class__].get_hierarchical_name(self, sep, root, me, reverse, namegetter)

    def get_children(self):
        return mapper_registry[self.__class__].get_children(self)

    def get_descendants(self, me=False):
        return mapper_registry[self.__class__].get_descendants(self, me)
