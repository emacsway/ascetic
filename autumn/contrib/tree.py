from __future__ import absolute_import
from sqlbuilder import smartsql
from .. import models
from . import polymorphic

# Under construction!!! Not testet yet!!!


def get_root_model(cls):
    """Returns base model for MTI"""
    if isinstance(cls, polymorphic.PolymorphicModel) and cls.root_model:
        return cls.root_model
    return cls


class MpModel(object):
    """The simplest Materialized Path realization.

    Strong KISS principle.
    You shoul to create CHAR field with name "tree_path",
    and "parent_id".
    """

    PATH_SEPARATOR = '/'
    PATH_DIGITS = 10

    parent = models.ForeignKey(
        'self',
        rel_name="children"
    )

    class Meta:
        abstract = True

    def save(self, using=None):
        """Sets content_type and calls parent method."""
        using = using or self._meta.using
        base_model = get_root_model(type(self))
        if self.pk:
            old_tree_path = base_model.qs.get(pk=self.pk).tree_path
        else:
            old_tree_path = None
        super(MpModel, self).save(using=using)

        tree_path = str(self.pk).zfill(self.PATH_DIGITS)
        if self.parent:
            tree_path = self.PATH_SEPARATOR.join((self.parent.tree_path, tree_path))
        self.tree_path = tree_path
        type(self).qs.using(using).where(
            type(self).s.pk == self.pk
        ).update({
            'tree_path': self.tree_path
        })

        if old_tree_path is not None and old_tree_path != tree_path:
            for obj in type(self).qs.using(using).where(
                type(self).s.tree_path.startswith(old_tree_path) &
                type(self).s.pk != self.pk
            ).iterator():
                type(self).qs.using(using).where(
                    type(self).s.pk == obj.pk
                ).update({
                    'tree_path': obj.tree_path.replace(old_tree_path, tree_path)
                })
        return self

    def get_ancestors_chained(self, root=False, me=False, reverse=True):
        objs = []
        current = self
        while (current if root else current.parent_id):
            if current != self or me:
                objs.append(current)
            current = current.parent
        if reverse:
            objs.reverse()
        return objs

    def get_ancestors_by_path(self, root=False, me=False, reverse=True):
        base_model = get_root_model(type(self))
        t = base_model.s
        qs = base_model.qs.where(
            smartsql.P(self.tree_path).startswith(t.tree_path)
        )
        if not root:
            qs = qs.where(base_model.s.parent.is_not(None))
        if not me:
            qs = qs.where(base_model.s.pk != self.pk)
        if reverse:
            qs = qs.order_by(base_model.s.tree_path)
        else:
            qs = qs.order_by(base_model.s.tree_path.desc())
        return qs

    def get_ancestors_by_paths(self, root=False, me=False, reverse=True):
        base_model = get_root_model(type(self))
        qs = base_model.qs
        cond = None
        paths = self.tree_path.split(self.PATH_SEPARATOR)
        while paths:
            q = (base_model.s.tree_path == self.PATH_SEPARATOR.join(paths))
            cond = cond | q if cond is not None else q
            paths.pop()

        qs = qs.where(cond)

        if not root:
            qs = qs.where(base_model.s.parent.is_not(None))
        if not me:
            qs = qs.where(base_model.s.pk != self.pk)

        if reverse:
            qs = qs.order_by(base_model.s.tree_path)
        else:
            qs = qs.order_by(base_model.s.tree_path.desc())
        return qs

    def get_ancestors(self, root=False, me=False, reverse=True):
        return self.get_ancestors_by_paths(root=root, me=me, reverse=reverse)

    def get_hierarchical_name(self, sep=', ', root=False, me=True, reverse=True):
        """returns children QuerySet instance for given parent_id"""
        return sep.join(map(unicode, self.get_ancestors(root=root, me=me, reverse=reverse)))

    def get_children(self):
        """Fix for MTI"""
        base_model = get_root_model(type(self))
        return base_model.qs.where(base_model.s.parent == self.pk)

    def _descendants(self):
        r = list(self.get_children())
        for i in r:
            r += i._descendants()
        return r

    def get_descendants_recursive(self, me=False):
        r = []
        if me:
            r.append(self)
        r += self._descendants()
        return r

    def get_descendants(self, me=False):
        base_model = get_root_model(type(self))
        qs = base_model.qs.where(base_model.s.tree_path.startswith(self.tree_path))
        if not me:
            qs = qs.where(base_model.s.pk != self.pk)
        return qs
