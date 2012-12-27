from __future__ import absolute_import, unicode_literals


def qn(name, using='default'):
    """Quotes DB name"""
    from .query import Query
    return Query(using=using).qn(name)
