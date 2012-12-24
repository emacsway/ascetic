from __future__ import absolute_import, unicode_literals
from functools import partial
from autumn.db.connection import connections


def quote_name(name, using='default'):
    
    if '.' in name:
        return '.'.join(map(partial(quote_name, using=using), name.split('.')))
    call = getattr(connections[using], 'quote_name', None)
    if call:
        return call(name)
    return '`{0}`'.format(name.replace('`', ''))
