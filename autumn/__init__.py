from __future__ import absolute_import, unicode_literals
import logging
from autumn import settings

__copyright__ = 'Copyright (c) 2008 Jared Kuolt'

version = (0, 5, 1, )
version_string = "Autumn ORM version {0:d}.{1:d}.{2:d}".format(*version)

if settings.DEBUG:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
