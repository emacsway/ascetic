from __future__ import absolute_import, unicode_literals
import logging
from autumn import settings

if settings.DEBUG:
    import warnings
    warnings.simplefilter('default')

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
