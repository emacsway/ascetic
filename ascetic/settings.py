import os

DATABASES_MYSQL = {
    'default': {
        'engine': "mysql",
        'user': "devel",
        'db': "devel_ascetic",
        'passwd': "devel",
        'debug': True,
        'initial_sql': "SET NAMES 'UTF8';",
    }
}

DATABASES_POSTGRESQL = {
    'default': {
        'engine': "postgresql",
        'user': "devel",
        'database': "devel_ascetic",
        'password': "devel",
        'debug': True,
        'initial_sql': "SET NAMES 'UTF8';",
    }
}

DATABASES = DATABASES_POSTGRESQL

DEBUG = True

LOGGER_INIT = 'ascetic.settings.init_logger'


def init_logger(settings):
    import logging
    if settings.DEBUG:
        import warnings
        warnings.simplefilter('default')

        logger = logging.getLogger('ascetic')
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

try:
    m = __import__(os.getenv('ASCETIC_SETTINGS', 'ascetic_settings'))
except ImportError:
    pass
else:
    for key in dir(m):
        if key[0] != '_':
            globals()[key] = getattr(m, key)


def configure(settings):
    globals().update(settings)
