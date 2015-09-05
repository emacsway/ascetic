import os

DATABASES_MYSQL = {
    'default': {
        'engine': "mysql",
        'user': "devel",
        'db': "devel_autumn",
        'passwd': "devel",
        'debug': True,
        'initial_sql': "SET NAMES 'UTF8';",
    }
}

DATABASES_POSTGRESQL = {
    'default': {
        'engine': "postgresql",
        'user': "devel",
        'database': "devel_autumn",
        'password': "devel",
        'debug': True,
        'initial_sql': "SET NAMES 'UTF8';",
    }
}

DATABASES = DATABASES_POSTGRESQL

DEBUG = True

SIGNAL_SENDER = 'autumn.signals.send_signal'

LOGGER_INIT = 'autumn.settings.init_logger'


def init_logger(settings):
    import logging
    if settings.DEBUG:
        import warnings
        warnings.simplefilter('default')

        logger = logging.getLogger('autumn')
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

try:
    m = __import__(os.getenv('AUTUMN_SETTINGS', 'autumn_settings'))
except ImportError:
    pass
else:
    for key in dir(m):
        if key[0] != '_':
            globals()[key] = getattr(m, key)


def configure(settings):
    globals().update(settings)
