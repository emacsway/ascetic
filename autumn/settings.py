import os

DATABASES = {
    'default': {
        'engine': "mysql",
        'user': "devel",
        'db': "devel_autumn",
        'passwd': "devel",
        'debug': True,
        'initial_sql': "SET NAMES 'UTF8';",
        'thread_safe': True,
    }
}

DATABASES = {
    'default': {
        'engine': "postgresql",
        'user': "devel",
        'database': "devel_autumn",
        'password': "devel",
        'debug': True,
        'initial_sql': "SET NAMES 'UTF8';",
        'thread_safe': True,
    }
}

DEBUG = True

SIGNAL_SENDER = 'autumn.signals.send_signal'

try:
    m = __import__(os.getenv('AUTUMN_SETTINGS', 'autumn_settings'))
except ImportError:
    pass
else:
    for key in dir(m):
        if key[0] != '_':
            globals()[key] = getattr(m, key)
