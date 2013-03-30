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

SIGNAL_SEND_HANDLER = 'autumn.signals.send_signal'

try:
    from autumn_settings import *
except ImportError:
    pass
