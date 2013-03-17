DATABASES = {
    'default': {
        'engine': "mysql",
        'user': "devel",
        'db': "devel_autumn",
        'passwd': "devel",
        'debug': True,
        'initial_sql': "SET NAMES 'UTF8';",
    }
}


def send_signal(*a, **kw):
    """Send signal abstract function. You should to override it.

    For example, you can use one from next event systems:
    https://github.com/jesusabdullah/pyee
    https://bitbucket.org/jek/blinker
    https://launchpad.net/pydispatcher
    https://github.com/theojulienne/PySignals
    https://github.com/olivierverdier/dispatch
    and others.
    """
    from autumn import signals
    return getattr(signals, kw.pop('signal')).send(*a, **kw)

try:
    from autumn_settings import *
except ImportError:
    pass
