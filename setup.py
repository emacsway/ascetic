from distutils.core import setup
from autumn import version

version = '.'.join([str(x) for x in version])

setup(
    name='autumn',
    version=version,
    description="A minimal ORM",
    author="Ivan Zakrevsky, Jared Kuolt and contributors",
    author_email="ivzak@yandex.ru",
    url="https://bitbucket.org/evotech/autumn",
    packages=['autumn', 'autumn.db', 'autumn.tests'],
    license="MIT License",
    keywords = "ORM Database SQL",
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
