#!/usr/bin/env python
# Copyright (c) 2012-2013 Ivan Zakrevsky, Jared Kuolt and contributors
from setuptools import setup, find_packages
from autumn import version

version = '.'.join([str(x) for x in version])

setup(
    name='autumn2',
    version='0.6.1',

    packages = find_packages(),

    author="Ivan Zakrevsky",
    author_email="ivzak@yandex.ru",
    description="A minimal ORM",
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
    url="https://bitbucket.org/evotech/autumn",
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
