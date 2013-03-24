#!/usr/bin/env python
# Copyright (c) 2012-2013 Ivan Zakrevsky, Jared Kuolt and contributors
import os.path
from setuptools import setup, find_packages

setup(
    name='autumn2',
    version='0.6.1',

    packages = find_packages(),

    author="Ivan Zakrevsky",
    author_email="ivzak@yandex.ru",
    description="A minimal python ORM",
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
    url="https://bitbucket.org/evotech/autumn",
    license="MIT License",
    keywords = "ORM Database SQL",
    install_requires=[
        'sqlbuilder >= 0.7.1'
    ],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
