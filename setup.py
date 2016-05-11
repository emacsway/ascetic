#!/usr/bin/env python
# Copyright (c) 2012-2013 Ivan Zakrevsky, Jared Kuolt and contributors
import os.path
from setuptools import setup, find_packages

setup(
    name='ascetic',
    version='0.7.2.29',

    packages = find_packages(exclude=('examples*',)),
    include_package_data=True,

    author="Ivan Zakrevsky",
    author_email="ivzak@yandex.ru",
    description="Lightweight python datamapper ORM (Object-relational mapper).",
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
    url="https://bitbucket.org/emacsway/ascetic",
    license="MIT License",
    keywords = "ORM Database DataMapper SQL",
    install_requires=[
        'sqlbuilder >= 0.7.9.39'
    ],
    classifiers = [
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
