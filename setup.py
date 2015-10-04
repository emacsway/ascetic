#!/usr/bin/env python
# Copyright (c) 2012-2013 Ivan Zakrevsky, Jared Kuolt and contributors
import os.path
from setuptools import setup, find_packages

setup(
    name='ascetic',
    version='0.7.2.24',

    packages = find_packages(),
    include_package_data=True,

    author="Ivan Zakrevsky",
    author_email="ivzak@yandex.ru",
    description="Lightweight python datamapper ORM (Object-relational mapper).",
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
    url="https://bitbucket.org/emacsway/ascetic",
    license="MIT License",
    keywords = "ORM Database SQL",
    install_requires=[
        'sqlbuilder >= 0.7.9.15'
    ],
    classifiers = [
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
