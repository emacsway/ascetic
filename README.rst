============================================
Ascetic, a lightweight Python datamapper ORM
============================================

Ascetic exists as a super-lightweight datamapper ORM (Object-Relational Mapper) for Python.

* Home Page: https://bitbucket.org/emacsway/ascetic
* Docs: http://ascetic.readthedocs.org/
* Browse https://bitbucket.org/emacsway/ascetic/src
* Get source code: ``git clone https://bitbucket.org/emacsway/ascetic.git``
* PyPI: https://pypi.python.org/pypi/ascetic
* Github mirror: https://github.com/emacsway/ascetic

Ascetic based on "`Data Mapper <http://martinfowler.com/eaaCatalog/dataMapper.html>`_" and "`Table Data Gateway <http://martinfowler.com/eaaCatalog/tableDataGateway.html>`_".
It also supports "`Active Record <http://www.martinfowler.com/eaaCatalog/activeRecord.html>`_" wrapper, but it's just a wrapper, - model class is free from service logic.
Ascetic ORM follows the `KISS principle <http://en.wikipedia.org/wiki/KISS_principle>`_.
Has automatic population of fields from database (see the example below), and minimal size.
You do not have to specify the columns in the class. This follows the `DRY <http://en.wikipedia.org/wiki/DRY_code>`_ principle. 
Ascetic as small as possible.

In ascetic.contrib (currently under development) you can found solutions for:

- multilingual
- polymorphic relations
- polymorphic models (supports for "`Single Table Inheritance <http://martinfowler.com/eaaCatalog/singleTableInheritance.html>`_", "`Concrete Table Inheritance <http://martinfowler.com/eaaCatalog/concreteTableInheritance.html>`_" and "`Class Table Inheritance <http://martinfowler.com/eaaCatalog/classTableInheritance.html>`_" aka Django "`Multi-table inheritance <https://docs.djangoproject.com/en/1.8/topics/db/models/#multi-table-inheritance>`_")
- "Materialized Path" implementation of tree
- versioning (that stores only diff, not content copy)

All solutions support composite primary/foreign keys.

"`Identity Map <http://martinfowler.com/eaaCatalog/identityMap.html>`__" has SERIALIZABLE isolation level by default.

What Ascetic does not? Ascetic does not make any data type conversions (use connection features like `this <http://initd.org/psycopg/docs/advanced.html#adapting-new-python-types-to-sql-syntax>`__), and does not has "`Unit of Work <http://martinfowler.com/eaaCatalog/unitOfWork.html>`__". I recommend using a `Storm ORM <https://storm.canonical.com/>`__, if you need it all.

Ascetic is released under the MIT License (see LICENSE file for details).

This project is currently under development, and not stable. If you are looking for stable KISS-style ORM, pay attention to `Storm ORM <https://storm.canonical.com/>`__.


PostgreSQL Example
===================

Using these tables:

::

    CREATE TABLE ascetic_tests_models_author (
        id serial NOT NULL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        bio TEXT
    );
    CREATE TABLE books (
        id serial NOT NULL PRIMARY KEY,
        title VARCHAR(255),
        author_id integer REFERENCES ascetic_tests_models_author(id) ON DELETE CASCADE
    );

You can configure in one the following ways:

\1. Put in your PYTHONPATH file ascetic_settings.py with your settings.
See file ascetic/settings.py for more details.

\2. Define settings module in evironment variable ASCETIC_SETTINGS.

\3. Call ascetic.settings.configure(), for example::

    import ascetic.settings.configure
    ascetic.settings.configure({
        'DATABASES': {
            'default': {
                'engine': "postgresql",
                'user': "devel",
                'database': "devel_ascetic",
                'password': "devel",
                'debug': True,
                'initial_sql': "SET NAMES 'UTF8';",
            }
        }
    })
    
We setup our objects like so:

::

    from ascetic.model import Model, ForeignKey, OneToMany, get_mapper

    class Author(Model):

        class Mapper(object):
            defaults = {'bio': 'No bio available'}
            validations = {'first_name': (
                lambda v: len(v) > 1 or "Too short first name",
                lambda self, key, value: value != self.last_name or "Please, enter another first name",
            )}

    class Book(Model):
        author = ForeignKey(Author, related_name='books')

        class Mapper(object):
            db_table = 'books'

Now we can create, retrieve, update and delete entries in our database.
Creation

::

    james = Author(first_name='James', last_name='Joyce')
    get_mapper(Author).save(james)  # Datamapper way

    u = Book(title='Ulysses', author_id=james.id)
    u.save()  # Use ActiveRecord wrapper


Retrieval
==========

::

    a = Author.get(1)
    a.first_name # James
    a.books      # Returns list of author's books

    # Returns a list, using LIMIT based on slice
    a = Author.q[:10]   # LIMIT 0, 10
    a = Author.q[20:30] # LIMIT 20, 10


Updating
=========

::

    a = Author.get(1)
    a.bio = 'What a crazy guy! Hard to read but... wow!'
    a.save()


Deleting
=========

::

    a.delete()


`SQLBuilder <https://bitbucket.org/emacsway/sqlbuilder/overview>`_ integration
===============================================================================

::

    object_list = Book.q.tables(
        (Book.s & Author.s).on(Book.s.author_id == Author.s.id)
    ).where(
        (Author.s.first_name != 'James') & (Author.s.last_name != 'Joyce')
    )[:10]

Query object based on `sqlbuilder.smartsql <https://bitbucket.org/emacsway/sqlbuilder/src/tip/sqlbuilder/smartsql>`_, see `more info <https://bitbucket.org/emacsway/sqlbuilder/overview>`_.


Signals support
================

* pre_init
* post_init
* pre_save
* post_save
* pre_delete
* post_delete
* class_prepared


More info
=========

See more info in docs: http://ascetic.readthedocs.org/


Web
====

You can use Ascetic ORM with lightweight web-frameworks, like `wheezy.web <https://bitbucket.org/akorn/wheezy.web>`_, `Bottle <http://bottlepy.org/>`_, `Tornado <http://www.tornadoweb.org/>`_, `pysi <https://bitbucket.org/imbolc/pysi>`_, etc.


Gratitude
==========

| Forked from `https://github.com/lucky/autumn <https://github.com/lucky/autumn>`_
| Thanks to `Jared Kuolt (lucky) <https://github.com/lucky>`_


Other projects
===============

See also:

* `Storm <https://storm.canonical.com/>`_ (properties from class) - excellent and simple ORM!
* Article (in Russian) "`Why did I choose the Storm ORM <http://emacsway.bitbucket.org/ru/storm-orm/>`_"
* `SQLAlchemy <http://www.sqlalchemy.org/>`_ (scheme from class or database, see "`autoload <http://docs.sqlalchemy.org/en/rel_1_1/core/reflection.html>`__" option)
* `Openorm <http://code.google.com/p/openorm/source/browse/python/>`_ (lightweight datamapper), `miror <https://bitbucket.org/emacsway/openorm/src/default/python/>`__
* `SQLObject <http://www.sqlobject.org/>`_ (scheme from class or database, see "fromDatabase" option)
* `Peewee <http://peewee.readthedocs.org/>`_ (scheme from class)
* `Bazaar ORM <http://www.nongnu.org/bazaar/>`_
* `Twistar <http://findingscience.com/twistar/>`_ (scheme from database), provides asynchronous DB interaction
* `Activemodel <http://code.google.com/p/activemodel/>`_ (scheme from database)
* `ActiveRecord <http://code.activestate.com/recipes/496905-an-activerecord-like-orm-object-relation-mapper-un/>`_ like ORM under 200 lines (scheme from database)
* `simpleql <https://bitbucket.org/robertodealmeida/simpleql/>`_ SQL table using nothing but Python to build the query
* `Generator expressions <http://code.activestate.com/recipes/442447/>`__ for database requests (Python recipe)
* `Object Relational Mappers (ORMs) <https://wiki.python.org/moin/HigherLevelDatabaseProgramming>`_
