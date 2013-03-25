=================================
Autumn, a lightweight Python ORM
=================================

Autumn exists as a super-lightweight Object-relational mapper (ORM) for Python.
Autumn ORM follows the `KISS principle <http://en.wikipedia.org/wiki/KISS_principle>`_.
It’s an alternative to `SQLObject <http://www.sqlobject.org/>`_,
`SQLAlchemy <http://www.sqlalchemy.org/>`_, `Storm <https://storm.canonical.com/>`_,
etc.
Perhaps the biggest difference is the automatic population of fields as
attributes (see the example below), and minimal size.
Autumn as small as possible.

It is released under the MIT License (see LICENSE file for details).

This project is currently considered beta software.

PostgreSQL Example
===================

Using these tables:

::

    CREATE TABLE autumn_tests_models_author (
        id serial NOT NULL PRIMARY KEY,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        bio TEXT
    );
    CREATE TABLE books (
        id serial NOT NULL PRIMARY KEY,
        title VARCHAR(255),
        author_id integer REFERENCES autumn_tests_models_author(id) ON DELETE CASCADE
    );

Put in your PYTHONPATH file autumn_settings.py with your settings.
See file autumn/settings.py for more details.

We setup our objects like so:

::

    from autumn.model import Model
    from autumn.db.relations import ForeignKey, OneToMany
    import datetime

    class Author(Model):
        books = OneToMany('Book')

        class Meta:
            defaults = {'bio': 'No bio available'}
            validations = {'first_name': (
                lambda v: len(v) > 1 or "Too short first name",
                lambda self, key, value: value != self.last_name or "Please, enter another first name",
            )}

    class Book(Model):
        author = ForeignKey(Author)

        class Meta:
            db_table = 'books'

Now we can create, retrieve, update and delete entries in our database.
Creation

::

    james = Author(first_name='James', last_name='Joyce')
    james.save()

    u = Book(title='Ulysses', author_id=james.id)
    u.save()

Retrieval
==========

::

    a = Author.get(1)
    a.first_name # James
    a.books      # Returns list of author's books

    # Returns a list, using LIMIT based on slice
    a = Author.get()[:10]   # LIMIT 0, 10
    a = Author.get()[20:30] # LIMIT 20, 10

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

`SQLBuilder <https://bitbucket.org/evotech/sqlbuilder/overview>`_ integration
=====================================================================

::

    ta = Author.ss
    tb = Book.ss
    qs = Book.qs
    object_list = qs.tables(
        qs.tables() & ta.on(tb.author_id == ta.id)
    ).where(
        (ta.first_name != 'James') & (ta.last_name != 'Joyce')
    )[:10]

QuerySet based on sqlbuilder.smartsql, more info `https://bitbucket.org/evotech/sqlbuilder <https://bitbucket.org/evotech/sqlbuilder>`_

Signals support
================

* pre_init
* post_init
* pre_save
* post_save
* pre_delete
* post_delete
* class_prepared

Gratitude
==========

| Forked from `https://github.com/lucky/autumn <https://github.com/lucky/autumn>`_
| Thanks to `Jared Kuolt (lucky) <https://github.com/lucky>`_
