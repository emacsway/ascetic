# Autumn, a Python ORM

Autumn exists as a super-lightweight Object-relational mapper (ORM) for Python. 
It’s an alternative to [SQLObject](http://www.sqlobject.org/), 
[SQLAlchemy](http://www.sqlalchemy.org/), [Storm](https://storm.canonical.com/),
etc. Perhaps the biggest difference is the automatic population of fields as 
attributes (see the example below).

It is released under the MIT License (see LICENSE file for details).

This project is currently considered beta software.

## MySQL Example

Using these tables:

    DROP TABLE IF EXISTS author;
    CREATE TABLE author (
        id INT(11) NOT NULL auto_increment,
        first_name VARCHAR(40) NOT NULL,
        last_name VARCHAR(40) NOT NULL,
        bio TEXT,
        PRIMARY KEY (id)
    );
    DROP TABLE IF EXISTS books;
    CREATE TABLE books (
        id INT(11) NOT NULL auto_increment,
        title VARCHAR(255),
        author_id INT(11),
        FOREIGN KEY (author_id) REFERENCES author(id),
        PRIMARY KEY (id)
    );

Put in your PYTHONPATH file autumn_settings.py with your settings.
See file autumn/settings.py for more details.

We setup our objects like so:

    from autumn.model import Model
    from autumn.db.relations import ForeignKey, OneToMany
    import datetime

    class Author(Model):
        books = OneToMany('Book')

        class Meta:
            defaults = {'bio': 'No bio available'}
            validations = {'first_name': lambda self, v: len(v) > 1}

    class Book(Model):
        author = ForeignKey(Author)

        class Meta:
            table = 'books'

Now we can create, retrieve, update and delete entries in our database.
Creation

    james = Author(first_name='James', last_name='Joyce')
    james.save()

    u = Book(title='Ulysses', author_id=james.id)
    u.save()

### Retrieval

    a = Author.get(1)
    a.first_name # James
    a.books      # Returns list of author's books

    # Returns a list, using LIMIT based on slice
    a = Author.get()[:10]   # LIMIT 0, 10
    a = Author.get()[20:30] # LIMIT 20, 10

### Updating

    a = Author.get(1)
    a.bio = 'What a crazy guy! Hard to read but... wow!'
    a.save()

### Deleting

    a.delete()

### [SQLBuilder](https://bitbucket.org/evotech/sqlbuilder) integration

    ta = Author.ss
    tb = Book.ss
    qs = tb.qs
    object_list = qs.tables(
        qs.tables() & ta.on(tb.author_id == ta.id)
    ).where(
        (ta.first_name != 'James') & (ta.last_name != 'Joyce')
    )[:10]

### Signals support

* pre_init
* post_init
* pre_save
* post_save
* pre_delete
* post_delete
* class_prepared

###  Gratitude

Forked from [https://github.com/lucky/autumn](https://github.com/lucky/autumn)  
Thanks to [Jared Kuolt (lucky)](https://github.com/lucky)
