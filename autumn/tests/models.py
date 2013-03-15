from __future__ import absolute_import, unicode_literals
from autumn.models import Model
from autumn.relations import ForeignKey, OneToMany
from autumn import validators


class Author(Model):
    books = OneToMany('autumn.tests.models.Book')

    class Meta:
        defaults = {'bio': 'No bio available'}
        validations = {'first_name': validators.Length(),
                       'last_name': (validators.Length(), lambda x: x != 'BadGuy!')}


class Book(Model):
    author = ForeignKey(Author)

    class Meta:
        db_table = 'books'
