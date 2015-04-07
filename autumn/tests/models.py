from __future__ import absolute_import
from autumn.models import Model, ForeignKey, OneToMany
from autumn import validators


class Author(Model):
    # books = OneToMany('autumn.tests.models.Book')

    class Meta:
        defaults = {'bio': 'No bio available'}
        validations = {'first_name': validators.Length(),
                       'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}


class Book(Model):
    author = ForeignKey(Author, rel_name='books')

    class Meta:
        db_table = 'books'
