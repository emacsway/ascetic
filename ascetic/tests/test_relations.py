import unittest

from ascetic import validators
from ascetic.databases import databases
from ascetic.models import Model
from ascetic.relations import ForeignKey

Author = Book = None


class TestCompositeRelation(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS ascetic_composite_author CASCADE;
            CREATE TABLE ascetic_composite_author (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_composite_book CASCADE;
            CREATE TABLE ascetic_composite_book (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id integer,
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES ascetic_composite_author (id, lang) ON DELETE CASCADE
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS ascetic_composite_author CASCADE;
            CREATE TABLE ascetic_composite_author (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_composite_book CASCADE;
            CREATE TABLE ascetic_composite_book (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id INT(11),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES ascetic_composite_author (id, lang)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS ascetic_composite_author;
            CREATE TABLE ascetic_composite_author (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_composite_book;
            CREATE TABLE ascetic_composite_book (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id INT(11),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES ascetic_composite_author (id, lang)
            );
        """
    }

    @classmethod
    def create_models(cls):

        class Author(Model):

            class Mapper(object):
                db_table = 'ascetic_composite_author'
                defaults = {'bio': 'No bio available'}
                validations = {'first_name': validators.Length(),
                               'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        class Book(Model):
            author = ForeignKey(Author, related_field=('id', 'lang'), field=('author_id', 'lang'), related_name='books')

            class Mapper(object):
                db_table = 'ascetic_composite_book'

        return locals()

    @classmethod
    def setUpClass(cls):
        db = databases['default']
        db.cursor().execute(cls.create_sql[db.engine])
        for model_name, model in cls.create_models().items():
            globals()[model_name] = model

    def setUp(self):
        db = databases['default']
        db.identity_map.disable()
        for table in ('ascetic_composite_author', 'ascetic_composite_book'):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
        author = Author(
            id=1,
            lang='en',
            first_name='First name',
            last_name='Last name',
        )
        self.assertIn('first_name', dir(author))
        self.assertIn('last_name', dir(author))
        author.save()
        author_pk = (1, 'en')
        author = Author.get(author_pk)
        self.assertEqual(author.pk, author_pk)

        book = Book(
            id=5,
            lang='en',
            title='Book title'
        )
        book.author = author
        book.save()
        book_pk = (5, 'en')
        book = Book.get(book_pk)
        self.assertEqual(book.pk, book_pk)
        self.assertEqual(book.author.pk, author_pk)

        author = Author.get(author_pk)
        self.assertEqual(author.books[0].pk, book_pk)
