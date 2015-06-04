import unittest
from autumn import validators
from autumn.connections import get_db
from autumn.contrib.polymorphic import PolymorphicGateway
from autumn.models import Model, ForeignKey

Author = Book = None


class TestModelTranslation(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS autumn_polymorphic_author CASCADE;
            CREATE TABLE autumn_polymorphic_author (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS autumn_polymorphic_book CASCADE;
            CREATE TABLE autumn_polymorphic_book (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id integer,
                polymorphic_type_id VARCHAR(255),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES autumn_polymorphic_author (id, lang) ON DELETE CASCADE
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS autumn_polymorphic_author CASCADE;
            CREATE TABLE autumn_polymorphic_author (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS autumn_polymorphic_book CASCADE;
            CREATE TABLE autumn_polymorphic_book (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id INT(11),
                polymorphic_type_id VARCHAR(255),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES autumn_polymorphic_author (id, lang)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS autumn_polymorphic_author;
            CREATE TABLE autumn_polymorphic_author (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS autumn_polymorphic_book;
            CREATE TABLE autumn_polymorphic_book (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id INT(11),
                polymorphic_type_id VARCHAR(255),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES autumn_polymorphic_author (id, lang)
            );
        """
    }

    @classmethod
    def create_models(cls):

        class Author(Model):

            class Gateway(object):
                db_table = 'autumn_polymorphic_author'
                defaults = {'bio': 'No bio available'}
                validations = {'first_name': validators.Length(),
                               'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        class Book(Model):
            author = ForeignKey(Author, rel_field=('id', 'lang'), field=('author_id', 'lang'), rel_name='books')

            class Gateway(PolymorphicGateway):
                name = 'autumn.contrib.tests.test_polymorphic.Book'
                db_table = 'autumn_polymorphic_book'

        return locals()

    @classmethod
    def setUpClass(cls):
        db = get_db()
        db.cursor().execute(cls.create_sql[db.engine])
        for model_name, model in cls.create_models().items():
            globals()[model_name] = model

    def setUp(self):
        db = get_db()
        for table in ('autumn_polymorphic_author', 'autumn_polymorphic_book',):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
        author = Author(
            id=1,
            lang='en',
            first_name='First name',
            last_name='Last name',
        )
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
