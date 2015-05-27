import unittest
from autumn import validators
from autumn.connections import get_db
from autumn.contrib.gfk import GenericForeignKey, GenericRelation
from autumn.models import Model

Author = Book = None


class TestModelTranslation(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS autumn_gfk_author CASCADE;
            CREATE TABLE autumn_gfk_author (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS autumn_gfk_book CASCADE;
            CREATE TABLE autumn_gfk_book (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                object_type_id VARCHAR(255),
                object_id integer,
                PRIMARY KEY (id, lang)
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS autumn_gfk_author CASCADE;
            CREATE TABLE autumn_gfk_author (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS autumn_gfk_book CASCADE;
            CREATE TABLE autumn_gfk_book (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                object_id INT(11),
                object_type_id VARCHAR(255),
                PRIMARY KEY (id, lang)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS autumn_gfk_author;
            CREATE TABLE autumn_gfk_author (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS autumn_gfk_book;
            CREATE TABLE autumn_gfk_book (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                object_type_id VARCHAR(255),
                object_id INT(11)
                PRIMARY KEY (id, lang)
            );
        """
    }

    @classmethod
    def create_models(cls):

        class Author(Model):
            books = GenericRelation('autumn.contrib.tests.test_gfk.Book', rel_name='author')

            class Gateway(object):
                db_table = 'autumn_gfk_author'
                defaults = {'bio': 'No bio available'}
                validations = {'first_name': validators.Length(),
                               'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        class Book(Model):
            author = GenericForeignKey(rel_field=('id', 'lang'), field=('object_id', 'lang'))

            class Gateway(object):
                name = 'autumn.contrib.tests.test_gfk.Book'
                db_table = 'autumn_gfk_book'

        return locals()

    @classmethod
    def setUpClass(cls):
        db = get_db()
        db.cursor().execute(cls.create_sql[db.engine])
        for model_name, model in cls.create_models().items():
            globals()[model_name] = model

    def setUp(self):
        db = get_db()
        for table in ('autumn_gfk_author', 'autumn_gfk_book',):
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
