import unittest
from ascetic import validators
from ascetic.databases import databases
from ascetic.contrib.polymorphic import PolymorphicMapper
from ascetic.models import Model, ForeignKey, IdentityMap, mapper_registry

Author = Book = Nonfiction = Avia = None


class TestModelTranslation(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS ascetic_polymorphic_author CASCADE;
            CREATE TABLE ascetic_polymorphic_author (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_book CASCADE;
            CREATE TABLE ascetic_polymorphic_book (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id integer,
                polymorphic_type_id VARCHAR(255),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES ascetic_polymorphic_author (id, lang) ON DELETE CASCADE
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_nonfiction CASCADE;
            CREATE TABLE ascetic_polymorphic_nonfiction (
                nonfiction_ptr_id integer NOT NULL,
                nonfiction_ptr_lang VARCHAR(6) NOT NULL,
                branch VARCHAR(255),
                PRIMARY KEY (nonfiction_ptr_id, nonfiction_ptr_lang),
                FOREIGN KEY (nonfiction_ptr_id, nonfiction_ptr_lang) REFERENCES ascetic_polymorphic_book (id, lang) ON DELETE CASCADE
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_avia CASCADE;
            CREATE TABLE ascetic_polymorphic_avia (
                avia_ptr_id integer NOT NULL,
                avia_ptr_lang VARCHAR(6) NOT NULL,
                model VARCHAR(255),
                PRIMARY KEY (avia_ptr_id, avia_ptr_lang),
                FOREIGN KEY (avia_ptr_id, avia_ptr_lang) REFERENCES ascetic_polymorphic_nonfiction (nonfiction_ptr_id, nonfiction_ptr_lang) ON DELETE CASCADE
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS ascetic_polymorphic_author CASCADE;
            CREATE TABLE ascetic_polymorphic_author (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_book CASCADE;
            CREATE TABLE ascetic_polymorphic_book (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id INT(11),
                polymorphic_type_id VARCHAR(255),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES ascetic_polymorphic_author (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_nonfiction CASCADE;
            CREATE TABLE ascetic_polymorphic_nonfiction (
                nonfiction_ptr_id INT(11) NOT NULL,
                nonfiction_ptr_lang VARCHAR(6) NOT NULL,
                branch VARCHAR(255),
                PRIMARY KEY (nonfiction_ptr_id, nonfiction_ptr_lang),
                FOREIGN KEY (nonfiction_ptr_id, nonfiction_ptr_lang) REFERENCES ascetic_polymorphic_book (id, lang) ON DELETE CASCADE
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_avia CASCADE;
            CREATE TABLE ascetic_polymorphic_avia (
                avia_ptr_id INT(11) NOT NULL,
                avia_ptr_lang VARCHAR(6) NOT NULL,
                model VARCHAR(255),
                PRIMARY KEY (avia_ptr_id, avia_ptr_lang),
                FOREIGN KEY (avia_ptr_id, avia_ptr_lang) REFERENCES ascetic_polymorphic_nonfiction (nonfiction_ptr_id, nonfiction_ptr_lang) ON DELETE CASCADE
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS ascetic_polymorphic_author;
            CREATE TABLE ascetic_polymorphic_author (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_book;
            CREATE TABLE ascetic_polymorphic_book (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                author_id INT(11),
                polymorphic_type_id VARCHAR(255),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (author_id, lang) REFERENCES ascetic_polymorphic_author (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_nonfiction CASCADE;
            CREATE TABLE ascetic_polymorphic_nonfiction (
                nonfiction_ptr_id INTEGER NOT NULL,
                nonfiction_ptr_lang VARCHAR(6) NOT NULL,
                branch VARCHAR(255),
                PRIMARY KEY (nonfiction_ptr_id, nonfiction_ptr_lang),
                FOREIGN KEY (nonfiction_ptr_id, nonfiction_ptr_lang) REFERENCES ascetic_polymorphic_book (id, lang) ON DELETE CASCADE
            );
            DROP TABLE IF EXISTS ascetic_polymorphic_avia CASCADE;
            CREATE TABLE ascetic_polymorphic_avia (
                avia_ptr_id INTEGER NOT NULL,
                avia_ptr_lang VARCHAR(6) NOT NULL,
                model VARCHAR(255),
                PRIMARY KEY (avia_ptr_id, avia_ptr_lang),
                FOREIGN KEY (avia_ptr_id, avia_ptr_lang) REFERENCES ascetic_polymorphic_nonfiction (nonfiction_ptr_id, nonfiction_ptr_lang) ON DELETE CASCADE
            );
        """
    }

    @classmethod
    def create_models(cls):

        class Author(Model):

            class Mapper(object):
                db_table = 'ascetic_polymorphic_author'
                defaults = {'bio': 'No bio available'}
                validations = {'first_name': validators.Length(),
                               'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        class Book(Model):
            author = ForeignKey(
                Author,
                rel_field=('id', 'lang'),
                field=('author_id', 'lang'),
                rel_name='books',
                rel_query=(lambda rel: mapper_registry[rel.rel_model].query)
            )

            class Mapper(PolymorphicMapper):
                name = 'ascetic.contrib.tests.test_polymorphic.Book'
                db_table = 'ascetic_polymorphic_book'
                polymorphic = True

        class Nonfiction(Book):

            class Mapper(PolymorphicMapper):
                name = 'ascetic.contrib.tests.test_polymorphic.Nonfiction'
                db_table = 'ascetic_polymorphic_nonfiction'
                polymorphic = True

        class Avia(Nonfiction):

            class Mapper(PolymorphicMapper):
                name = 'ascetic.contrib.tests.test_polymorphic.Avia'
                db_table = 'ascetic_polymorphic_avia'
                polymorphic = True

        return locals()

    @classmethod
    def setUpClass(cls):
        db = databases['default']
        db.cursor().execute(cls.create_sql[db.engine])
        for model_name, model in cls.create_models().items():
            globals()[model_name] = model

    def setUp(self):
        IdentityMap().disable()
        db = databases['default']
        for table in ('ascetic_polymorphic_author', 'ascetic_polymorphic_book',):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_book(self):
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

    def test_nonfiction(self):
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

        nonfiction_data = dict(
            id=5,
            lang='en',
            title='Book title',
            branch='instruction',
        )
        nonfiction = Nonfiction(**nonfiction_data)
        nonfiction.author = author
        nonfiction.save()
        nonfiction_pk = (5, 'en')

        nonfiction = Book.get(nonfiction_pk)
        self.assertIsInstance(nonfiction, Nonfiction)
        for k, v in nonfiction_data.items():
            self.assertEqual(getattr(nonfiction, k), v)
        self.assertEqual(nonfiction.pk, nonfiction_pk)
        self.assertEqual(nonfiction.author.pk, author_pk)
        self.assertEqual(author.books[0], nonfiction)

        nonfiction = Nonfiction.get(nonfiction_pk)
        self.assertIsInstance(nonfiction, Nonfiction)
        for k, v in nonfiction_data.items():
            self.assertEqual(getattr(nonfiction, k), v)
        self.assertEqual(nonfiction.pk, nonfiction_pk)
        self.assertEqual(nonfiction.author.pk, author_pk)
        self.assertEqual(author.books[0], nonfiction)

    def test_avia(self):
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

        avia_data = dict(
            id=5,
            lang='en',
            title='Book title',
            branch='instruction',
            model='An'
        )
        avia = Avia(**avia_data)
        avia.author = author
        avia.save()
        avia_pk = (5, 'en')

        avia = Book.get(avia_pk)
        self.assertIsInstance(avia, Nonfiction)
        for k, v in avia_data.items():
            self.assertEqual(getattr(avia, k), v)
        self.assertEqual(avia.pk, avia_pk)
        self.assertEqual(avia.author.pk, author_pk)
        self.assertEqual(author.books[0], avia)

        avia = Avia.get(avia_pk)
        self.assertIsInstance(avia, Nonfiction)
        for k, v in avia_data.items():
            self.assertEqual(getattr(avia, k), v)
        self.assertEqual(avia.pk, avia_pk)
        self.assertEqual(avia.author.pk, author_pk)
        self.assertEqual(author.books[0], avia)
