import unittest
from ascetic import validators
from ascetic.databases import databases
from ascetic.contrib.polymorphic import PolymorphicMapper
from ascetic.models import Mapper, ForeignKey, IdentityMap, mapper_registry, ObjectDoesNotExist

Author = Book = Nonfiction = Avia = None


class TestPolymorphic(unittest.TestCase):

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

        class Author(object):
            def __init__(self, id=None, lang=None, first_name=None, last_name=None, bio=None):
                self.id = id
                self.lang = lang
                self.first_name = first_name
                self.last_name = last_name
                self.bio = bio

        class AuthorMapper(Mapper):
            db_table = 'ascetic_polymorphic_author'
            defaults = {'bio': 'No bio available'}
            validations = {'first_name': validators.Length(),
                           'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        AuthorMapper(Author)

        class Book(object):
            def __init__(self, id=None, lang=None, polymorphic_type_id=None, title=None, author_id=None):
                self.id = id
                self.lang = lang
                self.polymorphic_type_id = polymorphic_type_id
                self.title = title
                self.author_id = author_id

        class BookMapper(PolymorphicMapper, Mapper):
            name = 'ascetic.contrib.tests.test_polymorphic.Book'
            db_table = 'ascetic_polymorphic_book'
            polymorphic = True

            relationships = {
                'author': ForeignKey(
                    Author,
                    rel_field=('id', 'lang'),
                    field=('author_id', 'lang'),
                    rel_name='books',
                    rel_query=(lambda rel: mapper_registry[rel.rel_model].query)
                )
            }

        BookMapper(Book)

        class Nonfiction(Book):
            def __init__(self, nonfiction_ptr_id=None, nonfiction_ptr_lang=None, branch=None, **kwargs):
                super(Nonfiction, self).__init__(**kwargs)
                self.nonfiction_ptr_id = nonfiction_ptr_id
                self.nonfiction_ptr_lang = nonfiction_ptr_lang
                self.branch = branch

        class NonfictionMapper(PolymorphicMapper, Mapper):
            name = 'ascetic.contrib.tests.test_polymorphic.Nonfiction'
            db_table = 'ascetic_polymorphic_nonfiction'
            polymorphic = True

        NonfictionMapper(Nonfiction)

        class Avia(Nonfiction):
            def __init__(self, avia_ptr_id=None, avia_ptr_lang=None, model=None, **kwargs):
                super(Avia, self).__init__(**kwargs)
                self.avia_ptr_id = avia_ptr_id
                self.avia_ptr_lang = avia_ptr_lang
                self.model = model

        class AviaMapper(PolymorphicMapper, Mapper):
            name = 'ascetic.contrib.tests.test_polymorphic.Avia'
            db_table = 'ascetic_polymorphic_avia'
            polymorphic = True

        AviaMapper(Avia)

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
        author_mapper = mapper_registry[Author]
        book_mapper = mapper_registry[Book]
        author = Author(
            id=1,
            lang='en',
            first_name='First name',
            last_name='Last name',
        )
        author_mapper.save(author)
        author_pk = (1, 'en')
        author = author_mapper.get(author_pk)
        self.assertEqual(author_mapper.get_pk(author), author_pk)

        book = Book(
            id=5,
            lang='en',
            title='Book title'
        )
        book.author = author
        book_mapper.save(book)
        book_pk = (5, 'en')
        book = book_mapper.get(book_pk)
        self.assertEqual(book_mapper.get_pk(book), book_pk)
        self.assertEqual(author_mapper.get_pk(book.author), author_pk)

        author = author_mapper.get(author_pk)
        self.assertEqual(book_mapper.get_pk(author.books[0]), book_pk)
        self.assertIsInstance(author.books[0], Book)

        book_mapper.delete(book)
        self.assertRaises(ObjectDoesNotExist, book_mapper.get, book_pk)

    def test_nonfiction(self):
        author_mapper = mapper_registry[Author]
        nonfiction_mapper = mapper_registry[Nonfiction]
        book_mapper = mapper_registry[Book]

        author = Author(
            id=1,
            lang='en',
            first_name='First name',
            last_name='Last name',
        )
        author_mapper.save(author)
        author_pk = (1, 'en')
        author = author_mapper.get(author_pk)
        self.assertEqual(author_mapper.get_pk(author), author_pk)

        nonfiction_data = dict(
            id=5,
            lang='en',
            title='Book title',
            branch='instruction',
        )
        nonfiction = Nonfiction(**nonfiction_data)
        nonfiction.author = author
        nonfiction_mapper.save(nonfiction)
        nonfiction_pk = (5, 'en')

        nonfiction = book_mapper.get(nonfiction_pk)
        self.assertIsInstance(nonfiction, Nonfiction)
        for k, v in nonfiction_data.items():
            self.assertEqual(getattr(nonfiction, k), v)
        self.assertEqual(nonfiction_mapper.get_pk(nonfiction), nonfiction_pk)
        self.assertEqual(author_mapper.get_pk(nonfiction.author), author_pk)
        self.assertEqual(book_mapper.get_pk(author.books[0]), nonfiction_mapper.get_pk(nonfiction))
        self.assertIsInstance(author.books[0], Nonfiction)

        nonfiction = nonfiction_mapper.get(nonfiction_pk)
        self.assertIsInstance(nonfiction, Nonfiction)
        for k, v in nonfiction_data.items():
            self.assertEqual(getattr(nonfiction, k), v)
        self.assertEqual(nonfiction_mapper.get_pk(nonfiction), nonfiction_pk)
        self.assertEqual(author_mapper.get_pk(nonfiction.author), author_pk)
        self.assertEqual(book_mapper.get_pk(author.books[0]), nonfiction_mapper.get_pk(nonfiction))
        self.assertIsInstance(author.books[0], Nonfiction)

        nonfiction_mapper.delete(nonfiction)
        self.assertRaises(ObjectDoesNotExist, book_mapper.get, nonfiction_pk)

    def test_avia(self):
        author_mapper = mapper_registry[Author]
        avia_mapper = mapper_registry[Avia]
        book_mapper = mapper_registry[Book]

        author = Author(
            id=1,
            lang='en',
            first_name='First name',
            last_name='Last name',
        )
        author_mapper.save(author)
        author_pk = (1, 'en')
        author = author_mapper.get(author_pk)
        self.assertEqual(author_mapper.get_pk(author), author_pk)

        avia_data = dict(
            id=5,
            lang='en',
            title='Book title',
            branch='instruction',
            model='An'
        )
        avia = Avia(**avia_data)
        avia.author = author
        avia_mapper.save(avia)
        avia_pk = (5, 'en')

        book = book_mapper.query.where(book_mapper.sql_table.pk == avia_pk).polymorphic(False)[0]
        self.assertEqual(book.__class__, Book)
        avia = book.concrete_instance
        self.assertIsInstance(avia, Avia)

        avia = book_mapper.get(avia_pk)
        self.assertIsInstance(avia, Avia)
        for k, v in avia_data.items():
            self.assertEqual(getattr(avia, k), v)
        self.assertEqual(avia_mapper.get_pk(avia), avia_pk)
        self.assertEqual(author_mapper.get_pk(avia.author), author_pk)
        self.assertEqual(book_mapper.get_pk(author.books[0]), avia_mapper.get_pk(avia))
        self.assertIsInstance(author.books[0], Avia)

        avia = avia_mapper.get(avia_pk)
        self.assertIsInstance(avia, Avia)
        for k, v in avia_data.items():
            self.assertEqual(getattr(avia, k), v)
        self.assertEqual(avia_mapper.get_pk(avia), avia_pk)
        self.assertEqual(author_mapper.get_pk(avia.author), author_pk)
        self.assertEqual(book_mapper.get_pk(author.books[0]), avia_mapper.get_pk(avia))
        self.assertIsInstance(author.books[0], Avia)

        avia_mapper.delete(avia)
        self.assertRaises(ObjectDoesNotExist, book_mapper.get, avia_pk)
