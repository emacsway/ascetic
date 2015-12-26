import unittest
from ascetic import validators
from ascetic.databases import databases
from ascetic.contrib.gfk import GenericForeignKey, GenericRelation
from ascetic.models import Model, Mapper, IdentityMap, mapper_registry

Author = Book = None


class TestGenericForeignKey(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS ascetic_gfk_author CASCADE;
            CREATE TABLE ascetic_gfk_author (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_gfk_book CASCADE;
            CREATE TABLE ascetic_gfk_book (
                id integer NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                object_type_id VARCHAR(255),
                object_id integer,
                PRIMARY KEY (id, lang)
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS ascetic_gfk_author CASCADE;
            CREATE TABLE ascetic_gfk_author (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_gfk_book CASCADE;
            CREATE TABLE ascetic_gfk_book (
                id INT(11) NOT NULL,
                lang VARCHAR(6) NOT NULL,
                title VARCHAR(255),
                object_id INT(11),
                object_type_id VARCHAR(255),
                PRIMARY KEY (id, lang)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS ascetic_gfk_author;
            CREATE TABLE ascetic_gfk_author (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id, lang)
            );
            DROP TABLE IF EXISTS ascetic_gfk_book;
            CREATE TABLE ascetic_gfk_book (
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

        class Author(object):
            def __init__(self, id=None, lang=None, first_name=None, last_name=None, bio=None):
                self.id = id
                self.lang = lang
                self.first_name = first_name
                self.last_name = last_name
                self.bio = bio

        class AuthorMapper(Mapper):
            db_table = 'ascetic_gfk_author'
            defaults = {'bio': 'No bio available'}
            validations = {'first_name': validators.Length(),
                           'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}
            relationships = {
                'books': GenericRelation('ascetic.contrib.tests.test_gfk.Book', rel_name='author'),
            }

        AuthorMapper(Author)

        class Book(object):
            def __init__(self, id=None, lang=None, title=None, author_id=None, object_type_id=None, object_id=None):
                self.id = id
                self.lang = lang
                self.title = title
                self.author_id = author_id
                self.object_type_id = object_type_id
                self.object_id = object_id

        class BookMapper(Mapper):
            name = 'ascetic.contrib.tests.test_gfk.Book'
            db_table = 'ascetic_gfk_book'
            relationships = {
                'author': GenericForeignKey(rel_field=('id', 'lang'), field=('object_id', 'lang')),
            }

        BookMapper(Book)

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
        for table in ('ascetic_gfk_author', 'ascetic_gfk_book',):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
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
