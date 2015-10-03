#!/usr/bin/env python
import unittest
from ascetic import validators
from ascetic import utils
from ascetic.databases import databases
from ascetic.models import Model, Mapper, ForeignKey, IdentityMap, mapper_registry
from sqlbuilder import smartsql
from sqlbuilder.smartsql.tests import *

Author = Book = AuthorC = BookC = None


class TestValidators(unittest.TestCase):

    maxDiff = None

    def test_validators(self):
        ev = validators.Email()
        assert ev('test@example.com')
        assert not ev('adsf@.asdf.asdf')
        assert validators.Length()('a')
        assert not validators.Length(2)('a')
        assert validators.Length(max_length=10)('abcdegf')
        assert not validators.Length(max_length=3)('abcdegf')

        n = validators.Number(1, 5)
        assert n(2)
        assert not n(6)
        assert validators.Number(1)(100.0)
        assert not validators.Number()('rawr!')

        vc = validators.ValidatorChain(validators.Length(8), validators.Email())
        assert vc('test@example.com')
        assert not vc('a@a.com')
        assert not vc('asdfasdfasdfasdfasdf')


class TestUtils(unittest.TestCase):

    maxDiff = None

    def test_resolve(self):
        from ascetic.databases import Database
        self.assertTrue(utils.resolve('ascetic.databases.Database') is Database)


class TestModels(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS ascetic_tests_author CASCADE;
            CREATE TABLE ascetic_tests_author (
                id serial NOT NULL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT
            );
            DROP TABLE IF EXISTS books CASCADE;
            CREATE TABLE books (
                id serial NOT NULL PRIMARY KEY,
                title VARCHAR(255),
                author_id integer REFERENCES ascetic_tests_author(id) ON DELETE CASCADE
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS ascetic_tests_author CASCADE;
            CREATE TABLE ascetic_tests_author (
                id INT(11) NOT NULL auto_increment,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id)
            );
            DROP TABLE IF EXISTS books CASCADE;
            CREATE TABLE books (
                id INT(11) NOT NULL auto_increment,
                title VARCHAR(255),
                author_id INT(11),
                FOREIGN KEY (author_id) REFERENCES ascetic_tests_author(id),
                PRIMARY KEY (id)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS ascetic_tests_author;
            CREATE TABLE ascetic_tests_author (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              first_name VARCHAR(40) NOT NULL,
              last_name VARCHAR(40) NOT NULL,
              bio TEXT
            );
            DROP TABLE IF EXISTS books;
            CREATE TABLE books (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title VARCHAR(255),
              author_id INT(11),
              FOREIGN KEY (author_id) REFERENCES ascetic_tests_author(id)
            );
        """
    }

    @classmethod
    def create_models(cls):
        class Author(object):
            # books = OneToMany('ascetic.tests.models.Book')

            def __init__(self, id=None, first_name=None, last_name=None, bio=None):
                self.id = id
                self.first_name = first_name
                self.last_name = last_name
                self.bio = bio

        class AuthorMapper(Mapper):
            db_table = 'ascetic_tests_author'
            defaults = {'bio': 'No bio available'}
            validations = {'first_name': validators.Length(),
                           'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        AuthorMapper(Author)

        class Book(object):

            def __init__(self, id=None, title=None, author_id=None):
                self.id = id
                self.title = title
                self.author_id = author_id

        class BookMapper(Mapper):
            db_table = 'books'
            relationships = {
                'author': ForeignKey(Author, rel_name='books')
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
        for table in ('ascetic_tests_author', 'books'):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
        # Create tables
        author_mapper = mapper_registry[Author]
        book_mapper = mapper_registry[Book]

        # Test Creation
        james = Author(first_name='James', last_name='Joyce')
        author_mapper.save(james)
        kurt = Author(first_name='Kurt', last_name='Vonnegut')
        author_mapper.save(kurt)
        tom = Author(first_name='Tom', last_name='Robbins')
        author_mapper.save(tom)

        book_mapper.save(Book(title='Ulysses', author_id=james.id))
        book_mapper.save(Book(title='Slaughter-House Five', author_id=kurt.id))
        book_mapper.save(Book(title='Jitterbug Perfume', author_id=tom.id))
        slww = Book(title='Still Life with Woodpecker', author_id=tom.id)
        book_mapper.save(slww)

        # Test Model.pk getter and setter
        pk = book_mapper.get_pk(slww)
        self.assertEqual(book_mapper.get_pk(slww), slww.id)
        book_mapper.set_pk(slww, tom.id)
        self.assertEqual(book_mapper.get_pk(slww), tom.id)
        book_mapper.set_pk(slww, pk)
        self.assertEqual(book_mapper.get_pk(slww), pk)

        # self.assertTrue(kurt == author_mapper.get(kurt.id))
        # self.assertTrue(kurt != tom)

        # Test ForeignKey
        self.assertEqual(slww.author.first_name, 'Tom')
        slww.author = kurt
        self.assertEqual(slww.author.first_name, 'Kurt')
        del slww.author
        self.assertEqual(slww.author, None)
        slww.author = None
        self.assertEqual(slww.author, None)
        slww.author = tom.id
        self.assertEqual(slww.author.first_name, 'Tom')

        # Test OneToMany
        self.assertEqual(len(list(tom.books)), 2)

        kid = kurt.id
        del(james, kurt, tom, slww)

        # Test retrieval
        b = book_mapper.get(title='Ulysses')

        a = author_mapper.get(id=b.author_id)
        self.assertEqual(a.id, b.author_id)

        a = author_mapper.query.where(author_mapper.sql_table.id == b.id)[:]
        # self.assert_(isinstance(a, list))
        self.assert_(isinstance(a, smartsql.Q))

        # Test update
        new_last_name = 'Vonnegut, Jr.'
        a = author_mapper.get(id=kid)
        a.last_name = new_last_name
        author_mapper.save(a)

        a = author_mapper.get(kid)
        self.assertEqual(a.last_name, new_last_name)

        # Test count
        self.assertEqual(author_mapper.query.count(), 3)
        self.assertEqual(len(book_mapper.query.clone()), 4)
        self.assertEqual(len(book_mapper.query.clone()[1:4]), 3)

        # Test delete
        author_mapper.delete(a)
        self.assertEqual(author_mapper.query.count(), 2)
        self.assertEqual(len(book_mapper.query.clone()), 3)

        # Test validation
        a = Author(first_name='', last_name='Ted')
        self.assertRaises(validators.ValidationError, author_mapper.validate, a)

        # Test defaults
        a.first_name = 'Bill and'
        author_mapper.save(a)
        self.assertEqual(a.bio, 'No bio available')

        a = Author(first_name='I am a', last_name='BadGuy!')
        self.assertRaises(validators.ValidationError, author_mapper.validate, a)

        print '### Testing for smartsql integration'
        fields = [author_mapper.query.db.compile(i)[0] for i in author_mapper.get_sql_fields()]
        if author_mapper.query.db.engine == 'postgresql':
            self.assertListEqual(
                fields,
                ['"ascetic_tests_author"."id"',
                 '"ascetic_tests_author"."first_name"',
                 '"ascetic_tests_author"."last_name"',
                 '"ascetic_tests_author"."bio"', ]
            )
        else:
            self.assertListEqual(
                fields,
                ['`ascetic_tests_author`.`id`',
                 '`ascetic_tests_author`.`first_name`',
                 '`ascetic_tests_author`.`last_name`',
                 '`ascetic_tests_author`.`bio`', ]
            )

        if author_mapper.query.db.engine == 'postgresql':
            self.assertEqual(book_mapper.sql_table.author, '"book"."author_id"')
        else:
            self.assertEqual(book_mapper.sql_table.author, '`book`.`author_id`')

        q = author_mapper.query
        if q.db.engine == 'postgresql':
            self.assertEqual(q.db.compile(q)[0], '''SELECT "ascetic_tests_author"."id", "ascetic_tests_author"."first_name", "ascetic_tests_author"."last_name", "ascetic_tests_author"."bio" FROM "ascetic_tests_author"''')
        else:
            self.assertEqual(q.db.compile(q)[0], """SELECT `ascetic_tests_author`.`id`, `ascetic_tests_author`.`first_name`, `ascetic_tests_author`.`last_name`, `ascetic_tests_author`.`bio` FROM `ascetic_tests_author`""")
        self.assertEqual(len(q), 3)
        for obj in q:
            self.assertTrue(isinstance(obj, Author))

        q = q.where(author_mapper.sql_table.id == b.author_id)
        if q.db.engine == 'postgresql':
            self.assertEqual(q.db.compile(q)[0], """SELECT "ascetic_tests_author"."id", "ascetic_tests_author"."first_name", "ascetic_tests_author"."last_name", "ascetic_tests_author"."bio" FROM "ascetic_tests_author" WHERE "ascetic_tests_author"."id" = %s""")
        else:
            self.assertEqual(q.db.compile(q)[0], """SELECT `ascetic_tests_author`.`id`, `ascetic_tests_author`.`first_name`, `ascetic_tests_author`.`last_name`, `ascetic_tests_author`.`bio` FROM `ascetic_tests_author` WHERE `ascetic_tests_author`.`id` = %s""")
        self.assertEqual(len(q), 1)
        self.assertTrue(isinstance(q[0], Author))

        # prefetch
        q = book_mapper.query.prefetch('author').order_by(book_mapper.sql_table.id)
        for obj in q:
            self.assertTrue(hasattr(obj, '_cache'))
            self.assertTrue('author' in obj._cache)
            self.assertEqual(obj._cache['author'], obj.author)

        for obj in author_mapper.query.prefetch('books').order_by(author_mapper.sql_table.id):
            self.assertTrue(hasattr(obj, '_cache'))
            self.assertTrue('books' in obj._cache)
            self.assertEqual(len(obj._cache['books']._cache), len(obj.books))
            for i in obj._cache['books']._cache:
                self.assertEqual(i._cache['author'], obj)


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
        class AuthorC(Model):
            # books = OneToMany('ascetic.tests.models.Book')

            class Mapper(object):
                db_table = 'ascetic_composite_author'
                defaults = {'bio': 'No bio available'}
                validations = {'first_name': validators.Length(),
                               'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        class BookC(Model):
            author = ForeignKey(AuthorC, rel_field=('id', 'lang'), field=('author_id', 'lang'), rel_name='books')

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
        IdentityMap().disable()
        db = databases['default']
        for table in ('ascetic_composite_author', 'ascetic_composite_book'):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
        author = AuthorC(
            id=1,
            lang='en',
            first_name='First name',
            last_name='Last name',
        )
        self.assertIn('first_name', dir(author))
        self.assertIn('last_name', dir(author))
        author.save()
        author_pk = (1, 'en')
        author = AuthorC.get(author_pk)
        self.assertEqual(author.pk, author_pk)

        book = BookC(
            id=5,
            lang='en',
            title='Book title'
        )
        book.author = author
        book.save()
        book_pk = (5, 'en')
        book = BookC.get(book_pk)
        self.assertEqual(book.pk, book_pk)
        self.assertEqual(book.author.pk, author_pk)

        author = AuthorC.get(author_pk)
        self.assertEqual(author.books[0].pk, book_pk)
