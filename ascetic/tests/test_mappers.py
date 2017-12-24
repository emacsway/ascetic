#!/usr/bin/env python
import unittest

from sqlbuilder import smartsql

from ascetic import exceptions, validators
from ascetic.databases import databases
from ascetic.mappers import Mapper, mapper_registry
from ascetic.relations import ForeignKey

Author = Book = None


class TestMapper(unittest.TestCase):

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
                'author': ForeignKey(Author, related_name='books')
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
        db = databases['default']
        db.identity_map.disable()
        for table in ('ascetic_tests_author', 'books'):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

        author_mapper = mapper_registry[Author]
        book_mapper = mapper_registry[Book]

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
        self.data = {
            'james': james,
            'kurt': kurt,
            'tom': tom,
            'slww': slww,
        }

    def test_pk(self):
        book_mapper = mapper_registry[Book]
        tom, slww = self.data['tom'], self.data['slww']

        pk = book_mapper.get_pk(slww)
        self.assertEqual(book_mapper.get_pk(slww), slww.id)
        book_mapper.set_pk(slww, tom.id)
        self.assertEqual(book_mapper.get_pk(slww), tom.id)
        book_mapper.set_pk(slww, pk)
        self.assertEqual(book_mapper.get_pk(slww), pk)

        # self.assertTrue(kurt == author_mapper.get(kurt.id))
        # self.assertTrue(kurt != tom)

    def test_fk(self):
        kurt, tom, slww = self.data['kurt'], self.data['tom'], self.data['slww']

        self.assertEqual(slww.author.first_name, 'Tom')
        slww.author = kurt
        self.assertEqual(slww.author.first_name, 'Kurt')
        del slww.author
        self.assertEqual(slww.author, None)
        slww.author = None
        self.assertEqual(slww.author, None)
        slww.author = tom.id
        self.assertEqual(slww.author.first_name, 'Tom')

    def test_o2m(self):
        tom = self.data['tom']
        self.assertEqual(len(list(tom.books)), 2)

    def test_retrieval(self):
        author_mapper, book_mapper = mapper_registry[Author], mapper_registry[Book]
        tom = self.data['tom']

        # Test retrieval
        b = book_mapper.get(title='Ulysses')

        a = author_mapper.get(id=b.author_id)
        self.assertEqual(a.id, b.author_id)

        a = author_mapper.query.where(author_mapper.sql_table.id == b.id)[:]
        # self.assert_(isinstance(a, list))
        self.assert_(isinstance(a, smartsql.Q))
        self.assertEqual(len(list(tom.books)), 2)

    def test_update(self):
        author_mapper = mapper_registry[Author]
        kurt = self.data['kurt']

        kid = kurt.id
        new_last_name = 'Vonnegut, Jr.'
        a = author_mapper.get(id=kid)
        a.last_name = new_last_name
        author_mapper.save(a)

        a = author_mapper.get(kid)
        self.assertEqual(a.last_name, new_last_name)

    def test_count(self):
        author_mapper, book_mapper = mapper_registry[Author], mapper_registry[Book]

        self.assertEqual(author_mapper.query.count(), 3)
        self.assertEqual(len(book_mapper.query.clone()), 4)
        self.assertEqual(len(book_mapper.query.clone()[1:4]), 3)

    def test_delete(self):
        author_mapper, book_mapper = mapper_registry[Author], mapper_registry[Book]
        kurt = self.data['kurt']

        author_mapper.delete(kurt)
        self.assertEqual(author_mapper.query.count(), 2)
        self.assertEqual(len(book_mapper.query.clone()), 3)

    def test_validation(self):
        author_mapper = mapper_registry[Author]

        a = Author(first_name='', last_name='Ted')
        self.assertRaises(exceptions.ValidationError, author_mapper.validate, a)

    def test_defaults(self):
        author_mapper = mapper_registry[Author]

        a = Author(first_name='Bill and', last_name='Ted')
        author_mapper.save(a)
        self.assertEqual(a.bio, 'No bio available')

        a = Author(first_name='I am a', last_name='BadGuy!')
        self.assertRaises(exceptions.ValidationError, author_mapper.validate, a)

    def test_smartsql(self):
        author_mapper, book_mapper = mapper_registry[Author], mapper_registry[Book]
        slww = self.data['slww']

        fields = [smartsql.compile(i)[0] for i in author_mapper.get_sql_fields()]
        self.assertListEqual(
            fields,
            ['"ascetic_tests_author"."id"',
             '"ascetic_tests_author"."first_name"',
             '"ascetic_tests_author"."last_name"',
             '"ascetic_tests_author"."bio"', ]
        )

        # self.assertEqual(smartsql.compile(book_mapper.sql_table.author)[0], '"books"."author_id"')
        smartsql.auto_name.counter = 0
        self.assertEqual(
            smartsql.compile(book_mapper.query.where(book_mapper.sql_table.author.id == 1)),
            ('SELECT "books"."id", "books"."title", "books"."author_id" FROM "books" INNER '
             'JOIN "ascetic_tests_author" AS "_auto_1" ON ("books"."author_id" = '
             '"_auto_1"."id") WHERE "_auto_1"."id" = %s',
             [1])
        )
        smartsql.auto_name.counter = 0
        self.assertEqual(
            smartsql.compile(author_mapper.query.where(
                (book_mapper.sql_table.author.id == 1) & (book_mapper.sql_table.author.first_name == 'Ivan')
            )),
            ('SELECT "ascetic_tests_author"."id", "ascetic_tests_author"."first_name", '
             '"ascetic_tests_author"."last_name", "ascetic_tests_author"."bio" FROM '
             '"ascetic_tests_author" INNER JOIN "ascetic_tests_author" AS "_auto_1" ON '
             '("books"."author_id" = "_auto_1"."id") INNER JOIN "ascetic_tests_author" AS '
             '"_auto_2" ON ("books"."author_id" = "_auto_2"."id") WHERE "_auto_1"."id" = '
             '%s AND "_auto_2"."first_name" = %s',
             [1, 'Ivan'])
        )
        smartsql.auto_name.counter = 0
        author_table = book_mapper.sql_table.author
        self.assertEqual(
            smartsql.compile(author_mapper.query.where(
                (author_table.id == 1) & (author_table.first_name == 'Ivan')
            )),
            ('SELECT "ascetic_tests_author"."id", "ascetic_tests_author"."first_name", '
             '"ascetic_tests_author"."last_name", "ascetic_tests_author"."bio" FROM '
             '"ascetic_tests_author" INNER JOIN "ascetic_tests_author" AS "_auto_1" ON '
             '("books"."author_id" = "_auto_1"."id") WHERE "_auto_1"."id" = %s AND '
             '"_auto_1"."first_name" = %s',
             [1, 'Ivan'])
        )

        q = author_mapper.query
        self.assertEqual(smartsql.compile(q)[0], '''SELECT "ascetic_tests_author"."id", "ascetic_tests_author"."first_name", "ascetic_tests_author"."last_name", "ascetic_tests_author"."bio" FROM "ascetic_tests_author"''')
        self.assertEqual(len(q), 3)
        for obj in q:
            self.assertTrue(isinstance(obj, Author))

        q = q.where(author_mapper.sql_table.id == slww.author_id)
        self.assertEqual(smartsql.compile(q)[0], """SELECT "ascetic_tests_author"."id", "ascetic_tests_author"."first_name", "ascetic_tests_author"."last_name", "ascetic_tests_author"."bio" FROM "ascetic_tests_author" WHERE "ascetic_tests_author"."id" = %s""")
        self.assertEqual(len(q), 1)
        self.assertTrue(isinstance(q[0], Author))

    def test_prefetch(self):
        author_mapper, book_mapper = mapper_registry[Author], mapper_registry[Book]

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
