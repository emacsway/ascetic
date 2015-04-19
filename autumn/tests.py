#!/usr/bin/env python
import unittest
from autumn import validators
from autumn import utils
from autumn.connections import get_db
from autumn.models import Model, ForeignKey
from sqlbuilder import smartsql

Author = Book = None


class TestModels(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS autumn_tests_author CASCADE;
            CREATE TABLE autumn_tests_author (
                id serial NOT NULL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                bio TEXT
            );
            DROP TABLE IF EXISTS books CASCADE;
            CREATE TABLE books (
                id serial NOT NULL PRIMARY KEY,
                title VARCHAR(255),
                author_id integer REFERENCES autumn_tests_author(id) ON DELETE CASCADE
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS autumn_tests_author CASCADE;
            CREATE TABLE autumn_tests_author (
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
                FOREIGN KEY (author_id) REFERENCES autumn_tests_author(id),
                PRIMARY KEY (id)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS autumn_tests_author;
            CREATE TABLE autumn_tests_author (
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
              FOREIGN KEY (author_id) REFERENCES autumn_tests_author(id)
            );
        """
    }

    @classmethod
    def create_models(cls):
        class Author(Model):
            # books = OneToMany('autumn.tests.models.Book')

            class Meta:
                db_table = 'autumn_tests_author'
                defaults = {'bio': 'No bio available'}
                validations = {'first_name': validators.Length(),
                               'last_name': (validators.Length(), lambda x: x != 'BadGuy!' or 'Bad last name', )}

        class Book(Model):
            author = ForeignKey(Author, rel_name='books')

            class Meta:
                db_table = 'books'

        return locals()

    @classmethod
    def setUpClass(cls):
        db = get_db()
        db.cursor().execute(cls.create_sql[db.engine])
        for model_name, model in cls.create_models().items():
            globals()[model_name] = model

    def setUp(self):
        db = get_db()
        for table in ('autumn_tests_author', 'books'):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
        # Create tables

        # Test Creation
        james = Author(first_name='James', last_name='Joyce')
        james.save()

        kurt = Author(first_name='Kurt', last_name='Vonnegut')
        kurt.save()

        tom = Author(first_name='Tom', last_name='Robbins')
        tom.save()

        Book(title='Ulysses', author_id=james.id).save()
        Book(title='Slaughter-House Five', author_id=kurt.id).save()
        Book(title='Jitterbug Perfume', author_id=tom.id).save()
        slww = Book(title='Still Life with Woodpecker', author_id=tom.id)
        slww.save()

        # Test Model.pk getter and setter
        pk = slww.pk
        self.assertEqual(slww.pk, slww.id)
        slww.pk = tom.id
        self.assertEqual(slww.pk, tom.id)
        slww.pk = pk
        self.assertEqual(slww.pk, pk)

        self.assertTrue(kurt == Author.get(kurt.id))
        self.assertTrue(kurt != tom)
        self.assertEqual(hash(kurt), kurt.pk)

        # Test ForeignKey
        self.assertEqual(slww.author.first_name, 'Tom')
        slww.author = kurt
        self.assertEqual(slww.author.first_name, 'Kurt')
        del slww.author
        self.assertEqual(slww.author, None)
        slww.author = None
        self.assertEqual(slww.author, None)
        slww.author = tom.pk
        self.assertEqual(slww.author.first_name, 'Tom')

        # Test OneToMany
        self.assertEqual(len(list(tom.books)), 2)

        kid = kurt.id
        del(james, kurt, tom, slww)

        # Test retrieval
        b = Book.get(title='Ulysses')[0]

        a = Author.get(id=b.author_id)[0]
        self.assertEqual(a.id, b.author_id)

        a = Author.get(id=b.id)[:]
        # self.assert_(isinstance(a, list))
        self.assert_(isinstance(a, smartsql.QS))

        # Test update
        new_last_name = 'Vonnegut, Jr.'
        a = Author.get(id=kid)[0]
        a.last_name = new_last_name
        a.save()

        a = Author.get(kid)
        self.assertEqual(a.last_name, new_last_name)

        # Test count
        self.assertEqual(Author.get().count(), 3)
        self.assertEqual(len(Book.get()), 4)
        self.assertEqual(len(Book.get()[1:4]), 3)

        # Test delete
        a.delete()
        self.assertEqual(Author.get().count(), 2)
        self.assertEqual(len(Book.get()), 3)

        # Test validation
        a = Author(first_name='', last_name='Ted')
        self.assertRaises(validators.ValidationError, a.validate)

        # Test defaults
        a.first_name = 'Bill and'
        a.save()
        self.assertEqual(a.bio, 'No bio available')

        a = Author(first_name='I am a', last_name='BadGuy!')
        self.assertRaises(validators.ValidationError, a.validate)

        print '### Testing for smartsql integration'
        t = Author.s
        fields = [t.qs.result.db.compile(i)[0] for i in t.get_fields()]
        if get_db().engine == 'postgresql':
            self.assertListEqual(
                fields,
                ['"autumn_tests_author"."id"',
                 '"autumn_tests_author"."first_name"',
                 '"autumn_tests_author"."last_name"',
                 '"autumn_tests_author"."bio"', ]
            )
        else:
            self.assertListEqual(
                fields,
                ['`autumn_tests_author`.`id`',
                 '`autumn_tests_author`.`first_name`',
                 '`autumn_tests_author`.`last_name`',
                 '`autumn_tests_author`.`bio`', ]
            )

        if get_db().engine == 'postgresql':
            self.assertEqual(Book.s.author, '"book"."author_id"')
        else:
            self.assertEqual(Book.s.author, '`book`.`author_id`')

        qs = t.qs
        if get_db().engine == 'postgresql':
            self.assertEqual(qs.result.db.compile(qs)[0], '''SELECT "autumn_tests_author"."id", "autumn_tests_author"."first_name", "autumn_tests_author"."last_name", "autumn_tests_author"."bio" FROM "autumn_tests_author"''')
        else:
            self.assertEqual(qs.result.db.compile(qs)[0], """SELECT `autumn_tests_author`.`id`, `autumn_tests_author`.`first_name`, `autumn_tests_author`.`last_name`, `autumn_tests_author`.`bio` FROM `autumn_tests_author`""")
        self.assertEqual(len(qs), 3)
        for obj in qs:
            self.assertTrue(isinstance(obj, Author))

        qs = qs.where(t.id == b.author_id)
        if get_db().engine == 'postgresql':
            self.assertEqual(qs.result.db.compile(qs)[0], """SELECT "autumn_tests_author"."id", "autumn_tests_author"."first_name", "autumn_tests_author"."last_name", "autumn_tests_author"."bio" FROM "autumn_tests_author" WHERE "autumn_tests_author"."id" = %s""")
        else:
            self.assertEqual(qs.result.db.compile(qs)[0], """SELECT `autumn_tests_author`.`id`, `autumn_tests_author`.`first_name`, `autumn_tests_author`.`last_name`, `autumn_tests_author`.`bio` FROM `autumn_tests_author` WHERE `autumn_tests_author`.`id` = %s""")
        self.assertEqual(len(qs), 1)
        self.assertTrue(isinstance(qs[0], Author))

        # prefetch
        for obj in Book.qs.prefetch('author').order_by(Book.s.id):
            self.assertTrue(hasattr(obj, 'author_prefetch'))
            self.assertEqual(obj.author_prefetch, obj.author)

        for obj in Author.qs.prefetch('books').order_by(Author.s.id):
            self.assertTrue(hasattr(obj, 'books_prefetch'))
            self.assertEqual(len(obj.books_prefetch), len(obj.books))
            for i in obj.books_prefetch:
                self.assertEqual(i.author_prefetch, obj)


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
        from autumn.connections import DummyCtx
        self.assertTrue(utils.resolve('autumn.connections.DummyCtx') is DummyCtx)
