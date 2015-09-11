import unittest
from ascetic.databases import databases
from ascetic.models import Model, IdentityMap, mapper_registry
from ascetic.contrib.modeltranslation import TranslationMapper

Author = None


class TranslationMapper(TranslationMapper):

    _language = 'ru'

    def get_languages(self):
        return ('ru', 'en')

    def get_language(self):
        return self._language


class TestModelTranslation(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS ascetic_modeltranslation_author CASCADE;
            CREATE TABLE ascetic_modeltranslation_author (
                id serial NOT NULL PRIMARY KEY,
                first_name_en VARCHAR(40),
                first_name_ru VARCHAR(40),
                last_name_en VARCHAR(40),
                last_name_ru VARCHAR(40),
                bio TEXT
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS ascetic_modeltranslation_author CASCADE;
            CREATE TABLE ascetic_modeltranslation_author (
                id INT(11) NOT NULL auto_increment,
                first_name_en VARCHAR(40),
                first_name_ru VARCHAR(40),
                last_name_en VARCHAR(40),
                last_name_ru VARCHAR(40),
                bio TEXT,
                PRIMARY KEY (id)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS ascetic_modeltranslation_author;
            CREATE TABLE ascetic_modeltranslation_author (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name VARCHAR(40),
                first_name VARCHAR(40),
                last_name VARCHAR(40),
                last_name VARCHAR(40),
              bio TEXT
            );
        """
    }

    @classmethod
    def create_models(cls):

        class Author(Model):
            class Mapper(TranslationMapper):
                db_table = 'ascetic_modeltranslation_author'
                map = {'first_alias': 'first_name'}
                defaults = {'bio': 'No bio available'}
                translated_fields = ('first_alias', 'last_name')

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
        for table in ('ascetic_modeltranslation_author',):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_meta(self):
        mapper = mapper_registry[Author]
        self.assertIn('id', mapper.fields)
        self.assertIn('bio', mapper.fields)
        self.assertIn('first_alias', mapper.fields)
        self.assertIn('last_name', mapper.fields)
        for lang in mapper.get_languages():
            self.assertNotIn('first_alias_{}'.format(lang), mapper.fields)
            self.assertNotIn('last_name_{}'.format(lang), mapper.fields)

        self.assertIn('id', mapper.columns)
        self.assertIn('bio', mapper.columns)
        for lang in mapper.get_languages():
            self.assertIn('first_name_{}'.format(lang), mapper.columns)
            self.assertIn('last_name_{}'.format(lang), mapper.columns)

        for lang in mapper.get_languages():
            self.assertNotIn('first_name', mapper.columns)
            self.assertNotIn('last_name', mapper.columns)

        self.assertEqual(len(mapper.fields), 4)
        self.assertEqual(len(mapper.columns), 6)

        current_language = mapper.get_language()
        for lang in mapper.get_languages():
            mapper._language = lang
            self.assertEqual(mapper.fields['id'].column, 'id')
            self.assertEqual(mapper.fields['bio'].column, 'bio')
            self.assertEqual(mapper.fields['first_alias'].column, 'first_name_{}'.format(lang))
            self.assertEqual(mapper.fields['last_name'].column, 'last_name_{}'.format(lang))
        mapper._language = current_language

    def test_model(self):
        author = Author(first_alias='First name', last_name='Last name')
        author.save()
        author = Author.get(author.id)
        self.assertEqual(author.first_alias, 'First name')
        self.assertEqual(author.last_name, 'Last name')
