import unittest
from autumn.connections import get_db
from autumn.models import Model
from autumn.contrib.modeltranslation import TranslationGatewayMixIn

Author = None


class TranslationGateway(TranslationGatewayMixIn):

    _language = 'ru'

    def get_languages(self):
        return ('ru', 'en')

    def get_language(self):
        return self._language


class TestModelTranslation(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS autumn_modeltranslation_author CASCADE;
            CREATE TABLE autumn_modeltranslation_author (
                id serial NOT NULL PRIMARY KEY,
                first_name_en VARCHAR(40),
                first_name_ru VARCHAR(40),
                last_name_en VARCHAR(40),
                last_name_ru VARCHAR(40),
                bio TEXT
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS autumn_modeltranslation_author CASCADE;
            CREATE TABLE autumn_modeltranslation_author (
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
            DROP TABLE IF EXISTS autumn_modeltranslation_author;
            CREATE TABLE autumn_modeltranslation_author (
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
            class Gateway(TranslationGateway):
                db_table = 'autumn_modeltranslation_author'
                map = {'first_alias': 'first_name'}
                defaults = {'bio': 'No bio available'}
                translated_fields = ('first_alias', 'last_name')

        return locals()

    @classmethod
    def setUpClass(cls):
        db = get_db()
        db.cursor().execute(cls.create_sql[db.engine])
        for model_name, model in cls.create_models().items():
            globals()[model_name] = model

    def setUp(self):
        db = get_db()
        for table in ('autumn_modeltranslation_author',):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_meta(self):
        gateway = Author._gateway
        self.assertIn('id', gateway.fields)
        self.assertIn('bio', gateway.fields)
        self.assertIn('first_alias', gateway.fields)
        self.assertIn('last_name', gateway.fields)
        for lang in gateway.get_languages():
            self.assertNotIn('first_alias_{}'.format(lang), gateway.fields)
            self.assertNotIn('last_name_{}'.format(lang), gateway.fields)

        self.assertIn('id', gateway.columns)
        self.assertIn('bio', gateway.columns)
        for lang in gateway.get_languages():
            self.assertIn('first_name_{}'.format(lang), gateway.columns)
            self.assertIn('last_name_{}'.format(lang), gateway.columns)

        for lang in gateway.get_languages():
            self.assertNotIn('first_name', gateway.columns)
            self.assertNotIn('last_name', gateway.columns)

        self.assertEqual(len(gateway.fields), 4)
        self.assertEqual(len(gateway.columns), 6)

        current_language = gateway.get_language()
        for lang in gateway.get_languages():
            gateway._language = lang
            self.assertEqual(gateway.fields['id'].column, 'id')
            self.assertEqual(gateway.fields['bio'].column, 'bio')
            self.assertEqual(gateway.fields['first_alias'].column, 'first_name_{}'.format(lang))
            self.assertEqual(gateway.fields['last_name'].column, 'last_name_{}'.format(lang))
        gateway._language = current_language

    def test_model(self):
        author = Author(first_alias='First name', last_name='Last name')
        author.save()
        author = Author.get(author.id)
        self.assertEqual(author.first_alias, 'First name')
        self.assertEqual(author.last_name, 'Last name')
