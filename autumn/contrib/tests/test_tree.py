import unittest
from autumn.connections import get_db
from autumn.models import Model
from autumn.contrib.tree import MpGateway, MpModel

Location = None


class TestMpTree(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS autumn_tree_location CASCADE;
            CREATE TABLE autumn_tree_location (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                name VARCHAR(40),
                parent_id integer,
                parent_lang VARCHAR(6),
                tree_path VARCHAR(256),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (parent_id, parent_lang) REFERENCES autumn_tree_location (id, lang) ON DELETE CASCADE
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS autumn_tree_location CASCADE;
            CREATE TABLE autumn_tree_location (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                name VARCHAR(40),
                parent_id integer,
                parent_lang VARCHAR(6),
                tree_path VARCHAR(256),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (parent_id, parent_lang) REFERENCES autumn_tree_location (id, lang) ON DELETE CASCADE
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS autumn_tree_location CASCADE;
            CREATE TABLE autumn_tree_location (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                name VARCHAR(40),
                parent_id integer,
                parent_lang VARCHAR(6),
                tree_path VARCHAR(256),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (parent_id, parent_lang) REFERENCES autumn_tree_location (id, lang) ON DELETE CASCADE
            );
        """
    }

    @classmethod
    def create_models(cls):

        class Location(MpModel, Model):
            class Gateway(MpGateway):
                db_table = 'autumn_tree_location'

        return locals()

    @classmethod
    def setUpClass(cls):
        db = get_db()
        db.cursor().execute(cls.create_sql[db.engine])
        for model_name, model in cls.create_models().items():
            globals()[model_name] = model

    def setUp(self):
        db = get_db()
        for table in ('autumn_tree_location',):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
        root = Location(
            id=1,
            lang='en',
            name='root'
        )
        root.save()

        obj_1_1 = Location(
            id=1,
            lang='en',
            name='root'
        )
        obj_1_1.parent=root
        root.save()
