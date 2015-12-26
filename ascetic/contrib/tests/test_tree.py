import unittest
from ascetic.databases import databases
from ascetic.models import IdentityMap, Mapper, mapper_registry
from ascetic.contrib.tree import MpMapper, MpModel

Location = None


class TestMpTree(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS ascetic_tree_location CASCADE;
            CREATE TABLE ascetic_tree_location (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                name VARCHAR(40),
                parent_id integer,
                parent_lang VARCHAR(6),
                tree_path VARCHAR(256),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (parent_id, parent_lang) REFERENCES ascetic_tree_location (id, lang) ON DELETE CASCADE
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS ascetic_tree_location CASCADE;
            CREATE TABLE ascetic_tree_location (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                name VARCHAR(40),
                parent_id integer,
                parent_lang VARCHAR(6),
                tree_path VARCHAR(256),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (parent_id, parent_lang) REFERENCES ascetic_tree_location (id, lang) ON DELETE CASCADE
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS ascetic_tree_location CASCADE;
            CREATE TABLE ascetic_tree_location (
                id INTEGER NOT NULL,
                lang VARCHAR(6) NOT NULL,
                name VARCHAR(40),
                parent_id integer,
                parent_lang VARCHAR(6),
                tree_path VARCHAR(256),
                PRIMARY KEY (id, lang),
                FOREIGN KEY (parent_id, parent_lang) REFERENCES ascetic_tree_location (id, lang) ON DELETE CASCADE
            );
        """
    }

    @classmethod
    def create_models(cls):

        class Location(MpModel):
            def __init__(self, id=None, lang=None, name=None, parent_id=None, parent_lang=None, tree_path=None):
                self.id = id
                self.lang = lang
                self.name = name
                self.parent_id = parent_id
                self.parent_lang = parent_lang
                self.tree_path = tree_path

        class LocationMapper(MpMapper, Mapper):
            db_table = 'ascetic_tree_location'

        LocationMapper(Location)

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
        for table in ('ascetic_tree_location',):
            db.execute('DELETE FROM {0}'.format(db.qn(table)))

    def test_model(self):
        location_mapper = mapper_registry[Location]
        root = Location(
            id=1,
            lang='en',
            name='root'
        )
        location_mapper.save(root)

        obj_1_1 = Location(
            id=2,
            lang='en',
            name='root'
        )
        obj_1_1.parent = root
        location_mapper.save(obj_1_1)
