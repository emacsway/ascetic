import unittest


class TestModelTranslation(unittest.TestCase):

    maxDiff = None

    create_sql = {
        'postgresql': """
            DROP TABLE IF EXISTS autumn_modeltranslation_author CASCADE;
            CREATE TABLE autumn_modeltranslation_author (
                id serial NOT NULL PRIMARY KEY,
                first_name_en VARCHAR(40) NOT NULL,
                first_name_ru VARCHAR(40) NOT NULL,
                last_name_en VARCHAR(40) NOT NULL,
                last_name_ru VARCHAR(40) NOT NULL,
                bio TEXT
            );
         """,
        'mysql': """
            DROP TABLE IF EXISTS autumn_modeltranslation_author CASCADE;
            CREATE TABLE autumn_modeltranslation_author (
                id INT(11) NOT NULL auto_increment,
                first_name_en VARCHAR(40) NOT NULL,
                first_name_ru VARCHAR(40) NOT NULL,
                last_name_en VARCHAR(40) NOT NULL,
                last_name_ru VARCHAR(40) NOT NULL,
                bio TEXT,
                PRIMARY KEY (id)
            );
         """,
        'sqlite3': """
            DROP TABLE IF EXISTS autumn_modeltranslation_author;
            CREATE TABLE autumn_modeltranslation_author (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              first_name VARCHAR(40) NOT NULL,
              first_name VARCHAR(40) NOT NULL,
              last_name VARCHAR(40) NOT NULL,
              last_name VARCHAR(40) NOT NULL,
              bio TEXT
            );
        """
    }

    def test_modeltranslation(self):
        self.assertTrue(True)
