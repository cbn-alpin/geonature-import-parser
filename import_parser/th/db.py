import psycopg2
import psycopg2.extras

from helpers.config import Config

class ThDatabase:
    db_connection = None
    db_cursor = None

    def connect_to_database(self):
        self.db_connection = psycopg2.connect(
            database=Config.get('db_name'),
            user=Config.get('db_user'),
            password=Config.get('db_pass'),
            host=Config.get('db_host'),
            port=Config.get('db_port'),
        )
        self.db_cursor = self.db_connection.cursor(
            cursor_factory = psycopg2.extras.DictCursor,
        )

    def print_database_infos(self):
        print('Database infos:')
        # Print PostgreSQL Connection properties
        for key, value in self.db_connection.get_dsn_parameters().items():
            print(f'\t{key}:{value}')

        # Print PostgreSQL version
        self.db_cursor.execute("SELECT version()")
        record = self.db_cursor.fetchone()
        print(f'You are connected to - {record}')

    def get_all_themes(self):
        self.db_cursor.execute(f"""
            SELECT nom_theme AS code, id_theme AS id
            FROM taxonomie.bib_themes
        """)
        records = self.db_cursor.fetchall()
        themes = {}
        for record in records:
            themes[record['code']] = record['id']
        return themes

    def get_all_attributes(self):
        self.db_cursor.execute(f"""
            SELECT nom_attribut AS code, id_attribut AS id
            FROM taxonomie.bib_attributs
        """)
        records = self.db_cursor.fetchall()
        themes = {}
        for record in records:
            themes[record['code']] = record['id']
        return themes
