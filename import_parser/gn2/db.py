import psycopg2
import psycopg2.extras

from helpers.config import Config


class GnDatabase:
    db_connection = None
    db_cursor = None

    def connect_to_database(self):
        self.db_connection = psycopg2.connect(
            database=Config.get("db_name"),
            user=Config.get("db_user"),
            password=Config.get("db_pass"),
            host=Config.get("db_host"),
            port=Config.get("db_port"),
        )
        self.db_cursor = self.db_connection.cursor(
            cursor_factory=psycopg2.extras.DictCursor,
        )

    def print_database_infos(self):
        print("Database infos:")
        # Print PostgreSQL Connection properties
        for key, value in self.db_connection.get_dsn_parameters().items():
            print(f"\t{key}:{value}")

        # Print PostgreSQL version
        self.db_cursor.execute("SELECT version()")
        record = self.db_cursor.fetchone()
        print(f"You are connected to - {record}")

    def close(self):
        self.db_cursor.close()
        self.db_connection.close()

    def get_dataset_id(self, code):
        self.db_cursor.execute(
            f"""
            SELECT dataset_shortname AS code, id_dataset AS id
            FROM gn_meta.t_datasets
            WHERE dataset_shortname = '{code}'
        """
        )
        records = self.db_cursor.fetchall()
        print(records)

    def get_all_datasets(self):
        if Config.has("fk.datasets") and Config.get("fk.datasets") == "UUID":
            code_field = "unique_dataset_id"
        else:
            code_field = "dataset_shortname"

        self.db_cursor.execute(
            f"""
            SELECT {code_field} AS code, id_dataset AS id
            FROM gn_meta.t_datasets
        """
        )
        records = self.db_cursor.fetchall()
        datasets = {}
        for record in records:
            datasets[record["code"]] = record["id"]
        return datasets

    def get_all_modules(self):
        self.db_cursor.execute(
            """
            SELECT module_code AS code, id_module AS id
            FROM gn_commons.t_modules
        """
        )
        records = self.db_cursor.fetchall()
        modules = {}
        for record in records:
            modules[record["code"]] = record["id"]
        return modules

    def get_all_sources(self):
        self.db_cursor.execute(
            """
            SELECT name_source AS code, id_source AS id
            FROM gn_synthese.t_sources
        """
        )
        records = self.db_cursor.fetchall()
        sources = {}
        for record in records:
            sources[record["code"]] = record["id"]
        return sources

    def get_all_areas(self):
        self.db_cursor.execute(
            """
            SELECT
                bib.type_code AS type,
                la.id_area AS id,
                la.area_code AS code
            FROM ref_geo.bib_areas_types bib
            JOIN ref_geo.l_areas la ON la.id_type = bib.id_type
        """
        )
        records = self.db_cursor.fetchall()
        areas = {}
        for record in records:
            areas.setdefault(record["type"], {})
            areas[record["type"]][record["code"]] = record["id"]
        return areas

    def get_all_nomenclatures(self):
        nomenclatures_columns_types = Config.getSection("NOMENCLATURES")
        types = list(nomenclatures_columns_types.values())

        self.db_cursor.execute(
            """
            SELECT bnt.mnemonique AS type, tn.cd_nomenclature AS code, tn.id_nomenclature AS id
            FROM ref_nomenclatures.t_nomenclatures AS tn
                INNER JOIN ref_nomenclatures.bib_nomenclatures_types AS bnt
                    ON (tn.id_type = bnt.id_type)
            WHERE bnt.mnemonique = ANY(%s)
            ORDER BY bnt.mnemonique ASC, tn.cd_nomenclature ASC
        """,
            (types,),
        )
        records = self.db_cursor.fetchall()
        nomenclatures = {}
        for record in records:
            nomenclatures.setdefault(record["type"], {})
            nomenclatures[record["type"]][record["code"]] = record["id"]
        return nomenclatures

    def get_all_scinames_codes(self):
        self.db_cursor.execute(
            """
            SELECT DISTINCT cd_nom AS code, lb_nom AS name
            FROM taxonomie.taxref
        """
        )
        records = self.db_cursor.fetchall()
        codes = {}
        for record in records:
            codes[str(record["code"])] = record["name"]
        return codes

    def get_all_organisms(self):
        if Config.has("fk.organisms") and Config.get("fk.organisms") == "UUID":
            code_field = "uuid_organisme"
        else:
            code_field = "nom_organisme"

        self.db_cursor.execute(
            f"""
            SELECT {code_field} AS code, id_organisme AS id
            FROM utilisateurs.bib_organismes
        """
        )
        records = self.db_cursor.fetchall()
        organisms = {}
        for record in records:
            organisms[record["code"]] = record["id"]
        return organisms

    def check_sciname_code(self, sciname_code):
        self.db_cursor.execute(
            """
            SELECT t.cd_nom
            FROM taxonomie.taxref AS t
            WHERE cd_nom = %s
        """,
            (sciname_code,),
        )
        return self.db_cursor.fetchone() is not None

    def get_all_acquisition_frameworks(self):
        if Config.has("fk.af") and Config.get("fk.af") == "UUID":
            code_field = "unique_acquisition_framework_id"
        else:
            code_field = "acquisition_framework_name"

        self.db_cursor.execute(
            f"""
            SELECT {code_field} AS code, id_acquisition_framework AS id
            FROM gn_meta.t_acquisition_frameworks
        """
        )
        records = self.db_cursor.fetchall()
        acquisition_frameworks = {}
        for record in records:
            acquisition_frameworks[record["code"]] = record["id"]
        return acquisition_frameworks

    def get_all_users(self):
        if Config.has("fk.users") and Config.get("fk.users") == "UUID":
            code_field = "uuid_role"
        else:
            code_field = "identifiant"

        self.db_cursor.execute(
            f"""
            SELECT {code_field} AS code, id_role AS id
            FROM utilisateurs.t_roles
        """
        )
        records = self.db_cursor.fetchall()
        users = {}
        for record in records:
            users[str(record["code"])] = record["id"]
        return users
