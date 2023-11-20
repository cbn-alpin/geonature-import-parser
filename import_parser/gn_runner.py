import os
import sys
import csv
import time
import datetime
import json

import click
from jinja2 import Environment, FileSystemLoader

from gn2.db import GnDatabase
from helpers.config import Config
from helpers.helpers import print_error
from gn2.parser import (
    calculate_csv_entries_number,
    remove_headers,
    add_headers,
    remove_columns,
    add_columns,
    insert_values_to_columns,
    force_protected_char,
    add_uuid_obs,
    replace_empty_value,
    check_sciname_code,
    check_dates,
    check_date_max_greater_than_min,
    check_date_min_in_future,
    check_date_max_in_future,
    fix_altitude_min,
    fix_altitude_max,
    fix_negative_altitudes,
    fix_inverted_altitudes,
    fix_altitudes_errors,
    fix_depth_min,
    fix_depth_max,
    replace_code_dataset,
    replace_code_module,
    replace_code_source,
    replace_code_nomenclature,
    replace_code_digitiser,
    replace_code_area,
    replace_code_organism,
    set_default_nomenclature_values,
    replace_code_acquisition_framework,
)


# Define OS Environment variables
root_dir = os.path.realpath(f"{os.path.dirname(os.path.abspath(__file__))}/../../")
config_shared_dir = os.path.realpath(f"{root_dir}/shared/config/")
app_dir = os.path.realpath(f"{os.path.dirname(os.path.abspath(__file__))}/../")
config_dir = os.path.realpath(f"{app_dir}/config/")
os.environ["IMPORT_PARSER.PATHES.ROOT"] = root_dir
os.environ["IMPORT_PARSER.PATHES.SHARED.CONFIG"] = config_shared_dir
os.environ["IMPORT_PARSER.PATHES.APP"] = app_dir
os.environ["IMPORT_PARSER.PATHES.APP.CONFIG"] = config_dir


@click.command()
@click.argument(
    "filename",
    type=click.Path(exists=True),
)
@click.option(
    "-t",
    "--type",
    "import_type",
    default="s",
    help="""Type of import file:
        s (=synthese),
        so =(source),
        d (=dataset),
        af (=acquisition_framework),
        o (=organism),
        u (=user),
        tr (=taxref_rank),
        t (=taxref),
    """,
)
@click.option(
    "-c",
    "--config",
    "actions_config_file",
    default=f"{config_dir}/actions.default.ini",
    help="Config file with actions to execute on CSV.",
)
@click.option(
    "-r",
    "--report",
    "report_dir",
    default=False,
    help="Directory where the report file is stored.",
)
def parse_file(filename, import_type, actions_config_file, report_dir):
    """
    GeoNature 2 Import Parser

    This script parse files containing Postregsql \copy data before integrate
    their in GeoNature 2 database.
    To avoid to use integer identifiers in import files we use alphanumeric value
    (code or uuid) for nomenclature, dataset,organisms or users linked data.

    This script produce new files suffixed by '_rti' (ready to import) where
    all codes or uuid were replaced by integers identifiers specific to a
    GeoNature 2 database.

    Each import files must follow a specific format describe in this SINP Wiki :
    https://wiki-sinp.cbn-alpin.fr/database/import-formats

    Access to the GeoNature database must be configured in
    '../shared/config/settings.ini' file.
    """
    start_time = time.time()

    filename_src = click.format_filename(filename)
    filename_dest = os.path.splitext(filename_src)[0] + "_rti.csv"

    set_actions_type(import_type)
    load_actions_config_file(actions_config_file)
    load_nomenclatures()

    reader_dialect = Config.get("csv.reader.dialect") if Config.has("csv.reader.dialect") else "tsv"
    writer_dialect = Config.get("csv.writer.dialect") if Config.has("csv.writer.dialect") else "tsv"

    click.echo("Source filename:" + filename_src)
    click.echo("Destination filename:" + filename_dest)
    click.echo("Type:" + import_type)
    click.echo("Remove columns ? " + str(Config.get("actions.remove_columns")))
    click.echo("Columns to remove: " + ", ".join(Config.get("actions.remove_columns.params")))
    click.echo("Add columns ? " + str(Config.get("actions.add_columns")))
    click.echo(
        "Columns to add: "
        + json.dumps(
            Config.get("actions.add_columns.params"), indent=4, sort_keys=True, default=str
        )
    )
    click.echo("Set columns values ? " + str(Config.get("actions.set_values")))
    click.echo(
        "Columns to set values: "
        + json.dumps(Config.get("actions.set_values.params"), indent=4, sort_keys=True, default=str)
    )
    click.echo(f"CSV Reader dialect: {reader_dialect}")
    click.echo(f"CSV Writer dialect: {writer_dialect}")
    click.echo(
        "fk.organisms ? "
        + (str(Config.get("fk.organisms")) if Config.has("fk.organisms") else "none")
    )
    click.echo("fk.af ? " + (str(Config.get("fk.af")) if Config.has("fk.af") else "none"))
    click.echo("fk.users ? " + (str(Config.get("fk.users")) if Config.has("fk.users") else "none"))

    csv.register_dialect(
        "ssv",
        delimiter=";",
        quotechar='"',
        doublequote=True,
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
        lineterminator="\r\n",
    )
    csv.register_dialect(
        "ssv-minimal",
        delimiter=";",
        quotechar='"',
        doublequote=True,
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        lineterminator="\n",
    )
    csv.register_dialect(
        "tsv",
        delimiter="\t",
        quotechar='"',
        doublequote=True,
        quoting=csv.QUOTE_MINIMAL,
        # quoting=csv.QUOTE_NONE,
        escapechar="\\",
        lineterminator="\n",
    )

    # Access to the database if necessary
    db_access_need = set(["s", "u", "af", "d"])
    if import_type in db_access_need:
        db = GnDatabase()
        db.connect_to_database()
        # Show database infos
        db.print_database_infos()

    # If necessary, get infos in the database
    if import_type == "s":
        datasets = db.get_all_datasets()
        modules = db.get_all_modules()
        sources = db.get_all_sources()
        nomenclatures = db.get_all_nomenclatures()
        scinames_codes = db.get_all_scinames_codes()
        users = db.get_all_users()
        areas = db.get_all_areas()
    elif import_type == "u":
        organisms = db.get_all_organisms()
    elif import_type == "af":
        nomenclatures = db.get_all_nomenclatures()
    elif import_type == "d":
        nomenclatures = db.get_all_nomenclatures()
        acquisition_frameworks = db.get_all_acquisition_frameworks()

    # Open CSV files
    with open(filename_src, "r", newline="", encoding="utf-8") as f_src:
        total_csv_lines_nbr = calculate_csv_entries_number(f_src)
        # TODO: add an option to analyse number of tabulation by lines
        # TODO: create a class to manage reports
        # TODO: use a template (JINJA ?) to render reports
        reports = {
            "lines_removed_total": 0,
            "sciname_removed_lines": {},
            "date_missing_removed_lines": [],
            "date_max_removed_lines": [],
            "date_min_in_future_removed_lines": [],
            "date_max_in_future_removed_lines": [],
            "source_code_unknown_lines": {},
            "dataset_code_unknown_lines": {},
            "area_code_unknown_lines": {},
            "organism_code_unknown_lines": {},
            "nomenclature_code_unknown_lines": {},
            "digitiser_code_unknown_lines": {},
            "af_code_unknown_lines": {},
            "altitude_negative_lines": [],
            "altitude_inverted_lines": [],
            "altitude_errors_lines": [],
            "altitude_min_fixed_lines": [],
            "altitude_max_fixed_lines": [],
            "depth_min_fixed_lines": [],
            "depth_max_fixed_lines": [],
        }

        reader = csv.DictReader(f_src, dialect=reader_dialect)
        with open(filename_dest, "w", newline="", encoding="utf-8") as f_dest:
            fieldnames = remove_headers(reader.fieldnames)
            fieldnames = add_headers(fieldnames)
            writer = csv.DictWriter(f_dest, dialect=writer_dialect, fieldnames=fieldnames)
            writer.writeheader()

            # TODO: see why progressbar don't work !
            with click.progressbar(
                length=int(total_csv_lines_nbr), label="Parsing lines", show_pos=True
            ) as pbar:
                try:
                    for row in reader:
                        # Initialize variables
                        write_row = True

                        # TODO: check if number of fields is egal to number of columns,
                        # else there is a tab in fields value !

                        # Remove useless columns
                        row = remove_columns(row, reader)

                        # Add new columns if necessary
                        row = add_columns(row)

                        # Insert value in colums
                        row = insert_values_to_columns(row)

                        # Maintain protected char
                        row = force_protected_char(row)

                        if import_type == "s":
                            # Add observation UUID
                            row = add_uuid_obs(row)

                            # Replace empty value in specific columns by NULL
                            row = replace_empty_value(row)

                            # Check Sciname code
                            if check_sciname_code(row, scinames_codes, reader, reports) is False:
                                write_row = False
                                print_error(
                                    f"Line {reader.line_num} removed, sciname code {row['cd_nom']} not exists in TaxRef !"
                                )

                            # Check date_min and date_max
                            if check_dates(row, reader, reports) is False:
                                write_row = False
                                print_error(
                                    f"Line {reader.line_num} removed, mandatory dates missing !"
                                )
                            elif check_date_max_greater_than_min(row, reader, reports) is False:
                                write_row = False
                                print_error(
                                    f"Line {reader.line_num} removed, date max not greater than date min !"
                                )
                            elif check_date_min_in_future(row, reader, reports) is False:
                                write_row = False
                                print_error(
                                    f"Line {reader.line_num} removed, date min in the future !"
                                )
                            elif check_date_max_in_future(row, reader, reports) is False:
                                write_row = False
                                print_error(
                                    f"Line {reader.line_num} removed, date max in the future !"
                                )

                            # Fix altitudes
                            if write_row is not False:
                                row = fix_altitude_min(row, reader, reports)
                                row = fix_altitude_max(row, reader, reports)
                                row = fix_negative_altitudes(row, reader, reports)
                                row = fix_inverted_altitudes(row, reader, reports)
                                row = fix_altitudes_errors(row, reader, reports)
                                row = fix_depth_min(row, reader, reports)
                                row = fix_depth_max(row, reader, reports)

                            # Replace codes
                            if write_row is not False:
                                # Replace Dataset Code
                                row = replace_code_dataset(row, datasets, reader, reports)
                                # Replace Module Code
                                row = replace_code_module(row, modules)
                                # Replace Source Code
                                row = replace_code_source(row, sources, reader, reports)
                                # Replace Nomenclatures Codes
                                row = replace_code_nomenclature(row, nomenclatures, reader, reports)
                                # Replace Digitiser Code
                                row = replace_code_digitiser(row, users, reader, reports)
                                # Replace Area Code
                                row = replace_code_area(row, areas, reader, reports)
                        elif import_type == "u":
                            # Replace Organism Code
                            row = replace_code_organism(row, organisms, reader, reports)
                        elif import_type == "af":
                            # Replace Nomenclatures Codes
                            row = replace_code_nomenclature(row, nomenclatures, reader, reports)
                        elif import_type == "d":
                            # Replace Nomenclatures Codes
                            row = set_default_nomenclature_values(row)
                            row = replace_code_nomenclature(row, nomenclatures, reader, reports)
                            row = replace_code_acquisition_framework(
                                row, acquisition_frameworks, reader, reports
                            )

                        # Write in destination file
                        if write_row is True:
                            writer.writerow(row)

                        # Update progressbar
                        # pbar.update(int(reader.line_num))
                        pbar.update(1)
                except csv.Error as e:
                    sys.exit(f"Error in file {filename}, line {reader.line_num}: {e}")

    # Script time elapsed
    time_elapsed = time.time() - start_time
    time_elapsed_for_human = str(datetime.timedelta(seconds=time_elapsed))

    # Build and print report
    app_path = os.environ["IMPORT_PARSER.PATHES.APP"]
    tpl_path = f"{app_path}/import_parser/templates"
    action_type = Config.get("actions.type").lower()
    tpl_file = f"reports/{action_type}.txt.j2"
    if os.path.exists(f"{tpl_path}/{tpl_file}"):
        file_loader = FileSystemLoader(searchpath=tpl_path)
        env = Environment(loader=file_loader)
        template = env.get_template(tpl_file)
        report_output = template.render(reports=reports, elapsed_time=time_elapsed_for_human)
        print(report_output)

        # Save the report
        if report_dir:
            if not os.path.exists(report_dir):
                os.makedirs(report_dir)
            current_date = datetime.date.today().isoformat()
            report_path = f"{report_dir}/{current_date}_{action_type}.report.txt"
            with open(report_path, "w") as fh:
                fh.write(report_output)


def set_actions_type(abbr_type):
    types = {
        "af": "ACQUISITION_FRAMEWORK",
        "d": "DATASET",
        "o": "ORGANISM",
        "s": "SYNTHESE",
        "so": "SOURCE",
        "u": "USER",
        "tr": "TAXREF_RANK",
        "t": "TAXREF",
    }
    if abbr_type in types:
        Config.setParameter("actions.type", types[abbr_type])
    else:
        print_error(f'Type "{abbr_type}" is not implemented !')


def load_actions_config_file(actions_config_file):
    if actions_config_file != "" and os.path.exists(actions_config_file):
        print(f"Actions config file: {actions_config_file}")
        Config.load(actions_config_file)
        define_current_actions()
    else:
        print_error(f'Actions config file "${actions_config_file}" not exists !')


def define_current_actions():
    actions_type = Config.get("actions.type")
    parameters = Config.getSection(actions_type)
    for key, value in parameters.items():
        Config.setParameter(key, value)


def load_nomenclatures():
    nomenclatures_needed = set(["SYNTHESE", "ACQUISITION_FRAMEWORK", "DATASET"])
    if Config.has("actions.type") and Config.get("actions.type") in nomenclatures_needed:
        Config.load(Config.nomenclatures_config_file_path)


if __name__ == "__main__":
    parse_file()
