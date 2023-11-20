import os
import sys
import csv
import time
import datetime
import json

import click

from th.db import ThDatabase
from helpers.config import Config
from helpers.helpers import print_error
from gn2.parser import (
    remove_headers,
    add_headers,
    remove_columns,
    add_columns,
    insert_values_to_columns,
    force_protected_char,
)
from th.parser import (
    check_attributes_headers,
    replace_code_theme,
    flip_text_row,
    check_taxon_code,
    replace_taxon_code,
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
    default="t",
    help="""Type of import file:
        t (=text),
        a (=attribute),
        th =(theme),
        m =(media),
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
    TaxHub Import Parser

    This script parse files containing Postregsql \copy data before integrate
    their in TaxHub database.
    To avoid to use integer identifiers in import files we use alphanumeric value
    (code) for theme and attributes linked data.

    This script produce new files suffixed by '_rti' (ready to import) where
    all codes were replaced by integers identifiers specific to a TaxHub database.

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
    db_access_need = set(["a", "t", "m"])
    if import_type in db_access_need:
        db = ThDatabase()
        db.connect_to_database()
        # Show database infos
        db.print_database_infos()

    # If necessary, get infos in the database
    if import_type == "a":
        themes = db.get_all_themes()
    elif import_type == "t":
        attributes = db.get_all_attributes()
    elif import_type == "m":
        taxons_codes = db.get_all_taxons_codes()
        scinames_codes = db.get_all_scinames_codes()

    # Open CSV files
    with open(filename_src, "r", newline="", encoding="utf-8") as f_src:
        reader = csv.DictReader(f_src, dialect=reader_dialect)
        with open(filename_dest, "w", newline="", encoding="utf-8") as f_dest:
            if import_type == "t":
                check_attributes_headers(attributes, reader.fieldnames)
                output_fieldnames = ["cd_ref", "attribut_id", "text"]
                writer = csv.DictWriter(
                    f_dest, dialect=writer_dialect, fieldnames=output_fieldnames
                )
                writer.writeheader()
            else:
                fieldnames = remove_headers(reader.fieldnames)
                fieldnames = add_headers(fieldnames)
                writer = csv.DictWriter(f_dest, dialect=writer_dialect, fieldnames=fieldnames)
                writer.writeheader()

            try:
                for row in reader:
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

                    if import_type == "a":
                        # Replace Theme Code
                        row = replace_code_theme(row, themes, reader)
                        # Write in destination file
                        writer.writerow(row)
                    elif import_type == "t":
                        # Flip attribute from column to line
                        for new_row in flip_text_row(row, attributes):
                            # Write in destination file
                            writer.writerow(new_row)
                    elif import_type == "m":
                        # Check Taxon code (=cd_ref)
                        if check_taxon_code(row, taxons_codes) is False:
                            print_error(
                                f"Line {reader.line_num}: taxon code {row['cd_ref']} not exists ! Trying to find a cd_ref by using cd_nom in TaxRef !"
                            )
                            (exists, row) = replace_taxon_code(row, scinames_codes)
                            if exists is False:
                                print_error(
                                    f"Line {reader.line_num} removed ! Taxon or sciname code {row['cd_ref']} not exists in TaxRef !"
                                )
                            else:
                                writer.writerow(row)
                        else:
                            writer.writerow(row)
                    else:
                        writer.writerow(row)
            except csv.Error as e:
                sys.exit(f"Error in file {filename}, line {reader.line_num}: {e}")

    # Script time elapsed
    time_elapsed = time.time() - start_time
    time_elapsed_for_human = str(datetime.timedelta(seconds=time_elapsed))
    print(f"Script time elapsed: {time_elapsed_for_human}")


def set_actions_type(abbr_type):
    types = {
        "th": "THEME",
        "a": "ATTRIBUT",
        "t": "TEXT",
        "m": "MEDIA",
    }
    if abbr_type in types:
        Config.setParameter("actions.type", types[abbr_type])
    else:
        print_error(f'Type "{abbr_type}" is not implemented !')
        exit()


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


if __name__ == "__main__":
    parse_file()
