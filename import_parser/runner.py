import os
import sys
import csv
import time
import datetime


import click

# Define OS Environment variables
root_dir = os.path.realpath(f'{os.path.dirname(os.path.abspath(__file__))}/../../')
config_shared_dir = os.path.realpath(f'{root_dir}/shared/config/')
app_dir = os.path.realpath(f'{os.path.dirname(os.path.abspath(__file__))}/../')
config_dir = os.path.realpath(f'{app_dir}/config/')
os.environ['IMPORT_PARSER.PATHES.ROOT'] = root_dir
os.environ['IMPORT_PARSER.PATHES.SHARED.CONFIG'] = config_shared_dir
os.environ['IMPORT_PARSER.PATHES.APP'] = app_dir
os.environ['IMPORT_PARSER.PATHES.APP.CONFIG'] = config_dir

from gn2.db import GnDatabase
from helpers.config import Config
from helpers.helpers import print_msg, print_info, print_error, print_verbose, find_ranges
from gn2.parser import *


@click.command()
@click.argument(
    'filename',
    type=click.Path(exists=True),
)
@click.option(
    '-t',
    '--type',
    'import_type',
    default='s',
    help='''Type of import file:
        s (=synthese),
        so =(source),
        d (=dataset),
        af (=acquisition_framework),
        o (=organism),
        u (=user)
    ''',
)
@click.option(
    '-c',
    '--config',
    'actions_config_file',
    default=f'{config_dir}/actions.default.ini',
    help='Config file with actions to execute on CSV.',
)
def parse_file(filename, import_type, actions_config_file):
    """
    GeoNature 2 Import Parser

    This script parse files containing Postregsql \copy data before integrate their in GeoNature 2 database.
    To avoid to use integer identifiers in import files we use alphanumeric value for nomenclature, dataset,
    organisms or users linked data.

    This script produce new files suffixed by '_rti' (ready to import) where all codes were replaced by integers
    identifiers specific to a GeoNature 2 database.

    Each import files must follow a specific format describe in this SINP Wiki :
    https://wiki-sinp.cbn-alpin.fr/database/import-formats

    Access to the GeoNature database must be configured in 'shared/config/settings.ini' file.
    """
    start_time = time.time()

    filename_src = click.format_filename(filename)
    filename_dest =  os.path.splitext(filename_src)[0] + '_rti.csv'

    set_actions_type(import_type)
    load_actions_config_file(actions_config_file)
    load_nomenclatures()

    click.echo('Source filename:' + filename_src)
    click.echo('Destination filename:' + filename_dest)
    click.echo('Type:' + import_type)
    click.echo('Remove columns ? ' + str(Config.get('actions.remove_columns')))
    click.echo('Columns to remove: ' + ', '.join(Config.get('actions.remove_columns.params')))

    csv.register_dialect(
        'sql_copy',
        delimiter='\t',
        quotechar='',
        escapechar='',
        quoting=csv.QUOTE_NONE,
        lineterminator="\n"
    )

    # Access to the database if necessary
    db_access_need = set(['s', 'u', 'af', 'd'])
    if import_type in db_access_need:
        db = GnDatabase()
        db.connect_to_database()
        # Show database infos
        db.print_database_infos()

    # If necessary, get infos in the database
    if import_type == 's' :
        datasets = db.get_all_datasets()
        modules = db.get_all_modules()
        sources = db.get_all_sources()
        nomenclatures = db.get_all_nomenclatures()
        scinames_codes = db.get_all_scinames_codes()
        users = db.get_all_users()
    elif import_type == 'u':
        organisms = db.get_all_organisms()
    elif import_type == 'af':
        nomenclatures = db.get_all_nomenclatures()
    elif import_type == 'd':
        nomenclatures = db.get_all_nomenclatures()
        acquisition_frameworks = db.get_all_acquisition_frameworks()

    # Open CSV files
    with open(filename_src, 'r', newline='', encoding='utf-8') as f_src:
        total_csv_lines_nbr = calculate_csv_entries_number(f_src)
        # TODO: add an option to analyse number of tabulation by lines
        # TODO: create a class to manage reports
        # TODO: use a template (JINJA ?) to render reports
        reports = {
            'lines_removed_total': 0,
            'sciname_removed_lines': {},
            'date_missing_removed_lines': [],
            'date_max_removed_lines': [],
            'source_code_unknown_lines': {},
            'dataset_code_unknown_lines': {},
            'organism_code_unknown_lines': {},
            'nomenclature_code_unknown_lines': {},
            'digitiser_code_unknown_lines': {},
            'altitude_inverted_lines': [],
            'altitude_errors_lines': [],
            'altitude_min_fixed_lines': [],
            'altitude_max_fixed_lines': [],
        }

        reader = csv.DictReader(f_src, dialect='sql_copy')
        with open(filename_dest, 'w', newline='', encoding='utf-8') as f_dest:
            fieldnames = remove_headers(reader.fieldnames)
            fieldnames = add_headers(fieldnames)
            writer = csv.DictWriter(f_dest, dialect='sql_copy', fieldnames=fieldnames)
            writer.writeheader()

            # TODO: see why progressbar don't work !
            with click.progressbar(length=int(total_csv_lines_nbr), label="Parsing lines", show_pos=True) as pbar:
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

                        if import_type == 's' :
                            # Add observation UUID
                            row = add_uuid_obs(row)

                            # Check Sciname code
                            if check_sciname_code(row, scinames_codes, reader, reports) == False:
                                write_row = False
                                print_error(f"Line {reader.line_num} removed, sciname code {row['cd_nom']} not exists in TaxRef !")

                            # Check date_min and date_max
                            if check_dates(row, reader, reports) == False:
                                write_row = False
                                print_error(f"Line {reader.line_num} removed, mandatory dates missing !")
                            elif check_date_max_greater_than_min(row, reader, reports) == False:
                                write_row = False
                                print_error(f"Line {reader.line_num} removed, date max not greater than date min !")

                            # Fix altitudes
                            if write_row != False:
                                row = fix_altitude_min(row, reader, reports)
                                row = fix_altitude_max(row, reader, reports)
                                row = fix_inverted_altitudes(row, reader, reports)
                                row = fix_altitudes_errors(row, reader, reports)

                            # Replace codes
                            if write_row != False:
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
                        elif import_type == 'u':
                            # Replace Organism Code
                            row = replace_code_organism(row, organisms, reader, reports)
                        elif import_type == 'af':
                            # Replace Nomenclatures Codes
                            row = replace_code_nomenclature(row, nomenclatures, reader, reports)
                        elif import_type == 'd':
                            # Replace Nomenclatures Codes
                            row = replace_code_nomenclature(row, nomenclatures, reader, reports)
                            row = replace_code_acquisition_framework(row, acquisition_frameworks)

                        # Write in destination file
                        if write_row == True:
                            writer.writerow(row)

                        # Update progressbar
                        #pbar.update(int(reader.line_num))
                        pbar.update(1)
                except csv.Error as e:
                    sys.exit(f'Error in file {filename}, line {reader.line_num}: {e}')
    # Report
    if import_type == 'u' :
        total = 0
        lines_to_print = []
        for code, lines in reports['organism_code_unknown_lines'].items():
            lines_to_print.append(f"       {code}: {', '.join(lines)}")
            total += len(lines)
        print_info(f'   List of {total} lines with unknown organism codes:')
        print_info('\n'.join(lines_to_print))
        print_info('-'*72)
    elif import_type == 's' :
        print_msg(f"Total lines removed: {reports['lines_removed_total']: }")
        print_info('-'*72)

        total = 0
        lines_to_print = []
        for sciname, lines in reports['sciname_removed_lines'].items():
            lines_to_print.append(f"       {sciname}: {', '.join(lines)}")
            total += len(lines)
        print_info(f'   List of {total} removed lines with unknown scinames codes:')
        print_info('\n'.join(lines_to_print))
        print_info('-'*72)

        total = len(reports['date_missing_removed_lines'])
        print_info(f'   List of {total} removed lines with missing date min or max:')
        print_info(f"       {', '.join(reports['date_missing_removed_lines'])}")
        print_info('-'*72)

        total = len(reports['date_max_removed_lines'])
        print_info(f'   List of {total} removed lines with date max not greater than date min:')
        print_info(f"       {', '.join(reports['date_max_removed_lines'])}")
        print_info('-'*72)

        total = 0
        lines_to_print = []
        for dataset_code, lines in reports['dataset_code_unknown_lines'].items():
            lines_to_print.append(f"       {dataset_code}: {', '.join(lines)}")
            total += len(lines)
        print_info(f'   List of {total} lines with unknown dataset codes:')
        print_info('\n'.join(lines_to_print))
        print_info('-'*72)

        total = 0
        lines_to_print = []
        for type_and_code, lines in reports['nomenclature_code_unknown_lines'].items():
            lines_to_print.append(f"       {type_and_code}: {', '.join(lines)}")
            total += len(lines)
        print_info(f'   List of {total} lines with unknown nomenclature codes:')
        print_info('\n'.join(lines_to_print))
        print_info('-'*72)

        total = 0
        lines_to_print = []
        for code, lines in reports['source_code_unknown_lines'].items():
            lines_to_print.append(f"       {code}: {', '.join(lines)}")
            total += len(lines)
        print_info(f'   List of {total} lines with unknown source codes:')
        print_info('\n'.join(lines_to_print))
        print_info('-'*72)

        total = 0
        lines_to_print = []
        for digitiser_code, lines in reports['digitiser_code_unknown_lines'].items():
            lines_to_print.append(f"       {digitiser_code}: {', '.join(lines)}")
            total += len(lines)
        print_info(f'   List of {total} lines with unknown digitiser codes:')
        print_info('\n'.join(lines_to_print))
        print_info('-'*72)

        total = len(reports['altitude_min_fixed_lines'])
        print_info(f'   List of {total} lines with altitude min fixed:')
        print_info(f"       {', '.join(reports['altitude_min_fixed_lines'])}")
        print_info('-'*72)

        total = len(reports['altitude_max_fixed_lines'])
        print_info(f'   List of {total} lines with altitude max fixed:')
        print_info(f"       {', '.join(reports['altitude_max_fixed_lines'])}")
        print_info('-'*72)

        total = len(reports['altitude_inverted_lines'])
        print_info(f'   List of {total} lines with altitudes inverted fixed:')
        print_info(f"       {', '.join(reports['altitude_inverted_lines'])}")
        print_info('-'*72)

        total = len(reports['altitude_errors_lines'])
        print_info(f'   List of {total} lines with altitudes errors:')
        print_info(f"       {', '.join(reports['altitude_errors_lines'])}")
        print_info('-'*72)

    # Script time elapsed
    time_elapsed = time.time() - start_time
    time_elapsed_for_human = str(datetime.timedelta(seconds=time_elapsed))
    print_msg('Script time')
    print_info(f'   Elapsed: {time_elapsed_for_human}')

def set_actions_type(abbr_type):
    types = {
        'af': 'ACQUISITION_FRAMEWORK',
        'd' : 'DATASET',
        'o': 'ORGANISM',
        's': 'SYNTHESE',
        'so': 'SOURCE',
        'u': 'USER',
    }
    if abbr_type in types:
        Config.setParameter('actions.type', types[abbr_type])
    else:
        print_error(f'Type "{abbr_type}" is not implemented !')

def load_actions_config_file(actions_config_file):
    if actions_config_file != '' and os.path.exists(actions_config_file) :
        print(f'Actions config file: {actions_config_file}')
        Config.load(actions_config_file)
        define_current_actions()
    else:
        print_error(f'Actions config file "${actions_config_file}" not exists !')

def define_current_actions():
    actions_type = Config.get('actions.type')
    parameters = Config.getSection(actions_type)
    for key, value in parameters.items():
        Config.setParameter(key, value)

def load_nomenclatures():
    nomenclatures_needed = set(['SYNTHESE', 'ACQUISITION_FRAMEWORK', 'DATASET'])
    if Config.has('actions.type') and Config.get('actions.type') in nomenclatures_needed :
        Config.load(Config.nomenclatures_config_file_path)


if __name__ == '__main__':
    parse_file()
