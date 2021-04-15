import os
import sys
import re
import uuid
import configparser

from helpers.config import Config
from helpers.helpers import (
    print_msg, print_info, print_error, print_verbose, find_ranges, is_uuid, is_empty_or_null
)

# TODO: use at least one class to store all methods
# TODO: for code (source, dataset) replacement, see if we set a NULL value or if we ignore the line


# Computing CSV file number of lines without header line
def calculate_csv_entries_number(file_handle):
    print_msg('Computing CSV file total number of entries...')
    total_lines = sum(1 for line in file_handle) - 1
    file_handle.seek(0)
    if total_lines < 1 :
        print_error("Number of total lines in CSV file can't be lower than 1.")
        exit(1)

    print_info(f'Number of entries in CSV files: {total_lines} ')
    return total_lines

# Remove row entries where fieldname match pattern
def remove_headers(fieldnames):
    output = fieldnames.copy()
    if Config.get('actions.remove_columns'):
        col_patterns = Config.get('actions.remove_columns.params')
        for pattern in col_patterns:
            for field in fieldnames:
                if re.match(rf'^{pattern}$', field):
                    output.remove(field)
    return output

# Add new row entries if necessary
def add_headers(fieldnames):
    output = fieldnames.copy()
    if Config.get('actions.add_uuid_obs') and 'unique_id_sinp' not in fieldnames:
        output.insert(0, 'unique_id_sinp')
    return output

# Remove row entries where fieldname match pattern
def remove_columns(row, reader):
    if Config.get('actions.remove_columns'):
        col_patterns = Config.get('actions.remove_columns.params')
        fieldnames = list(row.keys())
        for pattern in col_patterns:
            for field in fieldnames:
                try:
                    if re.match(rf'^{pattern}$', field):
                        del row[field]
                except TypeError as e:
                    report_value = get_report_field_value(row, reader)
                    msg = [
                        f"ERROR ({report_value}): in remove_columns().",
                        f"\tPattern: {pattern}",
                        f"\tField: {field}",
                    ]
                    print_error('\n'.join(msg))
                    print(row)
    return row

# Add row entries if necessary
def add_columns(row):
    if Config.get('actions.add_uuid_obs') and 'unique_id_sinp' not in row:
        row['unique_id_sinp'] = Config.get('null_value_string')
    return row

def insert_values_to_columns(row):
    if Config.get('actions.set_values'):
        col_values =  Config.get('actions.set_values.params')
        fieldnames = list(row.keys())
        for pattern, value in col_values.items():
            for field in fieldnames:
                if re.match(rf'^{pattern}$', field):
                    row[field] = value
    return row

def add_uuid_obs(row):
    if Config.get('actions.add_uuid_obs'):
        if not is_uuid(row['unique_id_sinp']):
            row['unique_id_sinp'] = uuid.uuid4()
    return row

def get_report_field_value(row, reader):
    value = str(reader.line_num)
    if (
        Config.has('reports.field')
        and Config.get('reports.field') in row
        and row[Config.get('reports.field')] != Config.get('null_value_string')
    ):
        value = row[Config.get('reports.field')]
    return value

def check_sciname_code(row, scinames_codes, reader, reports):
    exists = True
    if row['cd_nom'] != None and row['cd_nom'] != Config.get('null_value_string'):
        exists = (str(row['cd_nom']) in scinames_codes)
    if not exists:
        reports['lines_removed_total'] += 1
        report_value = get_report_field_value(row, reader)
        (
            reports['sciname_removed_lines']
            .setdefault(str(row['cd_nom']), [])
            .append(report_value)
        )
    return exists

def check_dates(row, reader, reports):
    is_ok = True
    if row['date_min'] == None or row['date_min'] == Config.get('null_value_string'):
        is_ok = False
    if row['date_max'] == None or row['date_max'] == Config.get('null_value_string'):
        is_ok = False
    if not is_ok:
        reports['lines_removed_total'] += 1
        report_value = get_report_field_value(row, reader)
        reports['date_missing_removed_lines'].append(report_value)
    return is_ok

def check_date_max_greater_than_min(row, reader, reports):
    is_ok = False
    if check_dates(row, reader, reports) and row['date_max'] >= row['date_min']:
        is_ok = True
    if not is_ok:
        reports['lines_removed_total'] += 1
        report_value = get_report_field_value(row, reader)
        reports['date_max_removed_lines'].append(report_value)
    return is_ok

def fix_altitude_min(row, reader, reports):
    decimal_separators = [',', '.']
    if 'altitude_min' in row.keys() and not is_empty_or_null(row['altitude_min']):
        alt = row['altitude_min']
        if [sep for sep in decimal_separators if (sep in alt)]:
            alt_fixed = alt
            for sep in decimal_separators:
                if alt_fixed.find(sep) >= 0:
                    alt_fixed = alt_fixed[:alt_fixed.find(sep)]
            report_value = get_report_field_value(row, reader)
            msg = [
                f'WARNING ({report_value}): altitude min fixing !',
                f'\tAltitude: {alt}',
                f'\tAltitude fixed: {alt_fixed}'
            ]
            print_error('\n'.join(msg))
            reports['altitude_min_fixed_lines'].append(report_value)
            row['altitude_min'] = alt_fixed
    return row

def fix_altitude_max(row, reader, reports):
    decimal_separators = [',', '.']
    if 'altitude_max' in row.keys() and not is_empty_or_null(row['altitude_max']):
        alt = row['altitude_max']
        if [sep for sep in decimal_separators if (sep in alt)]:
            alt_fixed = alt
            for sep in decimal_separators:
                if alt_fixed.find(sep) >= 0:
                    alt_fixed = alt_fixed[:alt_fixed.find(sep)]
            report_value = get_report_field_value(row, reader)
            reports['altitude_max_fixed_lines'].append(report_value)
            msg = [
                f'WARNING ({report_value}): altitude max fixing !',
                f'\tAltitude: {alt}',
                f'\tAltitude fixed: {alt_fixed}'
            ]
            print_error('\n'.join(msg))
            row['altitude_max'] = alt_fixed
    return row

def has_altitudes(row, reader, reports):
    is_ok = True
    if row['altitude_min'] == None or row['altitude_min'] == Config.get('null_value_string'):
        is_ok = False
    if row['altitude_max'] == None or row['altitude_max'] == Config.get('null_value_string'):
        is_ok = False
    return is_ok

def fix_inverted_altitudes(row, reader, reports):
    if has_altitudes(row, reader, reports) and int(row['altitude_max']) < int(row['altitude_min']):
        report_value = get_report_field_value(row, reader)
        reports['altitude_inverted_lines'].append(report_value)
        msg = f"WARNING ({report_value}): altitudes min ({row['altitude_min']}) - max ({row['altitude_max']}) inverted !"
        print_error(msg)
        tmp_alt_min = row['altitude_min']
        row['altitude_min'] = row['altitude_max']
        row['altitude_max'] = tmp_alt_min
    return row

def fix_altitudes_errors(row, reader, reports):
    is_ok = False
    if (
        has_altitudes(row, reader, reports) == False
        or (
            has_altitudes(row, reader, reports) == True and (
                int(row['altitude_max']) >= int(row['altitude_min'])
                and int(row['altitude_min']) >= 0 and int(row['altitude_min']) <= 4696 # Mont Blanc
                and int(row['altitude_max']) >= 0 and int(row['altitude_max']) <= 4696 # Mont Blanc
            )
        )
    ):
        is_ok = True
    if not is_ok:
        report_value = get_report_field_value(row, reader)
        msg = [
                f"WARNING ({report_value}): altitude error !",
                f"\Altitude min: {row['altitude_min']}",
                f"\Altitude max: {row['altitude_max']}",
                f"\tSet to null value string !"
            ]
        print_error('\n'.join(msg))
        reports['altitude_errors_lines'].append(report_value)
        row['altitude_min'] = Config.get('null_value_string')
        row['altitude_max'] = Config.get('null_value_string')
    return row

def replace_code_dataset(row, datasets, reader, reports):
    if 'code_dataset' in row.keys() and row['code_dataset'] != None:
        code = row['code_dataset']
        try:
            if datasets[code]:
                row['code_dataset'] = datasets[code]
        except KeyError as e:
            report_value = get_report_field_value(row, reader)
            msg = [
                f"WARNING ({report_value}): dataset code missing !",
                f"\tDataset code: {code}",
                f"\tSet to null value string !"
            ]
            print_error('\n'.join(msg))
            (
                reports['dataset_code_unknown_lines']
                .setdefault(str(code), [])
                .append(report_value)
            )
            row['code_dataset'] = Config.get('null_value_string')
    return row

def replace_code_module(row, modules):
    if 'code_module' in row.keys() and row['code_module'] != None:
        code = row['code_module']
        if modules[code]:
            row['code_module'] = modules[code]
    return row

def replace_code_source(row, sources, reader, reports):
    if 'code_source' in row.keys() and row['code_source'] != None:
        code = row['code_source']
        try:
            if sources[code]:
                row['code_source'] = sources[code]
        except KeyError as e:
            report_value = get_report_field_value(row, reader)
            msg = [
                f"WARNING ({report_value}): source code missing !",
                f"\tSource code: {code}",
                f"\tSet to null value string !"
            ]
            print_error('\n'.join(msg))
            (
                reports['source_code_unknown_lines']
                .setdefault(str(code), [])
                .append(report_value)
            )
            row['code_source'] = Config.get('null_value_string')
    return row

def replace_code_nomenclature(row, nomenclatures, reader, reports):
    columns_types = Config.getSection('NOMENCLATURES')
    fieldnames = list(row.keys())
    for field in fieldnames:
        if field.startswith('code_nomenclature_'):
            nomenclature_type = columns_types[field]
            code = row[field]
            if code != Config.get('null_value_string'):
                try:
                    row[field] = nomenclatures[nomenclature_type][code]
                except KeyError as e:
                    report_value = get_report_field_value(row, reader)
                    msg = [
                        f"WARNING ({report_value}): nomenclature entry missing !",
                        f"\tNomenclature type: {nomenclature_type}",
                        f"\tCode: {code}",
                        f"\tSet to null value string !"
                    ]
                    print_error('\n'.join(msg))
                    (
                        reports['nomenclature_code_unknown_lines']
                        .setdefault(f"{nomenclature_type}-{code}", [])
                        .append(report_value)
                    )
                    row[field] = Config.get('null_value_string')
    return row

def replace_code_organism(row, organisms, reader, reports):
    if 'code_organism' in row.keys() and row['code_organism'] != None:
        code = row['code_organism']
        try:
            if organisms[code]:
                row['code_organism'] = organisms[code]
        except KeyError as e:
            report_value = get_report_field_value(row, reader)
            msg = [
                f"WARNING ({report_value}): organism code missing !",
                f"\tOrganism code: {code}",
                f"\tSet to null value string !"
            ]
            print_error('\n'.join(msg))
            (
                reports['organism_code_unknown_lines']
                .setdefault(str(code), [])
                .append(report_value)
            )
            row['code_organism'] = Config.get('null_value_string')
    return row

def replace_code_acquisition_framework(row, acquisition_frameworks):
    if 'code_acquisition_framework' in row.keys() and row['code_acquisition_framework'] != None:
        code = row['code_acquisition_framework']
        if acquisition_frameworks[code]:
            row['code_acquisition_framework'] = acquisition_frameworks[code]
    return row

def replace_code_digitiser(row, users, reader, reports):
    if 'code_digitiser' in row and row['code_digitiser'] != None:
        code = row['code_digitiser']
        try:
            if users[code]:
                row['code_digitiser'] = users[code]
        except KeyError as e:
            report_value = get_report_field_value(row, reader)
            msg = [
                f"WARNING ({report_value}): digitiser (=> user) code missing !",
                f"\Digitiser code: {code}",
                f"\tSet to null value string !"
            ]
            print_error('\n'.join(msg))
            (
                reports['digitiser_code_unknown_lines']
                .setdefault(str(code), [])
                .append(report_value)
            )
            row['code_digitiser'] = Config.get('null_value_string')
    return row
