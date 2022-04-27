
from helpers.config import Config
from helpers.helpers import (print_error, is_empty_or_null, get_date_format)
from gn2.parser import get_report_field_value


def check_attributes_headers(attributes, fieldnames):
    exists = True
    for fieldname in fieldnames:
        if fieldname != 'cd_ref' and not (fieldname in attributes):
            msg = [
                f"WARNING ({fieldname}): attribute code missing !",
                f"\Attribute code: {fieldname}",
                f"\tExit !"
            ]
            print_error('\n'.join(msg))
            exit()
    return exists


def replace_code_theme(row, themes, reader):
    if 'theme_code' in row.keys() and not is_empty_or_null(row['theme_code']):
        code = row['theme_code']
        try:
            if themes[code]:
                row['theme_code'] = themes[code]
        except KeyError as e:
            report_value = get_report_field_value(row, reader)
            msg = [
                f"WARNING ({report_value}): theme code missing !",
                f"\Theme code: {code}",
                f"\tSet to null value string !"
            ]
            print_error('\n'.join(msg))
            row['theme_code'] = Config.get('null_value_string')
    return row


def flip_text_row(row, attributes):
    new_rows = []
    for fieldname in row.keys():
        value = row[fieldname]
        if not is_empty_or_null(value) and fieldname != 'cd_ref':
            new_rows.append({
                'cd_ref': row['cd_ref'],
                'attribut_id': attributes[fieldname],
                'text': value,
            })
    return new_rows

def check_taxon_code(row, taxons_codes):
    exists = False
    if row['cd_ref'] != None and row['cd_ref'] != Config.get('null_value_string'):
        exists = (str(row['cd_ref']) in taxons_codes.keys())
    return exists

def replace_taxon_code(row, scinames_codes):
    exists = False
    if row['cd_ref'] != None and row['cd_ref'] != Config.get('null_value_string'):
        exists = (str(row['cd_ref']) in scinames_codes.keys())
    if exists:
        row['cd_ref'] = scinames_codes[row['cd_ref']]
    return (exists, row)
