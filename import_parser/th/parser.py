import sys
from helpers.config import Config
from helpers.helpers import print_error, is_empty_or_null
from gn2.parser import get_report_field_value


def check_attributes_headers(attributes, fieldnames):
    exists = True
    for fieldname in fieldnames:
        if fieldname != "cd_ref" and fieldname not in attributes:
            msg = [
                f"WARNING ({fieldname}): attribute code missing !",
                f"\tAttribute code: {fieldname}",
                "\tExit !",
            ]
            print_error("\n".join(msg))
            sys.exit()
    return exists


def replace_code_theme(row, themes, reader):
    if "theme_code" in row and not is_empty_or_null(row["theme_code"]):
        code = row["theme_code"]
        try:
            if themes[code]:
                row["theme_code"] = themes[code]
        except KeyError:
            report_value = get_report_field_value(row, reader)
            msg = [
                f"WARNING ({report_value}): theme code missing !",
                f"\tTheme code: {code}",
                "\tSet to null value string !",
            ]
            print_error("\n".join(msg))
            row["theme_code"] = Config.get("null_value_string")
    return row


def flip_text_row(row, attributes):
    new_rows = []
    for fieldname in row:
        value = row[fieldname]
        if not is_empty_or_null(value) and fieldname != "cd_ref":
            new_rows.append(
                {
                    "cd_ref": row["cd_ref"],
                    "attribut_id": attributes[fieldname],
                    "text": value,
                }
            )
    return new_rows


def check_taxon_code(row, taxons_codes):
    exists = False
    if row["cd_ref"] is not None and row["cd_ref"] != Config.get("null_value_string"):
        exists = str(row["cd_ref"]) in taxons_codes
    return exists


def replace_taxon_code(row, scinames_codes, reader):
    exists = False
    if row["cd_ref"] is not None and row["cd_ref"] != Config.get("null_value_string"):
        exists = str(row["cd_ref"]) in scinames_codes
    if exists:
        row["cd_ref"] = scinames_codes[row["cd_ref"]]

    if exists is False:
        print_error(
            f"Line {reader.line_num} removed ! Taxon or sciname code {row['cd_ref']} not exists in TaxRef !"
        )
    return (exists, row)

def replace_code_type(row, reader):
    exists = False
    if "type" in row and not is_empty_or_null(row["type"]):
        code = row["type"]
        try:
            if Config.get("media_types")[code]:
                row["type"] = Config.get("media_types")[code]
                exists = True
        except KeyError:
            report_value = get_report_field_value(row, reader)
            msg = [
                f"WARNING ({report_value}): media type code missing !",
                f"\tLine {reader.line_num} removed ! Media type code {code} not exists !",
            ]
            print_error("\n".join(msg))
    return (exists, row)
