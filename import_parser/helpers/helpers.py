import click
import operator
import itertools
import uuid

from helpers.config import Config


# TODO: see why click echo with color don't work !
def print_msg(msg):
    click.echo(click.style(msg, fg='yellow'))


def print_info(msg):
    click.echo(click.style(msg, fg='white', bold='true'))


def print_error(msg):
    click.echo(click.style(msg, fg='red'))


def print_verbose(msg):
    click.echo(click.style(msg, fg='black'))


def find_ranges(data):
    """Yield range of consecutive numbers."""
    ranges = []
    for k,g in itertools.groupby(enumerate(data),lambda x:x[0]-x[1]):
        group = (map(operator.itemgetter(1),g))
        group = list(map(int,group))
        ranges.append((group[0],group[-1]))
    return ranges

def is_uuid(value):
    try:
        return uuid.UUID(value).version == 4
    except ValueError:
        return False

def is_empty_or_null(value):
    is_eon = False
    if value == None:
        is_eon = True
    elif value == '':
        is_eon = True
    elif value == Config.get('null_value_string'):
        is_eon = True
    return is_eon


def get_date_format(date):
    if "." in date:
        return "%Y-%m-%d %H:%M:%S.%f"
    elif ":" in date:
        return "%Y-%m-%d %H:%M:%S"
    else:
        return "%Y-%m-%d"
