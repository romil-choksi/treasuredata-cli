import click
import re
import unicodedata
import os

import time
from tdclient import api, client, errors, table_model, database_api

color = {
    'RED': '\033[91m',
    'BLUE': '\033[94m',
    'GREEN': '\033[92m',
    'BOLD': '\033[1m',
    'END': '\033[0m'
}


@click.command()
@click.argument("db_name")
@click.argument("table_name")
@click.option('-c', '--column', default='*', help='list of columns to be returned')
@click.option('-f', '--format', default='tsv', type=click.Choice(['csv', 'tsv']), help='output format for query result')
@click.option('-m', '--min', default='NULL', help='specify the minimum timestamp in UNIX timestamp format')
@click.option('-M', '--max', default='NULL', help='specify the maximum timestamp in UNIX timestamp format')
@click.option('-e', '--engine', default='presto', type=click.Choice(['hive', 'presto']), help='query engine to be used')
@click.option('-l', '--limit', type=click.INT, help='limit the number of rows to be returned')
@click.option('-k', '--key', envvar="TD_API_KEY", help='provide TreasureData API key')
@click.option('-k', '--endpoint', default='https://api.treasuredata.com', envvar="TD_API_SERVER", help='provide TreasureData API endpoint')
def entry_point(db_name, table_name, column, format, min, max, engine, limit, key, endpoint):
    global td_api_key, td_api_endpoint
    td_api_key = key
    td_api_endpoint = endpoint
    check_if_database_exists(db_name)
    check_if_table_exists(db_name, table_name)
    run_query(db_name, table_name, column, min, max, limit, format, engine)


def check_if_database_exists(db_name):
    try:
        api_obj = api.API(td_api_key, endpoint=td_api_endpoint)
        db_list = list(api_obj.list_databases().keys())
    except ValueError as ve:
        raise click.ClickException("%sProvide a valid API key%s" % (color.get('RED', ''), color.get('END', '')))
    except errors.AuthError as ae:
        raise click.ClickException('%sPlease provide correct authentication credentials%s' % (color.get('RED', ''), color.get('END', '')))
    except Exception as e:
        raise click.ClickException("%s%s%s" % (color.get('RED', ''), e, color.get('END', '')))
    if db_name not in db_list:
        raise click.ClickException("%sDB resource %s not found. Please provide a valid database name%s" % (color.get('RED', ''), db_name, color.get('END', '')))


def check_if_table_exists(db_name, table_name):
    try:
        api_obj = api.API(td_api_key, endpoint=td_api_endpoint)
        tables_list = list(api_obj.list_tables(db_name).keys())
    except Exception as e:
        raise click.ClickException("%s%s%s" % (color.get('RED', ''), e, color.get('END', '')))
    if table_name not in tables_list:
        raise click.ClickException("%sTable resource %s not found. Please provide a valid table name%s" % (color.get('RED', ''), table_name, color.get('END', '')))


def run_query(db_name, table_name, column, min, max, limit, format, engine):
    query = construct_query(table_name, column, min, max, limit)
    try:
        td = client.Client(td_api_key, endpoint=td_api_endpoint)
        out = td.query(db_name, query, type=engine)
        click.echo("Running query: %s %s %s" % (color.get('BOLD', ''), out.query, color.get('END', '')))
        while not out.finished():
            click.echo("%swaiting...%s" % (color.get('BLUE', ''), color.get('END', '')))
            time.sleep(5)
        assert out.success, "Failure"
        for row in td.job_result_format_each(out.job_id, format.lower()):
            click.echo(row)
    except ValueError as ve:
        raise click.ClickException("%sProvide a valid API key%s"  % (color.get('RED', ''), color.get('END', '')))
    except errors.NotFoundError as ne:
        raise click.ClickException("%sResource not found. Please provide a valid resource name%s" % (color.get('RED', ''), color.get('END', '')))
    except errors.AuthError as ae:
        raise click.ClickException('%sPlease provide correct authentication credentials%s' % (color.get('RED', ''), color.get('END', '')))
    except Exception as e:
        raise click.ClickException("%s%s%s" % (color.get('RED', ''), e, color.get('END', '')))


def construct_query(tbl_name, col_list, min_time, max_time, limit):
    query = 'select %s from %s where td_time_range(time, %s, %s)' % (col_list, tbl_name, min_time, max_time)
    if col_list != '*':
        # check if col_list is comma-separated and doesn't have special characters (list of chars to be checked can be extended)
        if re.search(r"[:@% ]", col_list) is not None:
            raise click.BadParameter(' %sPlease provide a comma-seperated columns list. Ex. col1,col2,col3%s' % (color.get('RED', ''), color.get('END', '')))
    if not limit:
        query = query + ';'
    else:
        if limit < 0:
            raise click.BadParameter('%sPlease provide correct values. limit needs to be a positive value%s' % (color.get('RED', ''), color.get('END', '')))
        query = query + ' limit %s ;' % limit
    if min_time != 'NULL' and max_time != 'NULL':
        if int(min_time) > int(max_time):
            raise click.BadParameter('%sPlease provide correct values. max_time needs to be greater than min_time%s' % (color.get('RED', ''), color.get('END', '')))
    return query


if __name__ == "__main__":
    entry_point()