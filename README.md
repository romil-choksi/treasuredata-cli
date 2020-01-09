# Treasure Data CLI

Command line tool to issue a query on Treasure Data and query a database and table to retrieve the values of a specified set of columns in a specified date/time range.

##Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the requirements.

```bash
pip install -r requirements.txt
```

## Usage

Treasure Data API key will be read from environment variable TD_API_KEY, if none is given via apikey= argument passed to tdclient.Client.

Treasure Data API endpoint https://api.treasuredata.com is used by default. You can override this with environment variable TD_API_SERVER, which in turn can be overridden via endpoint= argument passed to tdclient.Client.

Example
```
python3 treasure_data_cli.py --help
Usage: treasure_data_cli.py [OPTIONS] DB_NAME TABLE_NAME

Options:
  -c, --column TEXT           list of columns to be returned
  -f, --format [csv|tsv]      output format for query result
  -m, --min TEXT              specify the minimum timestamp in UNIX timestamp
                              format
  -M, --max TEXT              specify the maximum timestamp in UNIX timestamp
                              format
  -e, --engine [hive|presto]  query engine to be used
  -l, --limit INTEGER         limit the number of rows to be returned
  -k, --key TEXT              provide TreasureData API key
  -k, --endpoint TEXT         provide TreasureData API endpoint
  --help                      Show this message and exit.

python3 treasure_data_cli.py sample_datasets www_access --format csv --limit 10 --engine presto --key <API_KEY>
```

