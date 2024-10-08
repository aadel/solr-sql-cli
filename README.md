# Solr SQL CLI

Solr SQL command line that supports auto-completion and exporting query result.

![demo](https://github.com/aadel/solr-sql-cli/blob/main/media/ssc.gif)

## Requirements

The command-line requires Solr 9.x.

## Usage

The command line has two operating modes: interactive and non-interactive.

On linux. the interactive command line can be invoked by running `ssc.sh` as follows:

```
./ssc.sh collection_name
```

## Command line Options

`-c`, `--collection`: collection name, this argument is required

`-l`, `--protocol`: connection protocol, defaults value "http"

`--host`: host name, default value "solr"

`-p`, `--port`: connection port. default value 8983

`--path`: URL path, default value "solr"

`-s`, `--statement`: SQL statement to be run in non-interactive mode

`-o`, `--output-file`: query output file name

`-f`, `--output-format`: query output format. Supported formats are tabular (default) and CSV.

## Example
`python src/prompt.py -c books -s "SELECT sum(sale_price_f) s, author_s FROM books LIMIT 10" -o sale_sum.csv -f csv`

## Limitations

* Authentication is not supported
* Only tabular and CSV output formats are currently supported
