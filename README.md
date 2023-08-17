# elasticsearch-utils

Asynchronous data processing and import/export for Elasticsearch, written in Python.

## `es-importer`

```
usage: es-importer.py [-h] [--file-encoding FILE_ENCODING] [--scheme SCHEME]
                      [--host HOST] [--port PORT] [-u USERNAME] [-p PASSWORD]
                      [--ca-cert CA_CERT] [--pipeline PIPELINE]
                      [--no-generate-action] [--id-field ID_FIELD]
                      [-c CHUNK_SIZE] [--dry-run]
                      file_path index

Read, process and send data to Elasticsearch

positional arguments:
  file_path             Path to the input file
  index                 Name of the Elasticsearch index

options:
  -h, --help            show this help message and exit
  --file-encoding FILE_ENCODING
                        Input file encoding (default: utf-8)
  --scheme SCHEME       Elasticsearch HTTP scheme (default: https)
  --host HOST           Elasticsearch host (default: localhost)
  --port PORT           Elasticsearch port (default: 9200)
  -u USERNAME, --username USERNAME
                        Username for authentication (default: elastic)
  -p PASSWORD, --password PASSWORD
                        Password for authentication
  --ca-cert CA_CERT     Path to the CA certificate file
  --pipeline PIPELINE   Name of the Elasticsearch pipeline
  --no-generate-action  Whether to generate action lines
  --id-field ID_FIELD   Name of document ID field
  -c CHUNK_SIZE, --chunk_size CHUNK_SIZE
                        Number of lines to process at once (default: 1000)
  --dry-run             Print to stdout instead of sending to Elasticsearch
```

## `es-exporter`

```
usage: es-exporter.py [-h] [--post-process POST_PROCESS] [-o OUT] [--full]
                      [--file-encoding FILE_ENCODING] [--scheme SCHEME]
                      [--host HOST] [--port PORT] [-u USERNAME] [-p PASSWORD]
                      [--ca-cert CA_CERT] [--chunk-size CHUNK_SIZE] [-i INDEX]
                      query_file

Query, process and save data from Elasticsearch

positional arguments:
  query_file            Path to the query file. Use - for stdin.

options:
  -h, --help            show this help message and exit
  --post-process POST_PROCESS
                        Specify a command for post-processing each document
  -o OUT, --out OUT     Specify the output file
  --full                Include the full document
  --file-encoding FILE_ENCODING
                        Specify the encoding for file output
  --scheme SCHEME       Elasticsearch HTTP scheme (default: https)
  --host HOST           Elasticsearch host (default: localhost)
  --port PORT           Elasticsearch port (default: 9200)
  -u USERNAME, --username USERNAME
                        Username for authentication (default: elastic)
  -p PASSWORD, --password PASSWORD
                        Password for authentication
  --ca-cert CA_CERT     Path to the CA certificate file
  --chunk-size CHUNK_SIZE
                        Number of documents to process at once (default: 1000)
  -i INDEX, --index INDEX
                        Specify the Elasticsearch index
```
