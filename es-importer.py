#!/usr/bin/env python3
import argparse
import json
import asyncio
import gzip
import bz2
import zipfile
import lzma
import aiofiles
import io
import sys
import csv
from aiocsv import AsyncDictReader
from elasticsearch import AsyncElasticsearch
from typing import Any, AsyncIterator, List

async def read_lines(file_path: str, file_encoding: str) -> AsyncIterator[str]:
    supported_extensions = ['.gz', '.bz2', '.zip', '.xz']
    file_extension = file_path[file_path.rfind('.'):].lower()

    if file_path == '-':
        async with aiofiles.open(sys.stdin.readline, 'r', encoding=file_encoding) as stdin:
            async for line in stdin:
                yield line.strip()
    if file_extension in ('.json', '.jsonl'):
        async with aiofiles.open(file_path, 'r', encoding=file_encoding) as file:
            async for line in file:
                yield line.strip()
    elif file_extension in supported_extensions:
        opener_map = {
            '.gz': gzip.open,
            '.bz2': bz2.open,
            '.zip': zipfile.ZipFile,
            '.xz': lzma.open
        }
        opener = opener_map.get(file_extension)
        if opener:
            with io.open(file_path, 'rb') as file:
                with opener(file, 'rt', encoding=file_encoding) as decompressed_file:
                    for line in decompressed_file:
                        yield line.strip()
    else:
        raise ValueError("Unsupported file type. Only .csv, .json, .jsonl, .gz, .bz2, .zip, and .xz files are supported.")

async def read_jsonl(file_path: str, file_encoding: str) -> AsyncIterator[dict]:
    async for line in read_lines(file_path, encoding=file_encoding):
        yield json.loads(line)

async def read_csv(file_path: str, file_encoding: str) -> AsyncIterator[dict[str, str]]:
    async with aiofiles.open(file_path, 'r', encoding=file_encoding) as file:
        async for row in AsyncDictReader(file, quoting=csv.QUOTE_NONNUMERIC):
            yield row

async def process_stream(file_path: str, file_encoding: str, generate_action: bool, id_field: str) -> AsyncIterator[List[str]]:
    reader = read_csv if file_path.endswith('.csv') else read_jsonl
    async for obj in reader(file_path, file_encoding):
        if generate_action:
            id = None
            if id_field is not None:
                keypath = id_field.split('.')
                id = obj
                for key in keypath:
                    id = id[key]
            yield [
                process_action({ 'index' : {} }, id),
                json.dumps(obj)
            ]
        else:
            if obj.get('index'):
                yield [process_action(obj)]
            else:
                yield [json.dumps(obj)]

def process_action(obj: dict[str, Any], id: str = None) -> str:
    # Remove "_type" field from index action
    # This is deprecated in ES 7.0 and removed in 8.0
    obj['index'].pop('_type', None)
    # Add ID field
    if id is not None:
        obj['index']['_id'] = id
    return json.dumps(obj)

async def process_data(data: AsyncIterator[List[str]], chunk_size: int) -> AsyncIterator[List[str]]:
    processed_data = []
    async for lines in data:
        if len(processed_data) + len(lines) > chunk_size:
            yield processed_data
            processed_data = []
        processed_data += lines
    if processed_data:
        yield processed_data

async def send_data(data: List[str], index: str, pipeline: str, es: AsyncElasticsearch) -> None:
    actions = []
    for line in data:
        actions.append(json.loads(line))

    await es.bulk(body=actions, index=index, pipeline=pipeline)

async def process_file(file_path: str, file_encoding: str, index: str, es: AsyncElasticsearch, generate_action: bool, id_field: str, pipeline: str, chunk_size: int, dry_run: bool = False) -> None:
    data = process_stream(file_path, file_encoding, generate_action, id_field)
    async for processed_data in process_data(data, chunk_size):
        if dry_run:
            arg = '?pipeline={}'.format(pipeline) if pipeline is not None else ''
            print('PUT /{}/_bulk{}'.format(index, arg))
            for line in processed_data:
                print(line)
            print()
        else:
            await send_data(processed_data, index, pipeline, es)

async def main(file_path: str, file_encoding: str, index: str, host: str, port: int, username: str = None, password: str = None, use_ssl: bool = True, ca_cert: str = None, generate_action: bool = False, id_field: str = None, pipeline: str = None, chunk_size: int = 1000, dry_run: bool = False) -> None:
    if username is not 'elastic' and password is not None:
        raise ValueError("Username and password must be provided together.")

    if id_field is not None and not generate_action:
        raise ValueError("ID field can only be applied to generated actions.")

    if file_path.endswith('.csv') and not generate_action:
        raise ValueError("Actions must be generated for CSV file.")

    if ca_cert is not None and not use_ssl:
        raise ValueError("CA certificate can only be used with HTTPS.")

    es = AsyncElasticsearch(
        host=host,
        port=port,
        use_ssl=use_ssl,
        http_auth=(username, password) if username and password else None,
        ca_certs=ca_cert
    )
    await process_file(file_path, file_encoding, index, es, generate_action, id_field, pipeline, chunk_size, dry_run)
    await es.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Read, process and send data to Elasticsearch')
    parser.add_argument('file_path', type=str, help='Path to the input file')
    parser.add_argument('index', type=str, help='Name of the Elasticsearch index')
    parser.add_argument('--file-encoding', default='utf-8', help='Input file encoding (default: utf-8)')
    parser.add_argument('--host', default='localhost', help='Elasticsearch host (default: localhost)')
    parser.add_argument('--port', default=9200, help='Elasticsearch port (default: 9200)')
    parser.add_argument('-u', '--username', default='elastic', help='Username for authentication (default: elastic)')
    parser.add_argument('-p', '--password', default=None, type=str, help='Password for authentication')
    parser.add_argument('--insecure', action='store_true', help='Use plain HTTP instead of HTTPS')
    parser.add_argument('--ca-cert', default=None, type=str, help='Path to the CA certificate file')
    parser.add_argument('--pipeline', default=None, type=str, help='Name of the Elasticsearch pipeline')
    parser.add_argument('--no-generate-action', action='store_false', help='Whether to generate action lines')
    parser.add_argument('--id-field', default=None, type=str, help='Name of document ID field')
    parser.add_argument('-c', '--chunk_size', default=1000, help='Number of lines to process at once (default: 1000)')
    parser.add_argument('--dry-run', action='store_true', help='Print to stdout instead of sending to Elasticsearch')
    args = parser.parse_args()

    asyncio.run(main(args.file_path, args.file_encoding, args.index, args.host, args.port, args.username, args.password, not args.insecure, args.ca_cert,
                     not args.no_generate_action, args.id_field, args.pipeline, args.chunk_size, args.dry_run))
