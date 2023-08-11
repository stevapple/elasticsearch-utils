#!/usr/bin/env python3
import argparse
import asyncio
import json
import subprocess
import sys
from typing import AsyncIterator
from elastic_transport import NodeConfig
from elastic_transport._models import DEFAULT
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_scan


async def process_query(query_file: str, es: AsyncElasticsearch, chunk_size: int, index: str) -> AsyncIterator[dict]:
    if query_file == '-':
        query_source = sys.stdin
    else:
        query_source = open(query_file, 'r')

    try:
        query = json.load(query_source)
        async for result in async_scan(
                es,
                query=query,
                scroll='20m',
                size=chunk_size,
                index=index
        ):
            yield result
    finally:
        if query_file != '-':
            query_source.close()


async def post_process_document(document: dict, command: str, encoding: str) -> str:
    proc = await asyncio.create_subprocess_shell(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE
    )

    input_json = json.dumps(document)
    proc.stdin.write(input_json.encode(encoding))
    proc.stdin.close()

    output_bytes = await proc.stdout.read()

    await proc.wait()

    output = output_bytes.decode(encoding).strip()
    return output


async def process_results(results: AsyncIterator[dict], post_process: str, encoding: str,
                          output_file: str, full: bool) -> None:
    output_target = sys.stdout if output_file is None else open(output_file, 'w', encoding=encoding)

    try:
        async for result in results:
            processed_result = result if full else result['_source']
            if post_process:
                processed_output = await post_process_document(processed_result, post_process, encoding)
            else:
                processed_output = json.dumps(processed_result, ensure_ascii=False)

            if processed_output:
                print(processed_output, file=output_target)

    finally:
        if output_file is not None:
            output_target.close()


async def main(query_file: str, post_process: str, output_file: str, full: bool, encoding: str,
               host: str, port: int, username: str, password: str, use_ssl: bool, ca_cert: str,
               chunk_size: int, index: str = None) -> None:
    if username != 'elastic' and password is not None:
        raise ValueError("Username and password must be provided together.")

    if ca_cert is not None and not use_ssl:
        raise ValueError("CA certificate can only be used with HTTPS.")

    # Create Elasticsearch client
    es = AsyncElasticsearch(
        hosts=[
            NodeConfig(scheme='https' if use_ssl else 'http', host=host, port=port)
        ],
        http_auth=(username, password) if username and password else None,
        ca_certs=ca_cert if ca_cert is not None else DEFAULT
    )

    results = process_query(query_file, es, chunk_size, index)
    await process_results(results, post_process, encoding, output_file, full)

    await es.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Query, process and save data from Elasticsearch')
    parser.add_argument('query_file', type=str, help='Path to the query file. Use - for stdin.')
    parser.add_argument('--post-process', default=None, type=str,
                        help='Specify a command for post-processing each document')
    parser.add_argument('-o', '--out', default=None, type=str, help='Specify the output file')
    parser.add_argument('--full', action='store_true', help='Include the full document')
    parser.add_argument('--file-encoding', default='utf-8', help='Specify the encoding for file output')
    parser.add_argument('--host', default='localhost', help='Elasticsearch host (default: localhost)')
    parser.add_argument('--port', default=9200, help='Elasticsearch port (default: 9200)')
    parser.add_argument('-u', '--username', default='elastic', help='Username for authentication')
    parser.add_argument('-p', '--password', default=None, type=str, help='Password for authentication')
    parser.add_argument('--insecure', action='store_true', help='Use plain HTTP instead of HTTPS')
    parser.add_argument('--ca-cert', default=None, type=str, help='Path to the CA certificate file')
    parser.add_argument('--chunk-size', default=1000, help='Number of documents to process at once (default: 1000)')
    parser.add_argument('-i', '--index', default=None, type=str, help='Specify the Elasticsearch index')
    args = parser.parse_args()

    asyncio.run(main(
        args.query_file,
        args.post_process,
        args.out,
        args.full,
        args.file_encoding,
        args.host,
        args.port,
        args.username,
        args.password,
        not args.insecure,
        args.ca_cert,
        args.chunk_size,
        args.index
    ))
