import argparse
import base64
import glob
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch.client.ingest import IngestClient
from elasticsearch_dsl import Index, DocType, Attachment, Text
from elasticsearch_dsl.query import MultiMatch
from elasticsearch.exceptions import RequestError, NotFoundError


ELASTICSEARCH_ENDPOINT = os.environ.get('ELASTICSEARCH_ENDPOINT')
DEFAULT_INDEX_NAME = 'doc-search'


class Document(DocType):

    source_file = Attachment()


def create_index(client, *, index_name, **kwargs):
    p = IngestClient(client)
    p.put_pipeline(id='document_attachment', body={
        'description': "Extract attachment information",
        'processors': [{
            "attachment": {
                "field": "source_file"
            }
        }]
    })

    index = Index(index_name, using=client)
    index.doc_type(Document)
    try:
        index.create()
    except RequestError:
        print(f"Index named '{index_name}' already exists", file=sys.stderr)
        sys.exit(1)


def delete_index(client, *, index_name, **kwargs):
    try:
        Index(index_name, using=client).delete()
    except NotFoundError:
        print(f"No index named '{index_name}'", file=sys.stderr)
        sys.exit(1)


def save_docs(client, *, index_name, file_glob, **kwargs):
    index = Index(index_name, using=client)
    index.doc_type(Document)
    for filename in glob.iglob(file_glob, recursive=True):
        print(filename)
        doc = Document()
        with open(filename, 'rb') as f:
            doc.source_file = base64.b64encode(f.read()).decode('ascii')
        doc.save(using=client, pipeline='document_attachment')


def search_docs(client, *, index_name, query, **kwargs):
    s = Document.search(using=client)
    if isinstance(query, list):
        query = ' '.join(query)
    es_query = MultiMatch(query=query, fields=['attachment.content'])
    s = s.query(es_query)
    s = s.source(include=['attachment.*'], exclude=['source_file'])
    s = s.highlight('attachment.content')
    results = s.execute()
    for hit in results:
        try:
            print(hit.attachment['title'])
        except KeyError:
            print('Untitled')
        for hl in hit.meta.highlight['attachment.content']:
            print(' '.join(hl.split()))


def cli():
    parser = argparse.ArgumentParser(
        description='Text search for binary files (PDF, PPT, XLS, etc) '
                    'using Elasticsearch')
    subparsers = parser.add_subparsers()
    subparsers.required = True
    subparsers.dest = 'command'

    parser.add_argument('--endpoint', default=ELASTICSEARCH_ENDPOINT)
    parser.add_argument('--index-name', '-i', default=DEFAULT_INDEX_NAME)

    create_index_parser = subparsers.add_parser(
        'create',
        aliases=['index'],
        help='Create an empty index'
    )
    create_index_parser.set_defaults(func=create_index)

    delete_index_parser = subparsers.add_parser(
        'delete',
        help='Delete an index and its documents'
    )
    delete_index_parser.set_defaults(func=delete_index)

    save_docs_parser = subparsers.add_parser(
        'upload',
        aliases=['save'],
        help='Upload and index document(s)'
    )
    save_docs_parser.add_argument('file_glob', metavar='GLOB')
    save_docs_parser.set_defaults(func=save_docs)

    search_index_parser = subparsers.add_parser(
        'search',
        aliases=['s'],
        help='Search indexed documents for a word or phrase'
    )
    search_index_parser.add_argument('query', nargs='+')
    search_index_parser.set_defaults(func=search_docs)

    args = parser.parse_args()
    if not args.endpoint:
        parser.error('No Elasticsearch endpoint specified')

    client = Elasticsearch(hosts=[ELASTICSEARCH_ENDPOINT])
    args.func(**vars(args), client=client)


if __name__ == '__main__':
    cli()
