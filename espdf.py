import base64
import os
import pathlib
import sys

from elasticsearch import Elasticsearch
from elasticsearch.client.ingest import IngestClient
from elasticsearch_dsl import Index, DocType, Attachment, Text
from elasticsearch_dsl.query import MultiMatch


ELASTICSEARCH_ENDPOINT = os.environ['ELASTICSEARCH_ENDPOINT']
client = Elasticsearch(hosts=[ELASTICSEARCH_ENDPOINT])
index = Index('pdf-search-example', using=client)


@index.doc_type
class Document(DocType):

    source_file = Attachment()


def create_index():
    p = IngestClient(client)
    p.put_pipeline(id='document_attachment', body={
        'description': "Extract attachment information",
        'processors': [{
            "attachment": {
                "field": "source_file"
            }
        }]
    })
    index.doc_type(Document)
    index.create()


def delete_index():
    index.delete()


def save_docs():
    concepts = pathlib.Path(sys.argv[1])
    for concept in concepts.glob('**/*.pdf'):
        print(concept)
        doc = Document()
        with concept.open('rb') as f:
            doc.source_file = base64.b64encode(f.read()).decode('ascii')
        doc.save(using=client, pipeline='document_attachment')


def search():
    search_term = ' '.join(sys.argv[1:])
    if not search_term:
        print('No search term', file=sys.stderr)
        sys.exit(1)
    s = Document.search(using=client)
    query = MultiMatch(query=search_term, fields=['attachment.content'])
    s = s.query(query)
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


search()
