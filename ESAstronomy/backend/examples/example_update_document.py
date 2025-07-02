from elasticsearch import Elasticsearch
import json
from tqdm import tqdm

es = Elasticsearch("http://localhost:9200")
client_info = es.info()

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', settings={
    "index": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    }
}
)

document_ids = []
dummy_data = json.load(open('../../data/dummy_data.json'))
for document in tqdm(dummy_data, total=len(dummy_data)):
    response = es.index(index='my_index', body=document, refresh='wait_for')
    print(f"Indexed document ID: {response['_id']}")
    document_ids.append(response['_id'])

response = es.update(
    index='my_index',
    id=document_ids[0],
    script={
        "source": "ctx._source.title = params.title",
        "params": {
            "title": "Updated Title"
        }
    },
)

response = es.get(index='my_index', id=document_ids[0])
print(f"Updated document: {response['_source']}")

response = es.update(
    index='my_index',
    id=document_ids[0],
    script={
        "source": "ctx._source.new_field = 'dummy_value'",
    },
)

response = es.get(index='my_index', id=document_ids[0])
print(f"Updated document: {response['_source']}")

response = es.update(
    index='my_index',
    id=document_ids[0],
    doc={
        "new_field_2": "dummy_value_2",
    },
)

response = es.get(index='my_index', id=document_ids[0])
print(f"Updated document: {response['_source']}")

response = es.update(
    index='my_index',
    id=document_ids[0],
    script={
        "source": "ctx._source.remove('title')",
    },
)

response = es.get(index='my_index', id=document_ids[0])
print(f"Updated document: {response['_source']}")

response = es.update(
    index='my_index',
    id='1',
    doc={
        "book_id": 1234,
        "book_name": "a book"
    },
    doc_as_upsert=True
)

response = es.get(index='my_index', id='1')
print(f"Created document: {response['_source']}")

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')

response = es.count(index='my_index')
print(f"Document count: {response['count']}")