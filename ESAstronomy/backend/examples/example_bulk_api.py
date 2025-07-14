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
dummy_data = json.load(open('./data/dummy_data.json'))
for document in tqdm(dummy_data, total=len(dummy_data)):
    response = es.index(index='my_index', body=document, refresh='wait_for')
    print(f"Indexed document ID: {response['_id']}")
    document_ids.append(response['_id'])

# This approach is slow and inefficient.
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
    id=document_ids[1],
    script={
        "source": "ctx._source.new_field = 'dummy_value'",
    },
)

response = es.get(index='my_index', id=document_ids[1])
print(f"Updated document: {response['_source']}")

response = es.delete(
    index='my_index',
    id=document_ids[2],
)
print(f"Deleted document ID: {document_ids[2]}")

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', settings={
    "index": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    }
}
)

response = es.bulk(
    operations=[
        { 
            "index": {
                "_index": "my_index",
                "_id": "1",
            }
        },
        # Source 1
        {
            "title": "Sample Title 1",
            "text": "This is the first sample document text.",
            "created_on": "2024-09-22"
        },
        { 
            "index": {
                "_index": "my_index",
                "_id": "2",
            }
        },
        # Source 2
        {
            "title": "Sample Title 2",
            "text": "Here is another example of a document.",
            "created_on": "2024-09-24"
        },
        { 
            "index": {
                "_index": "my_index",
                "_id": "3",
            }
        },
        # Source 3
        {
            "title": "Sample Title 3",
            "text": "The content of the third document goes here.",
            "created_on": "2024-09-24"
        },
        # Action 4
        {
            "update": {
                "_index": "my_index",
                "_id": "1",
            }
        },
        {
            "doc": {
                "title": "Updated Title"
            }
        },
        # Action 5
        {
            "update": {
                "_index": "my_index",
                "_id": "2",
            }
        },
        {
            "doc": {
                "new_field": "dummy_value"
            }
        },
        # Action 6
        {
            "delete": {
                "_index": "my_index",
                "_id": "3",
            }
        }
    ]
)
print(f"Bulk operation response: {response['errors']}")
print(f"Indexed documents: {response.body}")