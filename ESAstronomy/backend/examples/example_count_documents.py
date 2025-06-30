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

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')

response = es.count(index='my_index')
count = response['count']
print(f"Total documents indexed in 'my_index': {count}")

query = {
    "range": {
        "created_on": {
            "gte": "2024-01-01",
            "lte": "2024-12-31",
            "format": "yyyy-MM-dd"
        }
    }
}

response = es.count(index='my_index', query=query)
count = response['count']
print(f"Documents created in 2024: {count}")