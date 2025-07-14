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

response = es.indices.exists(index='my_index')
print(response.body)

response = es.indices.exists(index='my_index_2')
print(response.body)

response = es.exists(index='my_index', id=document_ids[0])
print(response.body)

response = es.exists(index='my_index', id='do not exist')
print(response.body)