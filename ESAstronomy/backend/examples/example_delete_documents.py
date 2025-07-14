from elasticsearch import Elasticsearch
import json
from tqdm import tqdm

es = Elasticsearch("http://localhost:9200")
client_info = es.info()
# print("Elasticsearch Client Info:")
# print(client_info.body)

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
    response = es.index(index='my_index', body=document)
    document_ids.append(response['_id'])

response = es.delete(index='my_index', id=document_ids[0])
print(response.body)

try:
    response = es.get(index='my_index', id='5')
except Exception as e:
    print(f"Document with ID {document_ids[0]} not found. Error: {e}")