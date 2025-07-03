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
es.indices.delete(index='my_index_2', ignore_unavailable=True)
es.indices.create(index='my_index_2')

dummy_data = json.load(open('../../data/dummy_data.json'))
for document in tqdm(dummy_data, total=len(dummy_data)):
    response = es.index(index='my_index', body=document)

for document in tqdm(dummy_data, total=len(dummy_data)):
    response = es.index(index='my_index_2', body=document)

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')
es.indices.refresh(index='my_index_2')

response = es.search(index='my_index', body={
    "query": {
        "match_all": {}
        }
    })

n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents in my_index")

# Same result as above
response = es.search(index='my_index')

n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents in my_index")

# --------------------------------------------

# Search across multiple indices - pass as a list
response = es.search(index=['my_index', 'my_index_2'])
n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents across both indices")

# Alternative: Search across multiple indices - pass as comma-separated string 
# (no spaces)
response = es.search(index='my_index,my_index_2')
n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents across both indices (alternative syntax)")

response = es.search(index='_all')
n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents across all indices")