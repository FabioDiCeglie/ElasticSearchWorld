from elasticsearch import Elasticsearch
import json

es = Elasticsearch('http://localhost:9200')
client_info = es.info()
print('Connected to Elasticsearch!')
# print(client_info.body)

response = es.cluster.stats(human=True)
print(response["nodes"]["jvm"])

print("-----------------------")

response = es.cluster.stats(human=False)
print(response["nodes"]["jvm"])

print("-----------------------")

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index')

operations = []
index_name = 'my_index'
dummy_data = json.load(open("./data/dummy_data.json"))
for document in dummy_data:
    operations.append({'index': {'_index': index_name}})
    operations.append(document)

es.bulk(operations=operations)

# Refresh the index to make the documents searchable
es.indices.refresh(index=index_name)

response = es.search(
    index=index_name,
    body={
        "query": {
            "range": {
                "created_on": {
                    "gte": "2024-09-22||+1d/d",   # + 1 day
                    "lte": "now/d" 
                }
            }
        }
    }
)
hits = response['hits']['hits']
print(f"Found {len(hits)} documents")

print("------------------------")

response = es.search(
    index=index_name,
    body={
        "query": {
            "range": {
                "created_on": {
                    "gte": "2024-09-22||+1M/d",  # 2024-09-22 + 1 month
                    "lte": "now/d"
                }
            }
        }
    }
)
hits = response['hits']['hits']
print(f"Found {len(hits)} documents")

print("------------------------")

# Response filtering - inclusive filtering, exclusive filtering, combined filtering.

print("Inclusive filtering:")
response = es.search(
    index=index_name,
    body={
        "query": {
            "match_all": {}
        }
    },
    filter_path="hits.hits._id,hits.hits._source"  # Keep only _id and _source fields
)
print(response.body)

print("------------------------")
print("Exclusive filtering:")
response = es.search(
    index=index_name,
    body={
        "query": {
            "match_all": {}
        }
    },
    filter_path="-hits"  # Remove the hits key
)
print(response.body)

print("------------------------")
print("Combined filtering:")
response = es.search(
    index=index_name,
    body={
        "query": {
            "match_all": {}
        }
    },
    filter_path="hits.hits._id,-hits.hits._score"
)
print(response.body)

print("------------------------")
print("Flattening the response:")
response = es.indices.get_settings(
    index=index_name,
    flat_settings=True,
)
print(response.body)