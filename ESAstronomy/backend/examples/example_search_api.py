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

print("--------------------------------------------------")

# Search for documents with a specific field value using term query
response = es.search(index='my_index', body={
    "query": {
        "term": {
            "created_on": "2024-09-22"
        }
    }
})
n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents with created_at = '2024-09-22'")
retrieved_docs = response['hits']['hits']
print(f"Retrieved documents: {retrieved_docs}")

print("--------------------------------------------------")

# Search for any document that contains the word document in the text field
response = es.search(index='my_index', body={
    "query": {
        "match": {
            "text": "document"
        }
    }
})
n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents containing the word 'document' in the text field")
retrieved_docs = response['hits']['hits']
print(f"Retrieved documents: {retrieved_docs}")

print("--------------------------------------------------")

# Search for documents that were created before 2024-09-23 using range query
response = es.search(index='my_index', body={
    "query": {
        "range": {
            "created_on": {
                "lte": "2024-09-23"
            }
        }
    }
})
n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents created before 2024-09-23 using range query")
retrieved_docs = response['hits']['hits']
print(f"Retrieved documents: {retrieved_docs}")

print("--------------------------------------------------")

# Search for documents that meet the following criteria:
# - created_on is 2024-09-24
# Have the word third in the text field
response = es.search(index='my_index', body={
    "query": {
        "bool": {
            "must": [
                {
                    "match": {
                        "text": "third"
                    }
                },
                {
                    "range": {
                        "created_on": {
                            "gte": "2024-09-24",
                            "lt": "2024-09-25"
                        }
                    }
                }
            ]
        }
    }
})
n_hits = response['hits']['total']['value']
print(f"Found {n_hits} documents with 'third' in text and created_on = '2024-09-24' using bool query and must clause")
retrieved_docs = response['hits']['hits']
print(f"Retrieved documents: {retrieved_docs}")

print("--------------------------------------------------")

dummy_data = json.load(open('../../data/dummy_data_2.json'))
for document in tqdm(dummy_data, total=len(dummy_data)):
    response = es.index(index='my_index', body=document)

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')

# Search for documents and add size and from parameters
# This will return 3 results, skipping the first 2
response = es.search(index='my_index', body={
    "query": {
        "match_all": {}
    },
    "size": 3,
    "from": 2  # Skip the first 2 results
})
for hit in response['hits']['hits']:
    print("Retrieved the first 3 results but skip the first 2", hit["_source"])

print("--------------------------------------------------")

# Search for documents with a timeout
response = es.search(index='my_index', body={
    "query": {
        "match": {
            "text": "document"
        }
    },
    "timeout": "2s",  # Set a timeout of 2 seconds for the search
})

print(f"Search completed with timeout. Took {response['took']} ms")

print("--------------------------------------------------")

# Search average age across all documents
response = es.search(index='my_index', body={
    "query": {
        "match_all": {}
    },
    "aggs": {
        "avg_age": {
            "avg": {
                "field": "age"  # Assuming 'age' is a field in your documents
            }
        }   
    }
})

average_age = response['aggregations']['avg_age']['value']
print(f"Average age across all documents: {average_age}")

print("--------------------------------------------------")


response = es.search(index='my_index', body={
    "query": {
        "match": {
            "message": "important keyword" 
        }
    },
    "aggs": {
        "maxPrice": {
            "max": {
                "field": "price"
            }
        }   
    },
    "size": 5,
    "from": 0,  # Start from the first result,
    "timeout": "5s"  # Set a timeout of 5 seconds for the search
})

for hit in response['hits']['hits']:
    print("Retrieved document with message = important keyword, size = 5, from = 0, timeout = 5s :", hit["_source"])

max_price = response['aggregations']['maxPrice']['value']
print(f"Maximum price across all documents: {max_price}")

print("--------------------------------------------------")