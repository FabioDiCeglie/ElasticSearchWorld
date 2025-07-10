from elasticsearch import Elasticsearch
import json
from tqdm import tqdm

es = Elasticsearch("http://localhost:9200")
client_info = es.info()

settings = {
    "settings": {
        "analysis": {
            "filter": {
                "synonym_filter": {
                    "type": "synonym",
                    "synonyms": [
                        "car, automobile, vehicle",
                        "tv, television",
                        "smartphone, mobile, cell phone",
                        "jupyter, jupyter notebook, jupyterlab",
                        "jupiter, mars, earth, venus, mercury, saturn, uranus, neptune => planet"
                    ]
                }
            },
            "analyzer": {
                "synonym_analyzer": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "synonym_filter"
                    ]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "description": {
                "type": "text",
                "analyzer": "synonym_analyzer"
            }
        }
    }
}

index_name = "my_synonym_index"
es.indices.delete(index=index_name, ignore_unavailable=True)
response = es.indices.create(index=index_name, body=settings)
print(response.body)

operations = []
dummy_data = json.load(open("../../data/synonyms.json"))
for document in tqdm(dummy_data, total=len(dummy_data)):
    operations.append({'index': {'_index': index_name}})
    operations.append(document)

response = es.bulk(operations=operations)
# print(response.body)

# Refresh the index to make the changes searchable
es.indices.refresh(index=index_name)

query = {
    "query": {
        "match": {
            "description": "vehicle"
        }
    }
}

response = es.search(index=index_name, body=query)

print("Search Results:")
for hit in response["hits"]["hits"]:
    print(hit["_source"])

query = {
    "query": {
        "match": {
            "description": "planet"
        }
    }
}

response = es.search(index=index_name, body=query)

print("Search Results:")
for hit in response["hits"]["hits"]:
    print(hit["_source"])

# Expanding synonyms for search time only
settings = {
    "settings": {
        "analysis": {
            "filter": {
                "synonym_filter": {
                    "type": "synonym",
                    "synonyms": [
                        "car, automobile, vehicle",
                        "tv, television"
                    ]
                }
            },
            "analyzer": {
                "index_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                },
                "search_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "synonym_filter"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "description": {
                "type": "text",
                "analyzer": "index_analyzer",
                "search_analyzer": "search_analyzer"
            }
        }
    }
}

es.indices.delete(index=index_name)
response = es.indices.create(index=index_name, body=settings)
# print(response.body)

operations = []
dummy_data = json.load(open("../../data/synonyms.json"))
for document in tqdm(dummy_data, total=len(dummy_data)):
    operations.append({'index': {'_index': index_name}})
    operations.append(document)

response = es.bulk(operations=operations)
# print(response.body)

# Refresh the index to make the changes searchable
es.indices.refresh(index=index_name)

query = {
    "query": {
        "match": {
            "description": "vehicle"
        }
    }
}

response = es.search(index=index_name, body=query)

print("Search Results (Search-time synonyms):")
for hit in response["hits"]["hits"]:
    print(hit["_source"])