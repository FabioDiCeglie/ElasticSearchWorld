from elasticsearch import Elasticsearch
import json

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

operations = []
clothes_documents = json.load(open("../../data/clothes.json"))

for document in clothes_documents:
    operations.append({'index': {'_index': 'my_index'}})
    operations.append(document)

response = es.bulk(operations=operations)
# print(response.body)

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')

response = es.search(
    index="my_index",
    body={
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "brand": "adidas"
                        }
                    }
                ]
            }
        },
        "size": 100
    },
)

hits = response.body['hits']['hits']
print(f"Found {len(hits)} documents")

response = es.search(
    index="my_index",
    body={
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "brand": "adidas"
                        }
                    },
                    {
                        "term": {
                            "color": "yellow"
                        }
                    }
                ]
            }
        },
        "size": 100
    },
)

hits = response.body['hits']['hits']
print(f"Found {len(hits)} documents")
print("--------------------------------------------------")

# Post filters

response = es.search(
    index="my_index",
    body={
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "brand": "gucci"
                    }
                }
            }
        },
        "aggs": {
            "colors": {
                "terms": {
                    "field": "color.keyword"
                }
            },
            "color_red": {
                "filter": {
                    "term": {
                        "color.keyword": "red"
                    }
                },
                "aggs": {
                    "models": {
                        "terms": {
                            "field": "model.keyword"
                        }
                    }
                }
            }
        },
        "post_filter": {
            "term": {
                "color": "red"
            }
        },
        "size": 20
    }
)
# print(response.body)


colors_aggregation = response.body['aggregations']['colors']['buckets']
print(colors_aggregation)

print("--------------------------------------------------")

color_red_aggregation = response.body['aggregations']['color_red']['models']['buckets']
print(color_red_aggregation)

print("--------------------------------------------------")

hits = response.body['hits']['hits']
for hit in hits:
    print(f"Brand: {hit['_source']['brand']}")
    print(f"Color: {hit['_source']['color']}")
    print(f"Model: {hit['_source']['model']}")
    print("*" * 10)