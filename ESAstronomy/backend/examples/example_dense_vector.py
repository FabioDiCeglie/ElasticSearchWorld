from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
client_info = es.info()

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', settings={
    "index": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    }
},
    mappings={
    "properties": {
        "side_length": {
            "type": "dense_vector",
            "dims": 4,
        },
        "shape": {
            "type": "keyword"
        }
    }
}
)

response = es.index(index='my_index', id='1', document={
                    "shape": "square", "side_length": [5, 5, 5, 5]})
print(f"Indexed document ID: {response['_id']} created with dense vector")

# Invalid dense vector example
# This will raise an error because dense vectors must be flat arrays, not nested arrays
# The mapping expects 4 dimensions as a flat array like [5, 5, 5, 5]
# But [[5, 5], [5, 5]] is a 2D nested array which is invalid for dense_vector type
response = es.index(index='my_index', id='2', document={
                    "shape": "square", "side_length": [[5, 5], [5, 5]]})
print(f"This will fail: {response['_id']}")