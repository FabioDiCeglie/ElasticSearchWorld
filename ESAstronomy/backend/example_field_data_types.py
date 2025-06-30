from elasticsearch import Elasticsearch
import base64

es = Elasticsearch("http://localhost:9200")
client_info = es.info()
print("Elasticsearch Client Info:")
# print(client_info.body)

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', settings={
        "index" : {
            "number_of_shards": 3,
            "number_of_replicas": 2
        }
    },
    mappings={
        "properties": {
            "image_data": {
                "type": "binary"
            },
        }
    }
)

# Images type
image_path = "./test.jpg"
with open(image_path, "rb") as image_file:
    image_bytes = image_file.read()
    image_base64 = base64.b64encode(image_bytes).decode('utf-8')

document = {
    "image_data": image_base64,
}
response = es.index(index='my_index', body=document)
# print("Document indexed in 'my_index':", response)

# Object types
es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={
    "properties": {
        "author": { "properties": {
            "first_name": {"type": "text"},
            "last_name": {"type": "text"}
        }},
    }
})

document = {
    "author": {
        "first_name": "John",
        "last_name": "Doe"
    }
}
response = es.index(index='my_index', body=document)
print("Document with object type indexed in 'my_index':", response)

# Flattened objects type
es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={
    "properties": {
        "author": {
            "type": "flattened",
        }
    }
})

document = {
    "author": {
        "first_name": "John",
        "last_name": "Doe"
    }
}
response = es.index(index='my_index', body=document)
print("Document with flattened object indexed in 'my_index':", response)

# Nested objects type
es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={
    "properties": {
        "user": {
            "type": "nested",
        }
    }
})

documents = [
    {
        "first": "John",
        "last": "Doe",
    },
    {
        "first": "Jane",
        "last": "Franca",
    }
]
response = es.index(index='my_index', body={"user": documents})
print("Document with nested object indexed in 'my_index':", response)

# Text search type
es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={
    "properties": {
        "email_body": {
            "type": "text",
        },
    },
})

document = {
    "email_body": "This is a sample email body for testing text search capabilities."
}
response = es.index(index='my_index', body=document)
print("Document with text search indexed in 'my_index':", response)

# Completion suggester type
es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={
    "properties": {
        "suggest": {
            "type": "completion",
        }
    }
})

document_1 = {
    "suggest": {
        "input": ["Elasticsearch", "Search Engine"]
    }}

document_2 = {
    "suggest": {
        "input": ["OpenSearch", "Search Service"]
    }} 

response_1 = es.index(index='my_index', body=document_1)
response_2 = es.index(index='my_index', body=document_2)
print("Document with completion suggester indexed in 'my_index':", response_1)
print("Document with completion suggester indexed in 'my_index':", response_2)

# Spatial data type
es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={ "properties": {
    "location": {
        "type": "geo_point",
    }
}})

document = {
    "text": "Geopoint as an object using GeoJSON format",
    "location": {
        "type": "Point",
        "coordinates": [13.4050, 52.5200]  # Berlin coordinates
    }
}
response = es.index(index='my_index', body=document)
print("Document with spatial data indexed in 'my_index':", response)

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={ "properties": {
    "location": {
        "type": "geo_shape",
    }
}})

document_1 = {
    "text": "Geoshape as an object using GeoJSON format",
    "location": {
        "type": "LineString",
        "coordinates": [
            [-77.03653, 38.897676],  
            [-77.009051, 38.889939],
        ],
    }
}
document_2 = {
    "text": "Geoshape as an object using GeoJSON format",
    "location": {
        "type": "Polygon",
        "coordinates": [
            [
                [100, 0],
                [101, 0],
                [101, 1],
                [100, 1],
                [100, 0]
            ],
            [
                [100.2, 0.2],
                [100.8, 0.2],
                [100.8, 0.8],
                [100.2, 0.8],
                [100.2, 0.2]
            ]
        ]
    }
}
es.index(index='my_index', body=document_1)
es.index(index='my_index', body=document_2)
print("Document with geoshape indexed in 'my_index':", response)

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', mappings={
    "properties": {
        "location": {
            "type": "point",
        }
    }
})  

document = {
    "location": {
        "type": "Point",
        "coordinates": [-71.34, 41.12]
    }
}
response = es.index(index='my_index', body=document)
print("Document with point indexed in 'my_index':", response)