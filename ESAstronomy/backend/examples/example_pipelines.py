from elasticsearch import Elasticsearch
from tqdm import tqdm
import random
from datetime import datetime, timedelta
import time
import matplotlib.pyplot as plt
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

response = es.ingest.put_pipeline(id='lowercase_pipeline',
                                  description='The pipeline transforms the text to lowercase',
                                  processors=[{
                                      "lowercase": {
                                          "field": "text"
                                      }
                                  }])
print(response.body)

response = es.ingest.get_pipeline(id='lowercase_pipeline')
print(response.body)

# response = es.ingest.delete_pipeline(id='lowercase_pipeline')
# print(response.body)


# response = es.ingest.simulate(
#     id='lowercase_pipeline',
#     docs=[{
#         "_index": "my_index",
#         "_id": "1",
#         "_source": {
#             "text": "HELLO WORLD"
#         }
#     }]
# )
# print(response.body)


dummy_data = json.load(open('../../data/dummy_data.json'))
for i, document in enumerate(dummy_data):
    uppercased_text = document['text'].upper()
    document['text'] = uppercased_text
    dummy_data[i] = document

operations = []
for document in dummy_data:
    operations.append({"index": {"_index": "my_index"}})
    operations.append(document)

es.bulk(operations=operations, pipeline='lowercase_pipeline')
# print(response.body)

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')

response = es.search(index='my_index')
hits = response.body['hits']['hits']

for hit in hits:
    print(hit['_source'])

# PIPELINE FAILURE

# Not handling the failure of the pipeline
response = es.ingest.put_pipeline(
    id='pipeline_1',
    description='Pipeline with multiple transformations',
    processors=[
        {
            "lowercase": {
                "field": "text",
            }
        },
        {
            "set": {
                "field": "text",
                "value": "CHANGED BY PIPELINE",
            }
        },
    ]
)

document = {
    'title': 'Sample Title 4',
    'created_on': '2024-09-25',
}

# response = es.index(
#     index='my_index',
#     pipeline='pipeline_1',
#     body=document
# )
# print(response.body)

# Handling the failure of the pipeline
response = es.ingest.put_pipeline(
    id='pipeline_2',
    description='Pipeline with multiple transformations, handling and ignoring failures',
    processors=[
        {
            "lowercase": {
                "field": "text",
                "on_failure": [
                    {
                        "set": {
                            "field": "text",
                            "value": "FAILED TO LOWERCASE",
                            "ignore_failure": True,
                        }
                    }
                ]
            }
        },
        {
            "set": {
                "field": "new_field",
                "value": "ADDED BY PIPELINE",
                "ignore_failure": True,
            }
        },
    ]
)
document = {
    'title': 'Sample Title 4',
    'created_on': '2024-09-25',
}

response = es.index(
    index='my_index',
    pipeline='pipeline_2',
    body=document
)
print(response.body)

es.indices.refresh(index='my_index')

response = es.search(index='my_index')
hits = response.body['hits']['hits']

for hit in hits:
    print(hit['_source'])