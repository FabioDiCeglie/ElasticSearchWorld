# We use kNN search for fields mapped as dense_vector in Elasticsearch.

from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import torch
import json
from tqdm import tqdm

es = Elasticsearch("http://localhost:9200")
client_info = es.info()

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index',
                  settings={
                      "index": {
                          "number_of_shards": 3,
                          "number_of_replicas": 2
                      }
                  },
                  mappings={
                      "properties": {
                          "embedding": {
                              "type": "dense_vector",
                              "dims": 384  # all-MiniLM-L6-v2 produces 384-dimensional vectors
                          }
                      }
                  }
                  )

model = SentenceTransformer('all-MiniLM-L6-v2')

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

model = model.to(device)

documents = json.load(open('./data/knn_search.json'))


def get_embedding(text):
    return model.encode(text)


operations = []
for document in tqdm(documents, total=len(documents)):
    operations.append({
        "index": {"_index": "my_index"}})
    operations.append(
        {**document, "embedding": get_embedding(document['content'])})

response = es.bulk(operations=operations, refresh='wait_for')
print(f"Indexed {len(documents)} documents with embeddings")

print("--------------------------------------------------")

response = es.search(index='my_index', body={
    "query": {
        "match_all": {}
    }})

print(f"Found: {len(response['hits']['hits'])} documents")

print("--------------------------------------------------")

query = "What is a black hole?"
embedded_query = get_embedding(query)

response = es.search(index='my_index', knn={
    "field": "embedding",
    "query_vector": embedded_query,
    "num_candidates": 5,
    "k": 3,
}
)
print(f"Found {len(response['hits']['hits'])} documents for query '{query}':")
print("--------------------------------------------------")
hits = response['hits']['hits']
for hit in hits:
    print(f"Title: {hit['_source']['title']}")
    print(f"Content: {hit['_source']['content']}")
    print(f"Score: {hit['_score']}")
    print("*"*10)

print("--------------------------------------------------")

query = "How do we find exoplanets?"
embedded_query = get_embedding(query)

response = es.search(index='my_index', knn={
    "field": "embedding",
    "query_vector": embedded_query,
    "num_candidates": 5,
    "k": 3,
}
)
print(f"Found {len(response['hits']['hits'])} documents for query '{query}':")
print("--------------------------------------------------")
hits = response['hits']['hits']
for hit in hits:
    print(f"Title: {hit['_source']['title']}")
    print(f"Content: {hit['_source']['content']}")
    print(f"Score: {hit['_score']}")
    print("*"*10)

print("--------------------------------------------------")