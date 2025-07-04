# Converting text into dense vectors and indexing them in Elasticsearch
# Use embedding models for retrieving documents based on semantic similarity or searching for specific keywords.

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

documents = json.load(open('../../data/dummy_data.json'))


def get_embedding(text):
    return model.encode(text)


operations = []
for document in tqdm(documents, total=len(documents)):
    operations.append({
        "index": {"_index": "my_index"}})
    operations.append(
        {**document, "embedding": get_embedding(document['text']).tolist()})

response = es.bulk(operations=operations, refresh='wait_for')
print(f"Indexed {len(documents)} documents with embeddings")

print("--------------------------------------------------")

response = es.search(index='my_index', body={
    "query": {
        "match_all": {}
    }})

print(f"Found: {response['hits']['hits']}")

print("--------------------------------------------------")

# Note: Get and display the index mapping to verify the structure
response = es.indices.get_mapping(index='my_index')
print(f"Mapping: {response['my_index']['mappings']}")