import json
from typing import List
import torch
from tqdm import tqdm
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

from config import INDEX_NAME_EMBEDDING
from utils import get_es_client


def index_data(index_name: str, documents: List[dict], model: SentenceTransformer) -> None:
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = create_index(es, index_name=index_name, model=model)
    _ = insert_document(es, index_name=index_name, documents=documents, model=model)
    print(f"Indexed {len(documents)} documents into index '{index_name}'.")


def create_index(es: Elasticsearch, index_name: str, model: SentenceTransformer) -> dict:
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(
        index=index_name,
        mappings={
            "properties": {
                "embedding": {
                    "type": "dense_vector",
                }
            }
        },
    )


def insert_document(es: Elasticsearch, index_name: str, documents: List[dict], model: SentenceTransformer) -> dict:
    operations = []
    for document in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({"index": {"_index": index_name}})
        operations.append({
            **document, 
            "embedding": model.encode(document["explanation"])
        })
    return es.bulk(operations=operations, refresh=True)


if __name__ == "__main__":
    with open('./data_astronomy_api.json') as file:
        documents = json.load(file)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

    index_data(index_name=INDEX_NAME_EMBEDDING, documents=documents, model=model)