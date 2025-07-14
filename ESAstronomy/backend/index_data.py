import json
from typing import List
from tqdm import tqdm
from elasticsearch import Elasticsearch

from utils import get_es_client
from config import INDEX_NAME


def index_data(documents: List[dict]):
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = create_index(es, INDEX_NAME)
    _ = insert_document(es, documents)
    print(f"Indexed {len(documents)} documents into index '{INDEX_NAME}'.")


def create_index(es: Elasticsearch, index_name: str) -> dict:
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(index=index_name, ignore_existing=True)


def insert_document(es: Elasticsearch, documents: List[dict]) -> dict:
    operations = []
    for document in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({"index": {"_index": INDEX_NAME}})
        operations.append(document)
    return es.bulk(operations=operations, refresh=True)


if __name__ == "__main__":
    with open('data/apod.json') as file:
        documents = json.load(file)

    index_data(documents=documents)