import json
from typing import List
from tqdm import tqdm
from elasticsearch import Elasticsearch

from config import INDEX_NAME_RAW
from utils import get_es_client


def index_data(index_name: str, documents: List[dict]) -> None:
    pipeline_id = 'apod_pipeline'

    es = get_es_client(max_retries=5, sleep_time=5)
    _ = create_pipeline(es, pipeline_id=pipeline_id)
    _ = create_index(es, index_name=index_name)
    _ = insert_document(es, index_name=index_name, documents=documents, pipeline_id=pipeline_id)
    print(f"Indexed {len(documents)} documents into index '{index_name}'.")


def create_index(es: Elasticsearch, index_name: str) -> dict:
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(index=index_name)


def insert_document(es: Elasticsearch, index_name: str, documents: List[dict], pipeline_id: str) -> dict:
    operations = []
    for document in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({"index": {"_index": index_name}})
        operations.append(document)
    return es.bulk(operations=operations, refresh=True, pipeline=pipeline_id)


def create_pipeline(es: Elasticsearch, pipeline_id: str) -> dict:
    pipeline_body = {
        "description": "Pipeline that strips HTML tags from the explanation and title fields",
        "processors": [
            {
                "html_strip": {
                    "field": "explanation",
                }
            },
            {
                "html_strip": {
                    "field": "title"
                }
            }
        ]
    }
    return es.ingest.put_pipeline(id=pipeline_id, body=pipeline_body)


if __name__ == "__main__":
    with open('./apod_raw.json') as file:
        documents = json.load(file)

    index_data(index_name=INDEX_NAME_RAW, documents=documents)