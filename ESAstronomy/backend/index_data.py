import json
from typing import List
from tqdm import tqdm
from elasticsearch import Elasticsearch

from utils import get_es_client, get_index_name


def index_data(index_name: str, documents: List[dict], use_n_gram_tokenizer: bool = False) -> None:
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = create_index(es, index_name=index_name, use_n_gram_tokenizer=use_n_gram_tokenizer)
    _ = insert_document(es, index_name=index_name, documents=documents)
    print(f"Indexed {len(documents)} documents into index '{index_name}'.")


def create_index(es: Elasticsearch, index_name: str, use_n_gram_tokenizer: bool) -> dict:
    tokenizer = 'n_gram_tokenizer' if use_n_gram_tokenizer else 'standard'

    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(index=index_name, body={
        "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": tokenizer,
                    }
                },
                "tokenizer": {
                    "n_gram_tokenizer": {
                        "type": "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 30,
                        "token_chars": ["letter", "digit"]
                    }
                }
            }
        }
    })


def insert_document(es: Elasticsearch, index_name: str, documents: List[dict]) -> dict:
    operations = []
    for document in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({"index": {"_index": index_name}})
        operations.append(document)
    return es.bulk(operations=operations, refresh=True)


if __name__ == "__main__":
    with open('./data_astronomy_api.json') as file:
        documents = json.load(file)

    use_n_gram_tokenizer = True
    index_name = get_index_name(use_n_gram_tokenizer)

    index_data(index_name=index_name, documents=documents, use_n_gram_tokenizer=use_n_gram_tokenizer)