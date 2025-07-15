import time
from elasticsearch import Elasticsearch

from config import INDEX_NAME_DEFAULT, INDEX_NAME_N_GRAM


def get_es_client(max_retries: int = 1, sleep_time: int = 0) -> Elasticsearch:
    i = 0
    while i < max_retries:
        try:
            es = Elasticsearch('http://localhost:9200')
            print("Connected to Elasticsearch")
            print(f"Cluster info: \n {es.info()}")
            return es
        except Exception as e:
            print(f"Connection failed: {e}")
            time.sleep(sleep_time)
            i += 1
    return ConnectionError("Elasticsearch connection failed")


def get_index_name(use_n_gram_tokenizer: bool) -> str:
    return INDEX_NAME_N_GRAM if use_n_gram_tokenizer else INDEX_NAME_DEFAULT