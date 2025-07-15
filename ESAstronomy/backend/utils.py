import time

from elasticsearch import Elasticsearch


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