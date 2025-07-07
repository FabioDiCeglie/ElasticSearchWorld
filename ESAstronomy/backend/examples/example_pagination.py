from elasticsearch import Elasticsearch
from tqdm import tqdm
import random
from datetime import datetime, timedelta
import time
import matplotlib.pyplot as plt


es = Elasticsearch("http://localhost:9200")
client_info = es.info()

es.indices.delete(index='my_index', ignore_unavailable=True)
index_name = 'my_index'
index_body = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    },
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "value": {"type": "float"},
            "category": {"type": "keyword"},
            "description": {"type": "text"},
            "id": {"type": "keyword"}
        }
    }
}
es.indices.create(index='my_index', body=index_body)

base_documents = [
    {
        "category": "A",
        "value": 100,
        "description": "First sample document"
    },
    {
        "category": "B",
        "value": 200,
        "description": "Second sample document"
    },
    {
        "category": "C",
        "value": 300,
        "description": "Third sample document"
    },
    {
        "category": "D",
        "value": 400,
        "description": "Fourth sample document"
    },
    {
        "category": "E",
        "value": 500,
        "description": "Fifth sample document"
    }
]


def generate_bulk_data(base_documents, target_size=100_000):
    documents = []
    base_count = len(base_documents)
    duplications_needed = target_size // base_count

    base_timestamp = datetime.now()

    for i in range(duplications_needed):
        for document in base_documents:
            new_doc = document.copy()
            new_doc['id'] = f"doc_{len(documents)}"
            new_doc['timestamp'] = (
                base_timestamp - timedelta(minutes=len(documents))).isoformat()
            new_doc['value'] = document['value'] + random.uniform(-10, 10)
            documents.append(new_doc)

    return documents


documents = generate_bulk_data(base_documents, target_size=100_000)
print(f"Generated {len(documents)} documents")

operations = []
for document in tqdm(documents, total=len(documents)):
    operations.append({'index': {'_index': index_name}})
    operations.append(document)

response = es.bulk(operations=operations)
print(response.body["errors"])

es.indices.refresh(index=index_name)

count = es.count(index=index_name)["count"]
print(f"Indexed {count} documents")

# From size method
response = es.search(
    index=index_name,
    body={
        "from": 0,
        "size": 10,
        "sort": [{"timestamp": {"order": "desc"}, "id": {"order": "desc"}}],
    }
)

hits = response['hits']['hits']
for hit in hits:
    print(f"ID: {hit['_source']['id']}")

# Search after method
response = es.search(
    index=index_name,
    body={
        "size": 10,
        "sort": [{"timestamp": {"order": "desc"}, "id": {"order": "desc"}}],
    }
)
hits = response['hits']['hits']
for hit in hits:
    print(f"ID: {hit['_source']['id']}")
    print(f"Sort values: {hit['sort']}")
    print()

last_sort_value = hits[-1]['sort']
response = es.search(
    index=index_name,
    body={
        "size": 10,
        "sort": [{"timestamp": {"order": "desc"}, "id": {"order": "desc"}}],
        "search_after": last_sort_value
    }
)
hits = response['hits']['hits']
for hit in hits:
    print(f"ID: {hit['_source']['id']}")
    print(f"Sort values: {hit['sort']}")
    print()


def test_from_size_pagination(es, index_name, page_size=100, max_pages=50):
    timings = []

    for page in tqdm(range(max_pages)):
        start_time = time.time()

        _ = es.search(
            index=index_name,
            body={
                "from": page * page_size,
                "size": page_size,
                "sort": [
                    {"timestamp": "desc"},
                    {"id": "desc"}
                ]
            }
        )

        end_time = time.time()
        final_time = (end_time - start_time) * 1000
        timings.append((page + 1, final_time))

    return timings


from_size_timings = test_from_size_pagination(
    es, index_name, page_size=200, max_pages=50)


def test_search_after_pagination(es, index_name, page_size=100, max_pages=50):
    timings = []
    search_after = None

    for page in tqdm(range(max_pages)):
        start_time = time.time()

        body = {
            "size": page_size,
            "sort": [
                {"timestamp": "desc"},
                {"id": "desc"}
            ]
        }

        if search_after:
            body["search_after"] = search_after

        response = es.search(
            index=index_name,
            body=body
        )

        hits = response["hits"]["hits"]
        if hits:
            search_after = hits[-1]["sort"]

        end_time = time.time()
        final_time = (end_time - start_time) * 1000
        timings.append((page + 1, final_time))

    return timings


search_after_timings = test_search_after_pagination(
    es, index_name, page_size=200, max_pages=50)


def plot_comparison(from_size_timings, search_after_timings):
    plt.figure(figsize=(12, 6))

    pages_from_size, times_from_size = zip(*from_size_timings)
    pages_search_after, times_search_after = zip(*search_after_timings)

    plt.plot(pages_from_size, times_from_size, 'b-', label='from/size')
    plt.plot(pages_search_after, times_search_after,
             'g-', label='search_after')

    plt.xlabel('Page number')
    plt.ylabel('Response time (milliseconds)')
    plt.title('Pagination performance comparison')
    plt.legend()
    plt.grid(True)
    plt.show()


plot_comparison(from_size_timings, search_after_timings)

# The search_after method performs more efficiently, especially for 
# deep pagination, due to its stable response time. In contrast, 
# from/size may be suitable for shallow pagination but becomes inefficient 
# as the page depth grows.