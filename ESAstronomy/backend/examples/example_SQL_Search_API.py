from elasticsearch import Elasticsearch
import json

es = Elasticsearch("http://localhost:9200")
client_info = es.info()

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index')

operations = []
clothes_documents = json.load(open("./data/astronomy.json"))

for document in clothes_documents:
    operations.append({'index': {'_index': 'my_index'}})
    operations.append(document)

response = es.bulk(operations=operations)
# print(response.body)

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')

query = {
    "query": "SELECT title FROM my_index ORDER BY id LIMIT 5"
}

print("--------------------------------------------------")

response = es.sql.query(body=query)
for row in response['rows']:
    print(row)

print("--------------------------------------------------")

query = {
    "query": "SELECT * FROM my_index"
}


response = es.sql.query(body=query, format='json') # here in the format you can use: json, csv, txt, yaml, or table
for row in response['rows']:
    print(row)

print("--------------------------------------------------")

# Filter results using SQL
query = {
    "query": "SELECT * FROM my_index"
}

response = es.sql.query(
    body=query,
    filter={
        "term": {
            "title.keyword": "Black Holes"
        }
    },
)
print(response)

print("--------------------------------------------------")

# Pagination using SQL
query = {
    "query": "SELECT * FROM my_index ORDER BY id DESC"
}

response = es.sql.query(
    body=query,
    format='json',
    fetch_size=5,
)
print(response.body)
print("--------------------------------------------------")
while 'cursor' in response.body:
    response = es.sql.query(
        format='json',
        cursor=response.body['cursor'],
    )
    print(response.body)
    print()

print("--------------------------------------------------")

# Translate API
translate_query = {
    "query": "SELECT * FROM my_index WHERE content LIKE '%universe%' ORDER BY id DESC LIMIT 20"
}

translated_query = es.sql.translate(body=translate_query)
print(f"Translated Query: {translated_query.body}")