from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
client_info = es.info()

# Here’s a look at some frequently used ingest processors:

# Convert: Changes the data type of a field.
# Rename: Changes the name of a field.
# Set: Assigns a specified value to a field.
# HTML Strip: Strips HTML tags from a field's content.
# Lowercase: Transforms the text in a field to lowercase.
# Uppercase: Transforms the text in a field to uppercase.
# Trim: Removes whitespace from the beginning and end of a field's value.
# Split: Divides the field content into an array, using a comma , as the delimiter.
# Remove: Deletes a field from the document.
# Append: Adds a value to an array field.

document = {
    "price": "100.50",
    "old_name": "old_value",
    "description": "<p>This is a description with HTML.</p>",
    "username": "UserNAME",
    "category": "books",
    "title": "   Example Title with Whitespace   ",
    "tags": "tag1,tag2,tag3",
    "temporary_field": "This field should be removed"
}

pipeline_body = {
    "description": "Pipeline to demonstrate various ingest processors",
    "processors": [
        {
            "convert": {
                "field": "price",
                "type": "float",
                "ignore_missing": True
            }
        },
        {
            "rename": {
                "field": "old_name",
                "target_field": "new_name"
            }
        },
        {
            "set": {
                "field": "status",
                "value": "active"
            }
        },
        {
            "html_strip": {
                "field": "description"
            }
        },
        {
            "lowercase": {
                "field": "username"
            }
        },
        {
            "uppercase": {
                "field": "category"
            }
        },
        {
            "trim": {
                "field": "title"
            }
        },
        {
            "split": {
                "field": "tags",
                "separator": ","
            }
        },
        {
            "remove": {
                "field": "temporary_field"
            }
        },
        {
            "append": {
                "field": "tags",
                "value": ["new_tag"]
            }
        }
    ]
}

pipeline_id = "multi_steps_pipeline"
es.ingest.put_pipeline(id=pipeline_id, body=pipeline_body)
print(f"Pipeline '{pipeline_id}' created successfully.")

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', settings={
    "index": {
        "number_of_shards": 3,
        "number_of_replicas": 2
    }
}
)

response = es.index(index='my_index', body=document, pipeline=pipeline_id)

print(f"Document indexed with pipeline '{pipeline_id}': {response['result']}")

# Force refresh the index to make documents searchable
es.indices.refresh(index='my_index')

response = es.search(index='my_index')
hits = response['hits']['hits']

for hit in hits:
    print(f"Document ID: {hit['_id']}")
    print(f"Document Source: {hit['_source']}")
    print("-" * 40)