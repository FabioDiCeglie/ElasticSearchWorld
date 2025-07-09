from elasticsearch import Elasticsearch
import psutil
from datetime import datetime
import time

es = Elasticsearch("http://localhost:9200")
client_info = es.info()

# Define an Index Lifecycle Management (ILM) policy
# This policy will automatically manage the indices in the data stream
policy = {
    "phases": {
        "hot": {  # The hot phase is for active indexing and querying
            "actions": {
                "rollover": {
                    # Create a new backing index every 5 minutes
                    "max_age": "5m",
                }
            }
        },
        "delete": {  # The delete phase removes old data
            # Wait 20 minutes after rollover before deleting the index
            "min_age": "20m",
            "actions": {
                "delete": {}
            }
        }
    }
}

# Create the ILM policy in Elasticsearch
response = es.ilm.put_lifecycle(name="cpu_usage_policy_v2", policy=policy)
# print(response.body)

# Create an index template for the data stream
response = es.indices.put_index_template(
    name="cpu_example_template",
    # This template will be applied to any new index that matches this pattern
    index_patterns=[
        "cpu_example_template*"
    ],
    # This indicates that the template is for a data stream
    data_stream={},
    template={
        "settings": {
            # Enable time series mode for optimized storage and querying
            "index.mode": "time_series",
            # Link the ILM policy to the indices created by this template
            "index.lifecycle.name": "cpu_usage_policy_v2",
        },
        "mappings": {
            "properties": {
                "@timestamp": {  # The required timestamp field for time series data
                    "type": "date"
                },
                "cpu_usage": {
                    "type": "float",
                    # 'gauge' is for metrics that can go up or down, like CPU usage
                    "time_series_metric": "gauge"
                },
                "cpu_name": {
                    "type": "keyword",
                    # 'time_series_dimension' marks this as a field to group data by
                    "time_series_dimension": True
                }
            }
        }
    },
    # A high priority ensures this template is used over default templates
    priority=500,
    meta={
        "description": "Template for CPU usage data",
    },
    # Allow the data stream to be created automatically on the first index request
    allow_auto_create=True
)
# print(response.body)

# The alias for the data stream, which is used for indexing
index_alias = "cpu_example_template"

# Continuously ingest CPU usage data into the data stream
while True:
    document = {
        "@timestamp": datetime.utcnow(),
        "cpu_usage": psutil.cpu_percent(interval=0.1),
        "cpu_name": "i7-13650HX"  # Example dimension value
    }

    # Index the document into the data stream
    # Elasticsearch will automatically route it to the correct backing index
    es.index(index=index_alias, document=document, refresh=True)
    print(f"Indexed document: {document}")
    time.sleep(1) # Wait for 1 second before indexing the next document
