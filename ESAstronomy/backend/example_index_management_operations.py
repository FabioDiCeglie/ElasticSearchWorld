from pprint import pprint
"""
Example script demonstrating Elasticsearch index management operations.

This module shows how to:
1. Connect to an Elasticsearch cluster
2. Retrieve client information
3. Delete an existing index (if it exists)
4. Create a new index with custom shard and replica settings

Shards and Replicas:
- Shards: Horizontal partitions of an index that allow data to be distributed across
    multiple nodes in a cluster. Each shard is a separate Lucene index. More shards
    enable better parallelization and distribution of data and search operations.
    
- Replicas: Copies of primary shards that provide redundancy and fault tolerance.
    They also improve search performance by allowing read operations to be distributed
    across multiple copies. Replicas are never allocated on the same node as their
    primary shard.

Configuration used:
- number_of_shards: 3 (divides the index data into 3 primary shards)
- number_of_replicas: 2 (creates 2 replica copies of each primary shard)
- Total shards in cluster: 9 (3 primary + 6 replicas)
"""
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
client_info = es.info()
print("Elasticsearch Client Info:")
pprint(client_info.body)

es.indices.delete(index='my_index', ignore_unavailable=True)
es.indices.create(index='my_index', settings={
        "index" : {
            "number_of_shards": 3,
            "number_of_replicas": 2
        }
    }
)

# Check the indices created
indices_response = es.indices.get(index='my_index')
print("Index 'my_index' details:")
pprint(indices_response.body)