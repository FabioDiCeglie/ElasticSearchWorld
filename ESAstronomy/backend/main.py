from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from utils import get_es_client, get_index_name


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/v1/search/")
async def search(search_query: str, skip: int = 0, limit: int = 10, year: str | None = None) -> dict:
    try:
        es = get_es_client(max_retries=1, sleep_time=0)

        query = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": search_query,
                            "fields": ["title", "explanation"]
                        }
                    }
                ]
            }
        }
        if year:
            query["bool"]["filter"] = [
                {
                    "range": {
                        "date": {
                            "gte": f"{year}-01-01",
                            "lte": f"{year}-12-31",
                            "format": "yyyy-MM-dd"
                        }
                    }
                }
            ]
        response = es.search(
            index=get_index_name(use_n_gram_tokenizer=True),
            body={
                "query": query,
                "from": skip,
                "size": limit,
            },
            filter_path=["hits.hits._source", "hits.hits._score", "hits.total"]
        )
        total_hits = get_total_hits(response)
        max_pages = calculate_max_pages(total_hits, limit)

        return {
            "hits": response["hits"].get("hits", []), 
            "max_pages": max_pages
        }
    except Exception as e:
        return HTMLResponse(content=str(e), status_code=500)
    

def get_total_hits(response: dict) -> int:
    return response["hits"]["total"]["value"] if "total" in response["hits"] else 0


def calculate_max_pages(total_hits: int, limit: int) -> int:
    if limit <= 0:
        return 0
    return (total_hits + limit - 1) // limit


@app.get("/api/v1/get_docs_per_year_count/")
async def get_docs_per_year_count(search_query: str) -> dict:
    try:
        es = get_es_client(max_retries=1, sleep_time=0)
        query = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": search_query,
                            "fields": ["title", "explanation"]
                        }
                    }
                ]
            }
        }
        response = es.search(
            index=get_index_name(use_n_gram_tokenizer=True),
            body={
                "query": query,
                "aggs": {
                    "docs_per_year": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval": "year",
                            "format": "yyyy"
                        }
                    }
                }
            },
            filter_path=["aggregations.docs_per_year"]
        )
        return {"docs_per_year": extract_docs_per_year(response)}
    except Exception as e:
        return HTMLResponse(content=str(e), status_code=500)


def extract_docs_per_year(response: dict) -> dict:
    aggregations = response.get("aggregations", {})
    docs_per_year = aggregations.get("docs_per_year", {})
    buckets = docs_per_year.get("buckets", [])
    return {bucket["key_as_string"]: bucket["doc_count"] for bucket in buckets}