# ElasticSearchWorld

ElasticSearchWorld is a full-stack project demonstrating advanced search capabilities using ElasticSearch, with a Python FastAPI backend and a Vue.js frontend. It is designed for educational and practical exploration of ElasticSearch features, including semantic search, regular search, analyzers, pipelines, and more, with a focus on astronomy-related data.

## Project Structure
```
ESAstronomy/
  backend/    # Python FastAPI API, ElasticSearch logic, data indexing
    examples/ # Extensive ElasticSearch feature demos (see below)
  frontend/   # Vue.js SPA, search UI, gallery, pagination, filtering
```

### Backend (Python/FastAPI)
- Implements RESTful APIs for semantic and regular search (`/api/v1/semantic_search`, `/api/v1/regular_search`)
- Uses ElasticSearch for indexing and querying astronomy data
- Supports n-gram and standard tokenizers, pipelines, analyzers, synonyms, SQL API, and more
- Embedding-based semantic search using SentenceTransformers
- Data files for astronomy
- **Extensive `examples/` folder:**
  - Contains ready-to-run scripts demonstrating nearly every major ElasticSearch feature: bulk indexing, filters, analyzers, ingest processors, field types, pagination, SQL queries, synonyms, pipelines, and more
  - Ideal for learning, experimentation, and reference

### Frontend (Vue.js)
- Modern SPA for searching and browsing astronomy images and metadata
- Features: search bar, semantic/regular search toggle, year and tokenizer filters, pagination, gallery view, popovers
- Uses PrimeVue for UI components and theming
- Axios for API communication

## Installation

### Prerequisites
- Python 3.10+
- Node.js & npm
- ElasticSearch server (local or remote)

### ElasticSearch Setup
Start ElasticSearch locally (default: http://localhost:9200).
```
docker run -p 127.0.0.1:9200:9200 -d --name elasticsearch \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  -e "xpack.license.self_generated.type=trial" \
  -v "elasticsearch-data:/usr/share/elasticsearch/data" \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0
```

### Backend Setup
```bash
cd ESAstronomy/backend
brew install uv
uv venv
source venv/bin/activate
uv pip install -r requirements.txt
# Start FastAPI server (default: http://localhost:8000)
fastapi dev main.py
```

### Frontend Setup
```bash
cd ESAstronomy/frontend
npm install
npm run serve
# App runs at http://localhost:8080
```

## Usage
1. Start ElasticSearch, backend, and frontend servers.
2. Access the frontend at [http://localhost:8080](http://localhost:8080).
3. Search for astronomy images using semantic or regular search, filter by year, tokenizer, and view results in a gallery.

## API Endpoints
- `GET /api/v1/semantic_search` — Embedding-based semantic search
- `GET /api/v1/regular_search` — Keyword-based search with tokenizer options
- `GET /api/v1/get_docs_per_year_count` — Document count per year for filtering

## Example Features ( **All these features are covered in detail in the `ESAstronomy/backend/examples/` folder.** )
- Bulk indexing and updates
- Custom analyzers and tokenizers
- Ingest pipelines and processors
- SQL-like queries via ElasticSearch SQL API
- Synonym search and completion suggesters
- Pagination, filtering, and aggregation

## Data
Sample astronomy ( scraped from website ) and other domain datasets are provided in `ESAstronomy/backend/examples/data/` and `ESAstronomy/backend/apod_raw.json`.
