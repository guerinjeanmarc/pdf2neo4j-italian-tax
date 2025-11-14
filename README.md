# PDF to Neo4j for Agentic GraphRAG

A rapid prototype demonstrating PDF document processing into Neo4j knowledge graphs for agentic graphRAG applications, built on Italian tax documents.

## What It Does

Transforms PDF documents into a structured Neo4j knowledge graph with:
- **Hierarchical structure**: Documents → Sections → Chunks
- **Vector search**: Embeddings with enriched context (document + section metadata)
- **Semantic enrichment**: Extracted topics, keywords, and legal references
- **Entity linking**: Connections between references and actual documents

**Use case**: Building agentic graphRAG systems that can answer complex questions requiring multi-document reasoning.

## Quick Start

### Prerequisites
- Python 3.9+
- Neo4j Aura instance ([create free tier](https://neo4j.com/cloud/aura/))
- OpenAI API key

### Installation

```bash
# Clone repository
git clone https://github.com/guerinjeanmarc/pdf2neo4j-italian-tax.git
cd pdf2neo4j-italian-tax

# Create virtual environment with uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -r requirements.txt

# Configure environment
cp env.template .env
# Edit .env with your credentials
```

### Configuration

Edit `.env`:
```bash
# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here

# Neo4j Configuration (Aura recommended)
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password_here

# OpenAI Models
EMBEDDING_MODEL=text-embedding-3-small
EXTRACT_MODEL=gpt-4.1-mini
AGENT_MODEL=gpt-5

# Processing Configuration
CHUNK_SIZE=512
CHUNK_OVERLAP=50

# Paths
PDF_DIRECTORY=data
```

## Running the Pipeline

### 1. Download Documents

Prepare a CSV file with document URLs (see `data/sample_questions.csv` for format):

```bash
python download_pdfs.py
```

This downloads 128 PDFs to `data/` directory.
(Known issue: this script duplicates pdfs that appears multiple times in the csv)

### 2. Test on Sample Data (Recommended)

Before processing all documents, test the pipeline on a small sample :

(Known issue: if your 5 first documents are duplicates of the same pdf, this script will create 1 Document node only. If you want to create more Document nodes, just increase the limit below)

```bash
# Step 1: Process 5 documents
python scripts/process_all_documents.py --limit 5

# Step 2: Generate embeddings for those chunks
python scripts/generate_chunk_embeddings.py --parallel --max-concurrent 3

# Step 3: Extract legal references
python scripts/extract_legal_references.py --parallel --max-concurrent 5

# Step 4: Link references to documents
python scripts/link_references_to_documents.py

# Step 5: Extract topics and keywords
python scripts/extract_topics_keywords.py --parallel --max-concurrent 5
```

**Verify in Neo4j Browser:**
```cypher
// Should see ~5 documents with all relationships
MATCH (n)
RETURN labels(n)[0] as NodeType, count(n) as Count
ORDER BY Count DESC
```

If everything looks good, clear the database and run the full pipeline:

```bash
# Clear the database
python -c "
from pipeline.neo4j_ingester import Neo4jIngester
ingester = Neo4jIngester()
ingester.clear_database()
ingester.close()
print('✅ Database cleared!')
"
```

### 3. Run Full Pipeline

```bash
./run_pipeline.sh
```

**Pipeline steps:**
1. **Process PDFs**: Parse PDFs, extract sections, create chunks
2. **Generate embeddings**: Create vector embeddings with OpenAI
3. **Extract legal references**: LLM-based extraction of citations
4. **Link references**: Connect references to documents
5. **Extract topics/keywords**: LLM-based semantic tagging

### 4. Verify Results

Open Neo4j Browser and run:

```cypher
// Count nodes by type
MATCH (n)
RETURN labels(n)[0] as NodeType, count(n) as Count
ORDER BY Count DESC
```

## Architecture

### Code Structure

```
pipeline/               # Core processing modules
├── models.py          # Pydantic data models
├── config.py          # Configuration management
├── pdf_parser.py      # PDF parsing (PyMuPDF)
├── section_extractor.py  # Section detection (regex-based)
├── chunker.py         # Text chunking
├── toc_extractor.py   # Table of contents extraction
├── neo4j_ingester.py  # Neo4j connection & ingestion
├── legal_reference_extractor.py  # LLM-based extraction
└── topic_keyword_extractor.py    # LLM-based extraction

scripts/               # Pipeline scripts
├── process_all_documents.py      # Main PDF processing
├── generate_chunk_embeddings.py  # Embedding generation
├── extract_legal_references.py   # Legal citation extraction
├── link_references_to_documents.py  # Entity linking
└── extract_topics_keywords.py    # Topic/keyword extraction
```

### Key Design Decisions

**Embedding enrichment**: Chunks include document and section metadata for better retrieval context.

**Granularity**: Legal references extracted per chunk, topics/keywords per section (optimal for each type).

**LLM model**: `gpt-4.1-mini` for structured extraction (fast, cost-effective, reliable with Pydantic outputs).

**Embedding model**: `text-embedding-3-small` (1536 dimensions, excellent quality/cost ratio).

**Parallelization**: All LLM operations use async processing with configurable concurrency.

## Graph Schema

See [`docs/SCHEMA.md`](docs/SCHEMA.md) for complete schema documentation.

**Core nodes**: Document, Section, Chunk, Topic, Keyword, LegalReference, TableOfContents  
**Key relationships**: HAS_SECTION, HAS_CHUNK, DISCUSSES_TOPIC, REFERENCES_LAW, REFERS_TO

**Indexes**:
- Vector index on `Chunk.embedding` for semantic search
- Fulltext index on `Chunk.content` for keyword search

## Database Dump

A pre-populated Neo4j database dump is available for quick testing:

**Download**: [Neo4j Database Dump](https://drive.google.com/file/d/1AYRaK4SuAeQczmqvRj6EQLo0Ux8wT8Vt/view?usp=sharing)

To restore:
```bash
# Upload dump to Neo4j Aura or use neo4j-admin locally
neo4j-admin database load neo4j --from-path=/path/to/dump
```

## ⚠️ Important Notes

This is a **rapid prototype**, not production-ready code. Use as a starting point and adapt to your needs.

See [`docs/IMPROVEMENTS.md`](docs/IMPROVEMENTS.md) for detailed suggestions.

## Expected Results

After running the pipeline on ~67 PDF files:

- **~67** unique documents
- **~1,136** sections
- **~5,144** chunks (all with embeddings)
- **~3,130** topics
- **~7,662** keywords
- **~2,135** legal references
- **~19,301** total nodes
- **~33,893** total relationships

## Next Steps

Once your graph is built:

1. **Explore** the graph in Neo4j Browser
2. **Test queries** using vector and fulltext search
3. **Install the Neo4j MCP server** on Claude Desktop or Cursor
4. **Evaluate** using the sample questions on Claude Desktop or Cursor
3. **Build custom agent tools** for improved graph traversal and retrieval
4. **Implement ReAct loop** for question answering
5. **Evaluate** on your question set
