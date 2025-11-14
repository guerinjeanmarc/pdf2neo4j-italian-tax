# GitHub Release Notes

## What's Included

This release contains a complete working pipeline for building Neo4j knowledge graphs from PDF documents.

### Core Files

- **README.md** - Quick start guide and overview
- **requirements.txt** - Python dependencies
- **env.template** - Environment configuration template
- **run_pipeline.sh** - Automated pipeline execution script
- **download_pdfs.py** - Script to download PDFs from CSV

### Pipeline Code (`pipeline/`)

All core processing modules (unchanged from working version):
- PDF parsing and section extraction
- Text chunking
- Neo4j ingestion with indexes
- LLM-based extraction (legal references, topics, keywords)
- TOC extraction

### Scripts (`scripts/`)

Pipeline execution scripts:
- `process_all_documents.py` - Main PDF processing
- `generate_chunk_embeddings.py` - Vector embedding generation
- `extract_legal_references.py` - Legal citation extraction
- `link_references_to_documents.py` - Entity linking
- `extract_topics_keywords.py` - Topic/keyword extraction
- Validation and comparison utilities

### Documentation (`docs/`)

- **SCHEMA.md** - Complete graph schema reference
- **IMPROVEMENTS.md** - Suggested enhancements

### Sample Data (`data/`)

- **sample_questions.csv** - Example questions

## Setup Instructions

1. Copy `env.template` to `.env` and configure
2. Install dependencies: `pip install -r requirements.txt`
3. Download PDFs: `python download_pdfs.py`
4. Run pipeline: `./run_pipeline.sh`

## Important Notes

- This is a **rapid prototype**, not production code
- Code has been copied AS-IS from working implementation
- Designed for Italian tax documents but adaptable
- Requires Neo4j Aura instance and OpenAI API key


## Next Steps After Cloning

1. Review README.md for quick start
2. Configure .env with your credentials
3. Prepare your PDF documents and CSV
4. Run the pipeline
5. Explore the graph in Neo4j Browser
6. Build your RAG agent tools

## Support

This is a reference implementation. Users should adapt the code to their specific use case and requirements. See IMPROVEMENTS.md for enhancement suggestions.

