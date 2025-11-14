# Next Steps

This prototype provides a foundation for agentic GraphRAG. Here are the planned next steps based on the POC roadmap.

## Phase 1: Graph Enrichment (Critical)
Improve entity linking between LegalReference and Document

### Document Relationships Extraction
Extract semantic relationships between documents:
- **SUPERSEDES**: Documents that replace/update others (regex: "sostituisce", "abroga", "modifica")
- **CITES**: Document-level citations (extend from existing LegalReference links)
- **CLARIFIES**: Documents that clarify others (regex or LLM: "chiarisce", "precisa")

### Temporal Properties
Add time-based properties to documents:
- `validFrom`: When rule becomes effective
- `validUntil`: When rule expires (null if still valid)
- `isCurrentlyValid`: Computed property for queries

### Data Quality Validation
Implement comprehensive validation checks:
- Legal references accuracy and format
- Topics/keywords coverage and relevance
- Embeddings completeness (all chunks, correct dimensions)
- Graph completeness (no orphaned nodes)
- Section extraction validation against TOC

## Phase 2: ReAct Agent Development (Critical)

### Agent Framework
Build ReAct reasoning loop:
- Thought → Action → Observation cycle
- State management (conversation history, retrieved chunks)
- Tool orchestration
- Response generation with proper citations

### Agent Tools (8 planned)
1. `keyword_search(query, limit)` - Fulltext search on chunks
2. `semantic_search(query, limit)` - Vector similarity search
3. `get_document(document_id)` - Fetch document details
4. `get_section(section_id)` - Fetch section content
5. `find_related_documents(document_id, rel_type)` - Graph traversal (CITES, SUPERSEDES, CLARIFIES)
6. `find_by_topic(topic_name, limit)` - Topic-based filtering
7. `find_by_keyword(keyword, limit)` - Keyword-based filtering
8. `find_legal_references(reference_citation)` - Reference lookup

## Phase 3: Testing & Iteration (Critical)

### Question Answering Evaluation
- Test agent on all 55 questions from CSV
- Record answers, sources, reasoning, tool calls
- Compare with expected documents
- Calculate accuracy metrics by question type

### Agent Improvement Iterations
- Identify failure patterns
- Improve tools and schema
- Add few-shot examples
- Refine prompts for clarity and accuracy

## Phase 4: Optional Enhancements

### Topic Hierarchy
Create parent-child relationships:
- Example: "Ecobonus" → "Detrazioni Fiscali" → "Incentivi"
- Enable broader/narrower searches
- Improve topic clustering

### Entity Resolution
Merge similar topics and keywords:
- Clustering by embedding similarity
- Manual review and merge
- Consolidate duplicate concepts ("Ecobonus" vs "Eco-bonus")

### Section/Document Embeddings
Generate embeddings for higher-level nodes:
- Enable broader semantic queries
- "What are main topics in Circolare 20/2011?"
- Useful for document-level similarity

### Section Cross-References
Link sections referencing each other:
- Regex patterns for "vedi paragrafo X.Y.Z"
- Create REFERENCES_SECTION relationships
- Enable intra-document navigation

### Section Extraction Refinement
Improve accuracy:
- Better validation against TOC
- Reduce false positives
- Refine regex patterns based on document types
