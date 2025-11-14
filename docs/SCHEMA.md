# Graph Schema

This document describes the Neo4j graph schema used in this project.

## Node Types

### Document
Core document node representing Italian tax documents (Circolare, Risoluzione, Risposta).

**Properties:**
- `documentId` (KEY): Format `{TYPE}_{number}_{year}` (e.g., `CIR_20_2011`)
- `type`: Document type (Circolare, Risoluzione, Risposta)
- `number`: Document number
- `year`: Publication year (INTEGER)
- `title`: Document title
- `oggetto`: Document subject
- `url`: Source URL
- `pageCount`: Number of pages
- `metadata`: Additional metadata

### TableOfContents
Extracted table of contents for validation and quality assurance.

**Properties:**
- `tocId` (KEY): Unique identifier
- `documentId`: Parent document ID
- `rawText`: Raw TOC text
- `startPage`: Starting page number
- `endPage`: Ending page number
- `hasHeader`: Boolean indicating header presence
- `headerText`: Header text if present
- `entryCount`: Number of TOC entries
- `detectionMethod`: Method used for extraction

### Section
Hierarchical sections within documents.

**Properties:**
- `sectionId` (KEY): Unique identifier
- `documentId`: Parent document ID
- `sectionNumber`: Section number (e.g., "1.2.3")
- `title`: Section title
- `content`: Full section text
- `sectionType`: Type of section
- `level`: Hierarchy level
- `order`: Order within parent
- `pageNumber`: Starting page

### Chunk
Text chunks for semantic search (created with section-aware boundaries).

**Properties:**
- `chunkId` (KEY): Unique identifier
- `documentId`: Parent document ID
- `sectionId`: Parent section ID
- `content`: Chunk text (indexed for fulltext search)
- `embedding`: Vector embedding (1536 dimensions LIST)
- `chunkIndex`: Order within section
- `pageNumber`: Page number
- `metadata`: Additional metadata

### Topic
High-level tax topics extracted by LLM.

**Properties:**
- `topicId` (KEY): Unique identifier
- `name`: Topic name
- `description`: Topic description
- `normalizedName`: Normalized version for matching
- `created`: Boolean flag

### Keyword
Specific keywords/subtopics extracted by LLM.

**Properties:**
- `keywordId` (KEY): Unique identifier
- `name`: Keyword name
- `normalizedName`: Normalized version for matching
- `created`: Boolean flag

### LegalReference
Legal citations extracted from chunks (laws, decrees, circolari, etc.).

**Properties:**
- `referenceId` (KEY): Canonical ID matching Document format
- `type`: Reference type (circolare, decreto_legislativo, etc.) - INDEXED
- `citation`: Full citation text
- `number`: Law/decree number
- `year`: Year (STRING)
- `articleNumber`: Specific article if referenced
- `description`: Brief description

## Relationships

### Document Structure
- `HAS_TOC`: Document → TableOfContents
- `HAS_SECTION`: Document → Section (with `order` property)
- `HAS_SUBSECTION`: Section → Section (for hierarchy, with `order` property)
- `HAS_CHUNK`: Section → Chunk (with `order` property)

### Sequential Navigation
- `NEXT_SECTION`: Section → Section (reading order, with `order` property)
- `NEXT_CHUNK`: Chunk → Chunk (context expansion, with `order` property)

### Semantic Enrichment
- `REFERENCES_LAW`: Chunk → LegalReference (legal citations in text)
- `REFERS_TO`: LegalReference → Document (entity linking)
- `DISCUSSES_TOPIC`: Section → Topic (with `relevanceScore` FLOAT)
- `DISCUSSES_KEYWORD`: Section → Keyword (with `relevanceScore` FLOAT)

## Indexes

### Vector Index
- **Name**: `chunk_embedding_vector`
- **Property**: `Chunk.embedding`
- **Dimensions**: 1536
- **Similarity**: Cosine

### Fulltext Index
- **Name**: `chunk_content_fulltext`
- **Property**: `Chunk.content`

### Property Indexes
- `Document.documentId`
- `Document.type`
- `Document.year`
- `Section.sectionId`
- `Section.sectionType`
- `Chunk.chunkId`
- `Chunk.content`
- `Topic.topicId`
- `Topic.normalizedName`
- `Keyword.keywordId`
- `Keyword.normalizedName`
- `LegalReference.referenceId`
- `LegalReference.type`
- `TableOfContents.tocId`

## Current Database Statistics

Based on actual database (~128 PDF files):

### Nodes
- **Documents**: 67
- **TableOfContents**: 27
- **Sections**: 1,136
- **Chunks**: 5,144 (all with embeddings)
- **Topics**: 3,130
- **Keywords**: 7,662
- **Legal References**: 2,135
- **Total Nodes**: ~19,301

### Relationships
- **HAS_TOC**: 27
- **HAS_SECTION**: 1,136
- **HAS_SUBSECTION**: 335
- **HAS_CHUNK**: 5,144
- **NEXT_SECTION**: 1,069
- **NEXT_CHUNK**: 4,008
- **REFERENCES_LAW**: 4,842
- **REFERS_TO**: 16
- **DISCUSSES_TOPIC**: 4,258
- **DISCUSSES_KEYWORD**: 13,058
- **Total Relationships**: ~33,893
