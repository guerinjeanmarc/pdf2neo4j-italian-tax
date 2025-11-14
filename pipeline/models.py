"""
Data models for document processing pipeline.
"""
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import date


@dataclass
class Document:
    """Represents a tax document (Circolare, Risoluzione, Risposta)."""
    documentId: str
    type: str  # Circolare, Risoluzione, Risposta
    number: str
    year: int
    title: str
    oggetto: str
    publicationDate: Optional[date]
    url: str
    pageCount: int
    fullText: str
    metadata: dict = field(default_factory=dict)
    
    # Will be populated later
    fullTextEmbedding: Optional[List[float]] = None


@dataclass
class TableOfContents:
    """Represents the Table of Contents extracted from a document."""
    tocId: str
    documentId: str
    rawText: str  # Full TOC text as extracted
    startPage: int  # Where TOC starts (typically page 2 for Circolare)
    endPage: int  # Where TOC ends (page before PREMESSA or first section)
    hasHeader: bool  # Whether TOC has INDICE/SOMMARIO header
    headerText: Optional[str]  # TOC header if present
    entryCount: int  # Number of TOC entries detected
    detectionMethod: str  # "PREMESSA" or "FIRST_SECTION" for debugging


@dataclass
class Section:
    """Represents a section within a document."""
    sectionId: str
    documentId: str
    sectionNumber: str  # e.g., "2.1", "OGGETTO"
    title: str
    content: str
    sectionType: str  # OGGETTO, QUESITO, PARERE, numbered, etc.
    level: int  # Hierarchy level (1, 2, 3...)
    pageNumber: int
    order: int  # Sequential order in document
    
    # For hierarchical structure
    parentSectionId: Optional[str] = None
    
    # Will be populated later
    embedding: Optional[List[float]] = None


@dataclass
class Chunk:
    """Represents a text chunk for vector search."""
    chunkId: str
    documentId: str
    sectionId: Optional[str]
    content: str
    chunkIndex: int  # Index within section/document
    pageNumber: int
    metadata: dict = field(default_factory=dict)
    
    # Will be populated later
    embedding: Optional[List[float]] = None


@dataclass
class LegalReference:
    """Represents a legal reference extracted from text."""
    referenceId: str
    type: str  # decreto_legislativo, decreto_legge, legge, circolare, risoluzione, altro
    citation: str  # Full citation text
    number: Optional[str] = None  # Law/decree number
    year: Optional[str] = None  # Year as 4-digit string (e.g., "2011", "2024")
    article: Optional[str] = None  # Article number if mentioned
    description: Optional[str] = None  # Brief description from context
    sourceChunkId: Optional[str] = None  # Chunk where it was found


@dataclass
class Topic:
    """Represents a tax topic extracted from a section."""
    topicId: str
    name: str  # e.g., "Ecobonus", "Superbonus", "Credito d'imposta"
    description: str  # Brief description
    normalizedName: str  # Lowercase, normalized for deduplication
    
    # Will be populated later
    topicEmbedding: Optional[List[float]] = None


@dataclass
class Keyword:
    """Represents a keyword extracted from a section."""
    keywordId: str
    name: str  # e.g., "pannelli solari", "caldaie"
    normalizedName: str  # Lowercase, normalized for deduplication


@dataclass
class ProcessingResult:
    """Result of processing a single document."""
    success: bool
    documentId: Optional[str] = None
    filename: str = ""
    error: Optional[str] = None
    sections_count: int = 0
    chunks_count: int = 0
    processing_time: float = 0.0
    toc_extracted: bool = False
    toc_pages: int = 0

