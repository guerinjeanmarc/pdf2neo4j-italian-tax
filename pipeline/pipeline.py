"""
Main document processing pipeline orchestrator.
"""
import logging
import time
from pathlib import Path
from typing import List, Optional

from .models import ProcessingResult
from .config import Config
from .pdf_parser import PDFParser
from .toc_extractor import TOCExtractor
from .section_extractor import SectionExtractor
from .chunker import Chunker
from .neo4j_ingester import Neo4jIngester

logger = logging.getLogger(__name__)


class DocumentProcessingPipeline:
    """Orchestrates the document processing workflow."""
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize the pipeline with all components.
        
        Args:
            config: Configuration object (uses default if None)
        """
        self.config = config or Config()
        
        # Initialize components
        self.pdf_parser = PDFParser()
        self.toc_extractor = TOCExtractor()
        self.section_extractor = SectionExtractor()
        self.chunker = Chunker(
            chunk_size=self.config.CHUNK_SIZE,
            overlap=self.config.CHUNK_OVERLAP
        )
        self.neo4j_ingester = Neo4jIngester()
        
        logger.info("Document Processing Pipeline initialized")
    
    def setup(self):
        """Setup database constraints and indexes."""
        logger.info("Setting up Neo4j constraints...")
        self.neo4j_ingester.create_constraints()
        logger.info("Setup complete")
    
    def process_document(self, pdf_path: Path) -> ProcessingResult:
        """
        Process a single PDF document.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            ProcessingResult with success status and metrics
        """
        start_time = time.time()
        
        try:
            logger.info(f"Processing: {pdf_path.name}")
            
            # Stage 1: Parse PDF
            logger.debug("  Stage 1: Parsing PDF...")
            document = self.pdf_parser.parse(pdf_path)
            
            # Stage 2: Extract TOC (if applicable)
            logger.debug("  Stage 2: Extracting TOC...")
            toc = self.toc_extractor.extract_toc(document, pdf_path)
            toc_extracted = toc is not None
            toc_pages = (toc.endPage - toc.startPage + 1) if toc else 0
            
            # Stage 3: Extract sections (filtered by TOC)
            logger.debug("  Stage 3: Extracting sections...")
            sections = self.section_extractor.extract_sections(document, toc=toc)
            
            # Stage 4: Create chunks
            logger.debug("  Stage 4: Creating chunks...")
            all_chunks = []
            for section in sections:
                chunks = self.chunker.chunk_section(section)
                all_chunks.extend(chunks)
            
            # Stage 5: Ingest to Neo4j
            logger.debug("  Stage 5: Ingesting to Neo4j...")
            success = self.neo4j_ingester.ingest_document(
                document=document,
                sections=sections,
                chunks=all_chunks,
                toc=toc
            )
            
            processing_time = time.time() - start_time
            
            if success:
                toc_info = f", TOC: {toc_pages} pages" if toc_extracted else ""
                logger.info(f"✓ Completed {pdf_path.name} in {processing_time:.2f}s "
                          f"({len(sections)} sections, {len(all_chunks)} chunks{toc_info})")
                return ProcessingResult(
                    success=True,
                    documentId=document.documentId,
                    filename=pdf_path.name,
                    sections_count=len(sections),
                    chunks_count=len(all_chunks),
                    processing_time=processing_time,
                    toc_extracted=toc_extracted,
                    toc_pages=toc_pages
                )
            else:
                logger.error(f"✗ Failed to ingest {pdf_path.name}")
                return ProcessingResult(
                    success=False,
                    filename=pdf_path.name,
                    error="Ingestion failed",
                    processing_time=processing_time
                )
                
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"✗ Error processing {pdf_path.name}: {e}", exc_info=True)
            return ProcessingResult(
                success=False,
                filename=pdf_path.name,
                error=str(e),
                processing_time=processing_time
            )
    
    def process_directory(
        self, 
        pdf_directory: Path, 
        limit: Optional[int] = None,
        pattern: str = "*.pdf"
    ) -> List[ProcessingResult]:
        """
        Process all PDFs in a directory.
        
        Args:
            pdf_directory: Path to directory containing PDFs
            limit: Maximum number of documents to process (None = all)
            pattern: Glob pattern for PDF files
            
        Returns:
            List of ProcessingResult objects
        """
        pdf_files = list(pdf_directory.glob(pattern))
        
        if limit:
            pdf_files = pdf_files[:limit]
        
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        results = []
        for i, pdf_path in enumerate(pdf_files, 1):
            logger.info(f"\n[{i}/{len(pdf_files)}] Processing: {pdf_path.name}")
            result = self.process_document(pdf_path)
            results.append(result)
        
        return results
    
    def print_summary(self, results: List[ProcessingResult]):
        """Print a summary of processing results."""
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        
        total_sections = sum(r.sections_count for r in successful)
        total_chunks = sum(r.chunks_count for r in successful)
        total_time = sum(r.processing_time for r in results)
        
        print("\n" + "=" * 80)
        print("PROCESSING SUMMARY")
        print("=" * 80)
        print(f"Total documents:    {len(results)}")
        print(f"Successful:         {len(successful)}")
        print(f"Failed:             {len(failed)}")
        print(f"Total sections:     {total_sections}")
        print(f"Total chunks:       {total_chunks}")
        print(f"Total time:         {total_time:.2f}s")
        print(f"Avg time/doc:       {total_time/len(results):.2f}s")
        
        if failed:
            print(f"\nFailed documents:")
            for result in failed:
                print(f"  - {result.filename}: {result.error}")
        
        print("=" * 80)
    
    def close(self):
        """Cleanup resources."""
        self.neo4j_ingester.close()
        logger.info("Pipeline closed")

