"""
TOC (Table of Contents) extraction for Italian tax documents.
"""
import re
import fitz
from pathlib import Path
from typing import Optional
import logging

from .models import Document, TableOfContents

logger = logging.getLogger(__name__)


class TOCExtractor:
    """Extracts Table of Contents from Circolare documents."""
    
    def __init__(self):
        # Pattern for PREMESSA with page number
        self.premessa_pattern = re.compile(r'PREMESSA[\s.]+(\d+)', re.IGNORECASE | re.MULTILINE)
        
        # Pattern for first section (1.1) with page number
        self.first_section_pattern = re.compile(r'1\.1[^\d]*(\d+)', re.MULTILINE)
        
        # Pattern for INDICE/SOMMARIO headers
        self.toc_header_pattern = re.compile(r'^\s*(INDICE|SOMMARIO)\s*$', re.IGNORECASE | re.MULTILINE)
    
    def extract_toc(self, document: Document, pdf_path: Path) -> Optional[TableOfContents]:
        """
        Extracts TOC from a document if applicable.
        
        Returns:
            TableOfContents object if TOC found, None otherwise.
        """
        # Only Circolare documents have TOC
        if document.type != "Circolare":
            logger.debug(f"{document.documentId}: Not a Circolare, skipping TOC extraction")
            return None
        
        try:
            with fitz.open(pdf_path) as doc:
                if len(doc) < 2:
                    logger.warning(f"{document.documentId}: Document has < 2 pages, cannot extract TOC")
                    return None
                
                # TOC is always on page 2 for Circolare
                page_2_text = doc[1].get_text("text")
                
                # Check for INDICE/SOMMARIO header
                has_header = False
                header_text = None
                header_match = self.toc_header_pattern.search(page_2_text)
                if header_match:
                    has_header = True
                    header_text = header_match.group(1).upper()
                
                # Try to find TOC boundary
                toc_end_page = None
                detection_method = None
                
                # Method 1: Look for PREMESSA
                premessa_match = self.premessa_pattern.search(page_2_text)
                if premessa_match:
                    premessa_page = int(premessa_match.group(1))
                    
                    # Validate: check if PREMESSA actually exists on that page
                    if premessa_page <= len(doc):
                        actual_text = doc[premessa_page - 1].get_text("text")
                        if "PREMESSA" in actual_text.upper()[:500]:
                            toc_end_page = premessa_page - 1
                            detection_method = "PREMESSA"
                            logger.debug(f"{document.documentId}: TOC boundary detected via PREMESSA on page {premessa_page}")
                
                # Method 2: Fallback to first section (1.1)
                if toc_end_page is None:
                    section_match = self.first_section_pattern.search(page_2_text)
                    if section_match:
                        first_section_page = int(section_match.group(1))
                        
                        # Validate: check if section 1.1 exists on that page
                        if first_section_page <= len(doc):
                            actual_text = doc[first_section_page - 1].get_text("text")
                            if "1.1" in actual_text[:200]:
                                toc_end_page = first_section_page - 1
                                detection_method = "FIRST_SECTION"
                                logger.debug(f"{document.documentId}: TOC boundary detected via first section on page {first_section_page}")
                
                if toc_end_page is None:
                    logger.warning(f"{document.documentId}: Could not detect TOC boundary, skipping TOC extraction")
                    return None
                
                # Extract TOC raw text from page 2 to toc_end_page
                toc_raw_text = ""
                for page_num in range(1, toc_end_page):  # 0-indexed, so page 2 = index 1
                    if page_num < len(doc):
                        toc_raw_text += doc[page_num].get_text("text") + "\n"
                
                # Count TOC entries (rough estimate: lines with numbers)
                entry_count = len(re.findall(r'^\s*\d+\.', toc_raw_text, re.MULTILINE))
                
                toc = TableOfContents(
                    tocId=f"{document.documentId}_TOC",
                    documentId=document.documentId,
                    rawText=toc_raw_text.strip(),
                    startPage=2,
                    endPage=toc_end_page,
                    hasHeader=has_header,
                    headerText=header_text,
                    entryCount=entry_count,
                    detectionMethod=detection_method
                )
                
                logger.info(f"{document.documentId}: TOC extracted (pages 2-{toc_end_page}, {entry_count} entries, method: {detection_method})")
                return toc
        
        except Exception as e:
            logger.error(f"{document.documentId}: Error extracting TOC: {e}")
            return None
    
    def is_toc_page(self, page_number: int, toc: Optional[TableOfContents]) -> bool:
        """
        Checks if a given page number is within the TOC range.
        """
        if toc is None:
            return False
        return toc.startPage <= page_number <= toc.endPage

