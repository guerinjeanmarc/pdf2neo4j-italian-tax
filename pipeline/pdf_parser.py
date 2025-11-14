"""
PDF parser for Italian tax documents.
"""
import re
from pathlib import Path
from typing import Optional
from datetime import datetime

try:
    import fitz  # PyMuPDF
except ImportError:
    import pymupdf as fitz

from .models import Document


class PDFParser:
    """Parse Italian tax document PDFs and extract metadata."""
    
    def __init__(self):
        # Patterns for document type detection
        self.circolare_pattern = re.compile(
            r'Circolare.*?n[.\s]*(\d+)[/\s]*([A-Z])?.*?(\d{4})', 
            re.IGNORECASE
        )
        self.risoluzione_pattern = re.compile(
            r'Risoluzione.*?n[.\s]*(\d+).*?(\d{4})', 
            re.IGNORECASE
        )
        self.risposta_pattern = re.compile(
            r'Risposta.*?n[.\s]*(\d+).*?(\d{4})', 
            re.IGNORECASE
        )
        
        # OGGETTO pattern
        self.oggetto_pattern = re.compile(
            r'OGGETTO\s*:?\s*(.+?)(?=\n\n|QUESITO|PREMESSA|\Z)',
            re.DOTALL | re.IGNORECASE
        )
    
    def parse(self, pdf_path: Path) -> Document:
        """
        Parse a PDF and extract document information.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Document object with extracted metadata and content
        """
        doc_pymupdf = fitz.open(pdf_path)
        
        try:
            # Extract full text
            full_text = ""
            for page in doc_pymupdf:
                full_text += page.get_text()
            
            # Extract metadata from filename and content
            doc_type, number, year = self._extract_metadata(pdf_path.name, full_text)
            
            # Extract OGGETTO (subject)
            oggetto = self._extract_oggetto(full_text)
            
            # Create unique document ID
            doc_id = self._create_document_id(doc_type, number, year)
            
            # Extract title (usually first few lines)
            title = self._extract_title(full_text, pdf_path.name)
            
            # Create Document object
            document = Document(
                documentId=doc_id,
                type=doc_type,
                number=number,
                year=year,
                title=title,
                oggetto=oggetto,
                publicationDate=None,  # TODO: extract from content if needed
                url="",  # TODO: can be added from CSV if needed
                pageCount=len(doc_pymupdf),
                fullText=full_text,
                metadata={
                    "filename": pdf_path.name,
                    "file_size": pdf_path.stat().st_size
                }
            )
            
            return document
            
        finally:
            doc_pymupdf.close()
    
    def _extract_metadata(self, filename: str, content: str) -> tuple[str, str, int]:
        """
        Extract document type, number, and year from filename and content.
        
        Returns:
            (doc_type, number, year)
        """
        # Try to extract from content first (more reliable)
        # Circolare
        match = self.circolare_pattern.search(content[:2000])  # Check first 2000 chars
        if match:
            number = match.group(1)
            letter = match.group(2) if match.group(2) else ""
            year = int(match.group(3))
            return ("Circolare", f"{number}/{letter}" if letter else number, year)
        
        # Risoluzione
        match = self.risoluzione_pattern.search(content[:2000])
        if match:
            number = match.group(1)
            year = int(match.group(2))
            return ("Risoluzione", number, year)
        
        # Risposta
        match = self.risposta_pattern.search(content[:2000])
        if match:
            number = match.group(1)
            year = int(match.group(2))
            return ("Risposta", number, year)
        
        # Fallback to filename parsing
        filename_lower = filename.lower()
        
        if 'circolare' in filename_lower:
            # Try to extract number from filename
            num_match = re.search(r'(\d+)', filename)
            number = num_match.group(1) if num_match else "unknown"
            
            # Try to extract year
            year_match = re.search(r'(19|20)\d{2}', filename)
            year = int(year_match.group(0)) if year_match else datetime.now().year
            
            return ("Circolare", number, year)
        
        elif 'risoluzione' in filename_lower:
            num_match = re.search(r'(\d+)', filename)
            number = num_match.group(1) if num_match else "unknown"
            year_match = re.search(r'(19|20)\d{2}', filename)
            year = int(year_match.group(0)) if year_match else datetime.now().year
            return ("Risoluzione", number, year)
        
        elif 'risposta' in filename_lower:
            num_match = re.search(r'(\d+)', filename)
            number = num_match.group(1) if num_match else "unknown"
            year_match = re.search(r'(19|20)\d{2}', filename)
            year = int(year_match.group(0)) if year_match else datetime.now().year
            return ("Risposta", number, year)
        
        # Default if can't determine
        return ("Unknown", "unknown", datetime.now().year)
    
    def _extract_oggetto(self, content: str) -> str:
        """Extract the OGGETTO (subject) from document content."""
        match = self.oggetto_pattern.search(content)
        if match:
            oggetto = match.group(1).strip()
            # Clean up: remove extra whitespace, newlines
            oggetto = re.sub(r'\s+', ' ', oggetto)
            return oggetto[:500]  # Limit length
        
        return "No OGGETTO found"
    
    def _extract_title(self, content: str, filename: str) -> str:
        """Extract document title (first significant text or from filename)."""
        lines = content.split('\n')
        
        # Find first substantial line (> 10 chars, not just numbers/spaces)
        for line in lines[:20]:  # Check first 20 lines
            line = line.strip()
            if len(line) > 10 and not line.isdigit():
                # Clean up
                title = re.sub(r'\s+', ' ', line)
                return title[:200]
        
        # Fallback to filename
        return filename
    
    def _create_document_id(self, doc_type: str, number: str, year: int) -> str:
        """Create a unique document ID with explicit type mapping."""
        # Explicit mapping to avoid collisions (Risoluzione vs Risposta)
        type_map = {
            'circolare': 'CIR',
            'risoluzione': 'RIS',
            'risposta': 'RIP'  # Not RISP, to keep 3-char pattern
        }
        
        doc_type_short = type_map.get(doc_type.lower(), doc_type[:3].upper())
        number_clean = re.sub(r'[^\w]', '_', number)
        
        return f"{doc_type_short}_{number_clean}_{year}"

