"""
Section extraction for Italian tax documents.
"""
import re
from typing import List, Optional
from .models import Document, Section, TableOfContents
import logging

logger = logging.getLogger(__name__)


class SectionExtractor:
    """Extract hierarchical sections from document text."""
    
    def __init__(self):
        # Named section patterns (case-insensitive)
        self.named_sections = [
            r'^OGGETTO\s*:?',
            r'^QUESITO\s*:?',
            r'^SOLUZIONE\s+INTERPRETATIVA',
            r'^PARERE\s+DELL',
            r'^MOTIVAZIONE\s*:?',
            r'^CONCLUSIONE\s*:?',
            r'^INDICE\s*:?',
            r'^PREMESSA\s*:?',
            r'^RISPOSTA\s*:?',
            r'^ISTANZA\s*:?',
            r'^SOMMARIO\s*:?',
        ]
        
        # Numbered section patterns
        self.numbered_patterns = [
            r'^\d+\.\s+[A-ZÀ-Ÿ]',              # "1. TITLE" 
            r'^\d+\.\d+\s+[A-ZÀ-Ÿ]',           # "1.1 TITLE"
            r'^\d+\.\d+\.\d+\s+[A-ZÀ-Ÿ]',      # "1.1.1 TITLE"
            r'^\d+\s+[A-ZÀ-Ÿ][A-ZÀ-Ÿ\s]{10,}', # "1 LONG TITLE" (at least 10 chars of caps)
        ]
    
    def extract_sections(self, document: Document, toc: Optional[TableOfContents] = None) -> List[Section]:
        """
        Extract sections from a document.
        
        Args:
            document: Document object with fullText
            toc: Optional TableOfContents object to filter out TOC sections
            
        Returns:
            List of Section objects with hierarchical structure (TOC sections excluded)
        """
        sections = []
        lines = document.fullText.split('\n')
        
        current_section_lines = []
        current_section_start_line = 0
        current_section_header = None
        current_section_type = None
        current_page = 1
        section_counter = 0
        
        # Rough estimation: ~50 lines per page (adjust based on your PDFs)
        lines_per_page = 50
        
        for line_num, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Update page number estimation
            current_page = (line_num // lines_per_page) + 1
            
            if not line_stripped:
                continue
            
            # Check if this line is a section header
            is_section, section_type, level = self._is_section_header(line_stripped)
            
            if is_section:
                # Save previous section if exists
                if current_section_header and current_section_lines:
                    section = self._create_section(
                        document=document,
                        order=section_counter,
                        header=current_section_header,
                        content_lines=current_section_lines,
                        section_type=current_section_type,
                        page_number=current_page,
                        level=self._extract_level(current_section_header)
                    )
                    sections.append(section)
                    section_counter += 1
                
                # Start new section
                current_section_header = line_stripped
                current_section_type = section_type
                current_section_lines = []
                current_section_start_line = line_num
            else:
                # Add to current section content
                if current_section_header:
                    current_section_lines.append(line)
        
        # Add last section
        if current_section_header and current_section_lines:
            section = self._create_section(
                document=document,
                order=section_counter,
                header=current_section_header,
                content_lines=current_section_lines,
                section_type=current_section_type,
                page_number=current_page,
                level=self._extract_level(current_section_header)
            )
            sections.append(section)
        
        # Filter out TOC sections if TOC was provided
        if toc:
            sections_before = len(sections)
            sections = self._filter_toc_sections(sections, toc)
            sections_after = len(sections)
            if sections_before > sections_after:
                logger.info(f"{document.documentId}: Filtered out {sections_before - sections_after} TOC sections")
        
        # Re-number sections after filtering
        for i, section in enumerate(sections):
            section.order = i
        
        # Build hierarchical structure
        self._build_hierarchy(sections)
        
        return sections
    
    def _is_section_header(self, line: str) -> tuple[bool, str, int]:
        """
        Check if a line is a section header.
        
        Returns:
            (is_section, section_type, level)
        """
        # Check named sections
        for pattern in self.named_sections:
            if re.match(pattern, line, re.IGNORECASE):
                return (True, "named", 1)
        
        # Check numbered sections
        for pattern in self.numbered_patterns:
            if re.match(pattern, line):
                level = self._extract_level(line)
                return (True, "numbered", level)
        
        return (False, None, 0)
    
    def _extract_level(self, header: str) -> int:
        """Extract hierarchical level from section header."""
        # For numbered sections: count dots
        # "1." = level 1, "1.1" = level 2, "1.1.1" = level 3
        match = re.match(r'^(\d+(?:\.\d+)*)', header)
        if match:
            numbering = match.group(1)
            return len(numbering.split('.'))
        
        # Named sections are level 1
        return 1
    
    def _extract_section_number(self, header: str) -> str:
        """Extract section number from header."""
        # For numbered sections
        match = re.match(r'^(\d+(?:\.\d+)*)', header)
        if match:
            return match.group(1)
        
        # For named sections, use the name itself
        for pattern in self.named_sections:
            if re.match(pattern, header, re.IGNORECASE):
                # Extract the section name (e.g., "OGGETTO", "QUESITO")
                name_match = re.match(r'^([A-ZÀ-Ÿ\s]+)', header, re.IGNORECASE)
                if name_match:
                    return name_match.group(1).strip().upper()
        
        return "UNKNOWN"
    
    def _create_section(
        self, 
        document: Document, 
        order: int,
        header: str, 
        content_lines: List[str],
        section_type: str,
        page_number: int,
        level: int
    ) -> Section:
        """Create a Section object."""
        section_number = self._extract_section_number(header)
        content = '\n'.join(content_lines).strip()
        
        # Create section ID
        section_id = f"{document.documentId}_SEC_{order}"
        
        return Section(
            sectionId=section_id,
            documentId=document.documentId,
            sectionNumber=section_number,
            title=header[:200],  # Limit title length
            content=content,
            sectionType=section_type,
            level=level,
            pageNumber=page_number,
            order=order,
            parentSectionId=None  # Will be set in _build_hierarchy
        )
    
    def _filter_toc_sections(self, sections: List[Section], toc: TableOfContents) -> List[Section]:
        """
        Filter out sections that are part of the Table of Contents.
        
        A section is considered a TOC section if:
        1. It's on a TOC page (between toc.startPage and toc.endPage)
        2. It has very short content (< 100 chars)
        
        Args:
            sections: List of all extracted sections
            toc: TableOfContents object with page range
        
        Returns:
            Filtered list of sections (TOC sections removed)
        """
        filtered_sections = []
        
        for section in sections:
            # Check if section is on a TOC page
            if toc.startPage <= section.pageNumber <= toc.endPage:
                # Additional check: very short content = likely TOC entry
                if len(section.content) < 100:
                    logger.debug(f"Filtered TOC section: {section.sectionNumber} '{section.title[:50]}' (page {section.pageNumber}, {len(section.content)} chars)")
                    continue
            
            filtered_sections.append(section)
        
        return filtered_sections
    
    def _build_hierarchy(self, sections: List[Section]) -> None:
        """
        Build parent-child relationships between sections.
        Modifies sections in place by setting parentSectionId.
        """
        if not sections:
            return
        
        # Stack to track parent sections at each level
        parent_stack = {}
        
        for section in sections:
            level = section.level
            
            # Find parent (section at level-1)
            if level > 1 and (level - 1) in parent_stack:
                section.parentSectionId = parent_stack[level - 1].sectionId
            
            # Update stack: this section becomes parent for its level
            parent_stack[level] = section
            
            # Remove deeper levels from stack
            levels_to_remove = [l for l in parent_stack.keys() if l > level]
            for l in levels_to_remove:
                del parent_stack[l]

