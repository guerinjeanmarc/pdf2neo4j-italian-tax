"""
Text chunking for sections.
"""
import re
from typing import List
from .models import Section, Chunk


class Chunker:
    """Split sections into overlapping chunks for vector search."""
    
    def __init__(self, chunk_size: int = 512, overlap: int = 50):
        """
        Initialize chunker.
        
        Args:
            chunk_size: Target chunk size in tokens (approximate)
            overlap: Overlap between chunks in tokens (approximate)
        """
        self.chunk_size = chunk_size
        self.overlap = overlap
        
        # Rough estimation: 1 token ≈ 4 characters for Italian text
        self.chars_per_token = 4
    
    def chunk_section(self, section: Section) -> List[Chunk]:
        """
        Split a section into chunks.
        
        Args:
            section: Section object to chunk
            
        Returns:
            List of Chunk objects
        """
        if not section.content or len(section.content) < 100:
            # Section too small, create single chunk
            return [self._create_chunk(
                section=section,
                chunk_index=0,
                content=section.content,
                page_number=section.pageNumber
            )]
        
        # Split into chunks with overlap
        chunks = []
        chunk_size_chars = self.chunk_size * self.chars_per_token
        overlap_chars = self.overlap * self.chars_per_token
        
        # Split content into sentences first (for better chunk boundaries)
        sentences = self._split_into_sentences(section.content)
        
        current_chunk_text = ""
        chunk_index = 0
        
        for sentence in sentences:
            # Check if adding this sentence exceeds chunk size
            if len(current_chunk_text) + len(sentence) > chunk_size_chars and current_chunk_text:
                # Save current chunk
                chunk = self._create_chunk(
                    section=section,
                    chunk_index=chunk_index,
                    content=current_chunk_text.strip(),
                    page_number=section.pageNumber
                )
                chunks.append(chunk)
                chunk_index += 1
                
                # Start new chunk with overlap
                # Keep last part of previous chunk
                overlap_text = self._get_overlap_text(current_chunk_text, overlap_chars)
                current_chunk_text = overlap_text + " " + sentence
            else:
                current_chunk_text += " " + sentence
        
        # Add final chunk
        if current_chunk_text.strip():
            chunk = self._create_chunk(
                section=section,
                chunk_index=chunk_index,
                content=current_chunk_text.strip(),
                page_number=section.pageNumber
            )
            chunks.append(chunk)
        
        return chunks
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """
        Split text into sentences (simple approach).
        """
        # Split on sentence boundaries (. ! ? followed by space/newline)
        # Keep the punctuation with the sentence
        sentences = re.split(r'([.!?]+[\s\n]+)', text)
        
        # Recombine sentences with their punctuation
        result = []
        for i in range(0, len(sentences) - 1, 2):
            sentence = sentences[i] + (sentences[i + 1] if i + 1 < len(sentences) else "")
            sentence = sentence.strip()
            if sentence:
                result.append(sentence)
        
        # Handle last sentence if no punctuation
        if len(sentences) % 2 == 1 and sentences[-1].strip():
            result.append(sentences[-1].strip())
        
        return result if result else [text]
    
    def _get_overlap_text(self, text: str, overlap_chars: int) -> str:
        """Get the last N characters of text for overlap."""
        if len(text) <= overlap_chars:
            return text
        
        # Try to cut at sentence boundary within overlap region
        overlap_text = text[-overlap_chars:]
        
        # Find sentence start
        sentence_start = re.search(r'[.!?]\s+', overlap_text)
        if sentence_start:
            return overlap_text[sentence_start.end():]
        
        return overlap_text
    
    def _create_chunk(
        self, 
        section: Section, 
        chunk_index: int, 
        content: str,
        page_number: int
    ) -> Chunk:
        """Create a Chunk object."""
        chunk_id = f"{section.sectionId}_CHK_{chunk_index}"
        
        return Chunk(
            chunkId=chunk_id,
            documentId=section.documentId,
            sectionId=section.sectionId,
            content=content,
            chunkIndex=chunk_index,
            pageNumber=page_number,
            metadata={
                "section_number": section.sectionNumber,
                "section_type": section.sectionType
            }
        )

