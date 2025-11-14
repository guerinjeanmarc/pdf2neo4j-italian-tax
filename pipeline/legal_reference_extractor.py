"""
Extract legal references from chunk text using LLM.
"""
import re
import logging
from typing import List, Dict, Optional
from datetime import datetime
import json
from pydantic import BaseModel, Field

try:
    from openai import OpenAI
except ImportError:
    raise ImportError("Please install openai: pip install openai")

from .config import Config

logger = logging.getLogger(__name__)


class LegalReferenceExtracted(BaseModel):
    """Pydantic model for a single extracted legal reference."""
    type: str = Field(description="Type: decreto_legislativo, decreto_legge, legge, circolare, risoluzione, risposta, or altro")
    number: str = Field(description="Document or law number (e.g., '20', '208', '63')")
    year: str = Field(description="Year as 4-digit string (e.g., '2011', '2024', '2018')")
    article: Optional[str] = Field(None, description="Article number if specific article referenced (e.g., '5', '13-bis')")
    citation: str = Field(description="The exact citation text from the document")
    description: Optional[str] = Field(None, description="Brief description of what this reference is about")


class LegalReferencesResponse(BaseModel):
    """Pydantic model for the complete extraction response."""
    references: List[LegalReferenceExtracted] = Field(default_factory=list, description="List of extracted legal references")


class LegalReferenceExtractor:
    """Extract legal references from text using LLM."""
    
    def __init__(self, model: str = None):
        """
        Initialize the extractor.
        
        Args:
            model: OpenAI model to use (defaults to Config.EXTRACT_MODEL)
        """
        self.client = OpenAI(api_key=Config.OPENAI_API_KEY)
        self.model = model or Config.EXTRACT_MODEL
        
        # Common Italian legal reference patterns (for validation)
        self.patterns = {
            'decreto_legislativo': re.compile(
                r'[Dd]ecreto\s+[Ll]egislativo\s+(?:n\.\s*)?(\d+)[/\s]+del[/\s]+(\d{1,2}[\./]\d{1,2}[\./]\d{2,4})',
                re.IGNORECASE
            ),
            'decreto_legge': re.compile(
                r'[Dd]ecreto\s+[Ll]egge\s+(?:n\.\s*)?(\d+)[/\s]+del[/\s]+(\d{1,2}[\./]\d{1,2}[\./]\d{2,4})',
                re.IGNORECASE
            ),
            'legge': re.compile(
                r'[Ll]egge\s+(?:n\.\s*)?(\d+)[/\s]+del[/\s]+(\d{1,2}[\./]\d{1,2}[\./]\d{2,4})',
                re.IGNORECASE
            ),
            'articolo': re.compile(
                r'[Aa]rticolo\s+(\d+(?:-[a-z]+)?)',
                re.IGNORECASE
            ),
            'circolare': re.compile(
                r'[Cc]ircolare\s+(?:n\.\s*)?(\d+)[/\s]*([E-]?)[/\s]+del[/\s]+(\d{1,2}[\./]\d{1,2}[\./]\d{2,4})',
                re.IGNORECASE
            ),
            'risoluzione': re.compile(
                r'[Rr]isoluzione\s+(?:n\.\s*)?(\d+)[/\s]*([E-]?)[/\s]+del[/\s]+(\d{1,2}[\./]\d{1,2}[\./]\d{2,4})',
                re.IGNORECASE
            ),
        }
    
    def extract_from_chunk(self, chunk_content: str, chunk_id: str) -> List[Dict]:
        """
        Extract legal references from a chunk using LLM with structured output.
        
        Args:
            chunk_content: The text content of the chunk
            chunk_id: Unique ID of the chunk
            
        Returns:
            List of legal reference dictionaries
        """
        # Quick regex pre-check to avoid LLM call if no references
        has_potential_reference = any(
            pattern.search(chunk_content) 
            for pattern in self.patterns.values()
        )
        
        if not has_potential_reference:
            return []
        
        system_prompt = """You are an expert in Italian tax law. Extract all legal references from the given text.

For each reference, identify:
1. Type: decreto_legislativo, decreto_legge, legge, circolare, risoluzione, risposta, or altro
2. Number: The document or law number (e.g., "20", "208", "63")
3. Year: The 4-digit year (e.g., "2011", "2024", "2018")
4. Article: Article number if specific article is referenced (e.g., "5", "13-bis")
5. Citation: The exact citation text from the document
6. Description: Brief description of what this reference is about (from context)

Extract the year from phrases like "del 2011", "del 15/03/2024", "/2011", etc.
If you cannot determine the number or year with confidence, skip that reference.

If no references are found, return an empty list."""

        user_prompt = f"""Extract all legal references from this text:

{chunk_content}"""

        try:
            response = self.client.beta.chat.completions.parse(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format=LegalReferencesResponse,
                max_completion_tokens=2000
            )
            
            # Get parsed response
            parsed_response = response.choices[0].message.parsed
            
            if not parsed_response or not parsed_response.references:
                return []
            
            # Validate and enrich
            validated_refs = []
            for ref in parsed_response.references:
                ref_dict = ref.model_dump()
                validated_ref = self._validate_and_enrich(ref_dict, chunk_id)
                if validated_ref:
                    validated_refs.append(validated_ref)
            
            if validated_refs:
                logger.debug(f"Extracted {len(validated_refs)} references from chunk {chunk_id}")
            
            return validated_refs
            
        except Exception as e:
            logger.error(f"Error extracting references from chunk {chunk_id}: {e}")
            return []
    
    def _validate_and_enrich(self, ref: Dict, chunk_id: str) -> Optional[Dict]:
        """
        Validate and enrich a legal reference.
        
        Args:
            ref: Raw reference dict from LLM
            chunk_id: Source chunk ID
            
        Returns:
            Validated and enriched reference dict, or None if invalid
        """
        # Required fields
        if not ref.get('type') or not ref.get('citation'):
            logger.warning(f"Reference missing type or citation in chunk {chunk_id}: {ref}")
            return None
        
        # Normalize type
        ref['type'] = ref['type'].lower()
        
        # Validate number and year (required for canonical ID)
        if not ref.get('number') or not ref.get('year'):
            logger.debug(f"Reference missing number or year in chunk {chunk_id}: {ref.get('citation')}")
            # Still valid, but will get fallback ID
        
        # Validate year format (should be 4 digits)
        if ref.get('year'):
            year_str = str(ref['year']).strip()
            # Check for null/None strings from LLM
            if year_str.lower() in ('null', 'none', ''):
                ref['year'] = None
            elif not re.match(r'^\d{4}$', year_str):
                logger.warning(f"Invalid year format '{year_str}' in chunk {chunk_id}, expected 4 digits")
                # Try to extract 4-digit year
                year_match = re.search(r'\b(19|20)\d{2}\b', year_str)
                if year_match:
                    ref['year'] = year_match.group(0)
                else:
                    ref['year'] = None
        
        # Create canonical reference ID
        ref_id = self._create_reference_id(ref)
        ref['referenceId'] = ref_id
        
        # Add source chunk
        ref['sourceChunkId'] = chunk_id
        
        return ref
    
    def _create_reference_id(self, ref: Dict) -> str:
        """
        Create a canonical reference ID that matches Document IDs when possible.
        
        For our document types (circolare, risoluzione, risposta):
            - Format: {TYPE}_{number}_{year} (matches Document.documentId)
            - Examples: CIR_20_2011, RIS_208_2024, RIP_15_2025
            - Article info stored in node, not in ID
            
        For external references (laws, decrees):
            - Format: EXT_{TYPE}_{number}_{year}
            - Examples: EXT_DL_63_2013, EXT_LEG_296_2006
            
        For incomplete references:
            - Fallback to hash-based ID: REF_{hash}
        """
        import hashlib
        
        # Map types to 3-letter codes (matching Document IDs)
        type_map = {
            'circolare': 'CIR',
            'risoluzione': 'RIS',
            'risposta': 'RIP',
            'decreto_legislativo': 'EXT_DL',
            'decreto_legge': 'EXT_DEC',
            'legge': 'EXT_LEG',
            'altro': 'EXT_OTH'
        }
        
        ref_type = ref.get('type', '').lower()
        type_code = type_map.get(ref_type, 'REF')
        number = str(ref.get('number', '')).strip() if ref.get('number') else ''
        year = str(ref.get('year', '')).strip() if ref.get('year') else ''
        
        # Check if we have enough info for canonical ID
        if number and year and len(year) == 4:
            # Normalize number to match Document IDs
            # Remove letter suffixes like /E, /bis, -E, etc. (e.g., "52/E" → "52", "208-bis" → "208")
            number_normalized = re.sub(r'[/\-\s]*[A-Za-z]+$', '', number).strip()
            
            # If nothing left after removing suffix, keep original
            if not number_normalized:
                number_normalized = number
            
            # Clean remaining special chars (replace with underscore)
            number_clean = re.sub(r'[^\w]', '_', number_normalized)
            # Remove trailing underscores
            number_clean = number_clean.strip('_')
            
            return f"{type_code}_{number_clean}_{year}"
        else:
            # Incomplete reference - use hash-based fallback
            hash_str = f"{ref_type}_{ref.get('citation', '')}"
            hash_short = hashlib.md5(hash_str.encode()).hexdigest()[:8]
            logger.debug(f"Using hash-based ID for incomplete reference: {ref.get('citation')}")
            return f"REF_{hash_short}"
    
    def batch_extract_from_chunks(
        self, 
        chunks: List[Dict], 
        max_chunks: int = None
    ) -> Dict[str, List[Dict]]:
        """
        Extract references from multiple chunks.
        
        Args:
            chunks: List of chunk dicts with 'chunkId' and 'content' keys
            max_chunks: Maximum number of chunks to process (for testing)
            
        Returns:
            Dict mapping chunk_id -> list of references
        """
        results = {}
        chunks_to_process = chunks[:max_chunks] if max_chunks else chunks
        
        logger.info(f"Extracting legal references from {len(chunks_to_process)} chunks...")
        
        for i, chunk in enumerate(chunks_to_process):
            if (i + 1) % 100 == 0:
                logger.info(f"  Processed {i + 1}/{len(chunks_to_process)} chunks...")
            
            chunk_id = chunk['chunkId']
            content = chunk['content']
            
            refs = self.extract_from_chunk(content, chunk_id)
            if refs:
                results[chunk_id] = refs
        
        total_refs = sum(len(refs) for refs in results.values())
        logger.info(f"Extracted {total_refs} references from {len(results)} chunks")
        
        return results

