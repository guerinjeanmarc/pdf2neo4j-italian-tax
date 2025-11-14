"""
Async version of legal reference extractor for parallel processing.
"""
import re
import logging
from typing import List, Dict, Optional
import asyncio
from pydantic import BaseModel, Field

try:
    from openai import AsyncOpenAI
except ImportError:
    raise ImportError("Please install openai: pip install openai")

from .config import Config
from .legal_reference_extractor import (
    LegalReferenceExtracted,
    LegalReferencesResponse,
    LegalReferenceExtractor
)

logger = logging.getLogger(__name__)


class AsyncLegalReferenceExtractor(LegalReferenceExtractor):
    """Async version for parallel processing."""
    
    def __init__(self, model: str = None, max_concurrent: int = 10):
        """
        Initialize the async extractor.
        
        Args:
            model: OpenAI model to use
            max_concurrent: Maximum number of concurrent API calls
        """
        super().__init__(model)
        self.async_client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    async def extract_from_chunk_async(self, chunk_content: str, chunk_id: str) -> List[Dict]:
        """
        Async version of extract_from_chunk.
        
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
            async with self.semaphore:  # Limit concurrent requests
                response = await self.async_client.beta.chat.completions.parse(
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
    
    async def batch_extract_from_chunks_async(
        self, 
        chunks: List[Dict], 
        max_chunks: int = None
    ) -> Dict[str, List[Dict]]:
        """
        Extract references from multiple chunks in parallel.
        
        Args:
            chunks: List of chunk dicts with 'chunkId' and 'content' keys
            max_chunks: Maximum number of chunks to process (for testing)
            
        Returns:
            Dict mapping chunk_id -> list of references
        """
        results = {}
        chunks_to_process = chunks[:max_chunks] if max_chunks else chunks
        
        logger.info(f"Extracting legal references from {len(chunks_to_process)} chunks (parallel)...")
        
        # Create tasks for all chunks
        tasks = [
            self.extract_from_chunk_async(chunk['content'], chunk['chunkId'])
            for chunk in chunks_to_process
        ]
        
        # Process all chunks in parallel
        all_refs = await asyncio.gather(*tasks)
        
        # Map results back to chunk IDs
        for chunk, refs in zip(chunks_to_process, all_refs):
            if refs:
                results[chunk['chunkId']] = refs
        
        total_refs = sum(len(refs) for refs in results.values())
        logger.info(f"Extracted {total_refs} references from {len(results)} chunks")
        
        return results

