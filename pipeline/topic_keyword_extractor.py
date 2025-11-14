"""
Extract topics and keywords from sections using LLM with structured output.
"""
import logging
from typing import List, Optional
from pydantic import BaseModel, Field
import asyncio

try:
    from openai import OpenAI, AsyncOpenAI
except ImportError:
    raise ImportError("Please install openai: pip install openai")

from .config import Config

logger = logging.getLogger(__name__)


class TopicExtracted(BaseModel):
    """Pydantic model for a single extracted topic."""
    name: str = Field(description="Topic name (e.g., 'Ecobonus', 'Superbonus', 'Credito d'imposta')")
    description: str = Field(description="Brief description of what this topic covers in this section")
    relevance_score: float = Field(description="Relevance score 0.0-1.0 indicating importance in this section", ge=0.0, le=1.0)


class KeywordExtracted(BaseModel):
    """Pydantic model for a single extracted keyword."""
    keyword: str = Field(description="Specific keyword or term (e.g., 'pannelli solari', 'caldaie', 'cappotto termico')")
    relevance_score: float = Field(description="Relevance score 0.0-1.0 indicating importance", ge=0.0, le=1.0)


class SectionAnalysis(BaseModel):
    """Pydantic model for complete section analysis."""
    topics: List[TopicExtracted] = Field(
        default_factory=list,
        description="Main tax topics discussed in this section (2-5 topics, high-level concepts)"
    )
    keywords: List[KeywordExtracted] = Field(
        default_factory=list,
        description="Important keywords and specific terms (5-15 keywords, concrete items/concepts)"
    )


class TopicKeywordExtractor:
    """Extract topics and keywords from sections using LLM."""
    
    def __init__(self, model: str = None):
        """
        Initialize the extractor.
        
        Args:
            model: OpenAI model to use (defaults to Config.EXTRACT_MODEL)
        """
        self.client = OpenAI(api_key=Config.OPENAI_API_KEY)
        self.model = model or Config.EXTRACT_MODEL
        logger.info(f"Initialized TopicKeywordExtractor with model: {self.model}")
    
    def extract_from_section(
        self, 
        section_content: str, 
        section_id: str,
        section_title: str = ""
    ) -> Optional[SectionAnalysis]:
        """
        Extract topics and keywords from a section using LLM with structured output.
        
        Args:
            section_content: The text content of the section
            section_id: Unique ID of the section
            section_title: Title of the section (optional, helps with context)
            
        Returns:
            SectionAnalysis with topics and keywords, or None if extraction fails
        """
        # Skip very short sections (likely not meaningful)
        if len(section_content.strip()) < 50:
            logger.debug(f"Skipping section {section_id} - too short ({len(section_content)} chars)")
            return None
        
        system_prompt = """You are an expert in Italian tax law. Analyze the given section and extract:

1. **Topics** (2-5 high-level tax concepts):
   - Main tax incentives (e.g., "Ecobonus", "Superbonus", "Sismabonus")
   - Tax credit types (e.g., "Credito d'imposta", "Detrazione fiscale")
   - Tax procedures (e.g., "Dichiarazione dei redditi", "Comunicazione all'Agenzia")
   - Legal concepts (e.g., "Impatriati", "Residenza fiscale")
   
2. **Keywords** (5-15 specific terms):
   - Specific items eligible for tax benefits (e.g., "pannelli solari", "caldaie", "cappotto termico")
   - Technical terms (e.g., "efficienza energetica", "classe energetica")
   - Amounts and percentages (e.g., "50%", "65%", "110%")
   - Dates and deadlines (e.g., "31 dicembre 2024", "scadenza")
   - Relevant entities (e.g., "condominio", "imprese", "lavoratori impatriati")

For each topic/keyword, provide a relevance score (0.0-1.0) based on how central it is to this section.

If the section is not about tax topics, return empty lists."""

        context = f"Section Title: {section_title}\n\n" if section_title else ""
        user_prompt = f"""{context}Extract topics and keywords from this Italian tax document section:

{section_content}"""

        try:
            response = self.client.beta.chat.completions.parse(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format=SectionAnalysis,
                max_completion_tokens=1500
            )
            
            # Get parsed response
            parsed_response = response.choices[0].message.parsed
            
            if not parsed_response:
                logger.warning(f"No analysis returned for section {section_id}")
                return None
            
            # Log results
            topic_count = len(parsed_response.topics)
            keyword_count = len(parsed_response.keywords)
            
            if topic_count > 0 or keyword_count > 0:
                logger.debug(
                    f"Extracted from {section_id}: "
                    f"{topic_count} topics, {keyword_count} keywords"
                )
            
            return parsed_response
            
        except Exception as e:
            logger.error(f"Error extracting topics/keywords from section {section_id}: {e}")
            return None
    
    def batch_extract_from_sections(
        self,
        sections: List[dict],
        max_sections: int = None
    ) -> dict[str, SectionAnalysis]:
        """
        Extract topics and keywords from multiple sections sequentially.
        
        Args:
            sections: List of section dicts with 'sectionId', 'content', and 'title' keys
            max_sections: Maximum number of sections to process (for testing)
            
        Returns:
            Dict mapping section_id -> SectionAnalysis
        """
        results = {}
        sections_to_process = sections[:max_sections] if max_sections else sections
        
        logger.info(f"Extracting topics/keywords from {len(sections_to_process)} sections...")
        
        for i, section in enumerate(sections_to_process):
            if (i + 1) % 50 == 0:
                logger.info(f"  Processed {i + 1}/{len(sections_to_process)} sections...")
            
            section_id = section['sectionId']
            content = section['content']
            title = section.get('title', '')
            
            analysis = self.extract_from_section(content, section_id, title)
            if analysis and (analysis.topics or analysis.keywords):
                results[section_id] = analysis
        
        total_topics = sum(len(a.topics) for a in results.values())
        total_keywords = sum(len(a.keywords) for a in results.values())
        
        logger.info(
            f"Extracted from {len(results)} sections: "
            f"{total_topics} topics, {total_keywords} keywords"
        )
        
        return results


class AsyncTopicKeywordExtractor(TopicKeywordExtractor):
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
    
    async def extract_from_section_async(
        self,
        section_content: str,
        section_id: str,
        section_title: str = ""
    ) -> Optional[SectionAnalysis]:
        """
        Async version of extract_from_section.
        
        Args:
            section_content: The text content of the section
            section_id: Unique ID of the section
            section_title: Title of the section (optional)
            
        Returns:
            SectionAnalysis with topics and keywords, or None if extraction fails
        """
        # Skip very short sections
        if len(section_content.strip()) < 50:
            return None
        
        system_prompt = """You are an expert in Italian tax law. Analyze the given section and extract:

1. **Topics** (2-5 high-level tax concepts):
   - Main tax incentives (e.g., "Ecobonus", "Superbonus", "Sismabonus")
   - Tax credit types (e.g., "Credito d'imposta", "Detrazione fiscale")
   - Tax procedures (e.g., "Dichiarazione dei redditi", "Comunicazione all'Agenzia")
   - Legal concepts (e.g., "Impatriati", "Residenza fiscale")
   
2. **Keywords** (5-15 specific terms):
   - Specific items eligible for tax benefits (e.g., "pannelli solari", "caldaie", "cappotto termico")
   - Technical terms (e.g., "efficienza energetica", "classe energetica")
   - Amounts and percentages (e.g., "50%", "65%", "110%")
   - Dates and deadlines (e.g., "31 dicembre 2024", "scadenza")
   - Relevant entities (e.g., "condominio", "imprese", "lavoratori impatriati")

For each topic/keyword, provide a relevance score (0.0-1.0) based on how central it is to this section.

If the section is not about tax topics, return empty lists."""

        context = f"Section Title: {section_title}\n\n" if section_title else ""
        user_prompt = f"""{context}Extract topics and keywords from this Italian tax document section:

{section_content}"""

        try:
            async with self.semaphore:  # Limit concurrent requests
                response = await self.async_client.beta.chat.completions.parse(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format=SectionAnalysis,
                    max_completion_tokens=1500
                )
                
                # Get parsed response
                parsed_response = response.choices[0].message.parsed
                
                if not parsed_response:
                    return None
                
                # Log results
                topic_count = len(parsed_response.topics)
                keyword_count = len(parsed_response.keywords)
                
                if topic_count > 0 or keyword_count > 0:
                    logger.debug(
                        f"Extracted from {section_id}: "
                        f"{topic_count} topics, {keyword_count} keywords"
                    )
                
                return parsed_response
                
        except Exception as e:
            logger.error(f"Error extracting topics/keywords from section {section_id}: {e}")
            return None
    
    async def batch_extract_from_sections_async(
        self,
        sections: List[dict],
        max_sections: int = None
    ) -> dict[str, SectionAnalysis]:
        """
        Extract topics and keywords from multiple sections in parallel.
        
        Args:
            sections: List of section dicts with 'sectionId', 'content', and 'title' keys
            max_sections: Maximum number of sections to process (for testing)
            
        Returns:
            Dict mapping section_id -> SectionAnalysis
        """
        results = {}
        sections_to_process = sections[:max_sections] if max_sections else sections
        
        logger.info(f"Extracting topics/keywords from {len(sections_to_process)} sections (parallel)...")
        
        # Create tasks for all sections
        tasks = [
            self.extract_from_section_async(
                section['content'],
                section['sectionId'],
                section.get('title', '')
            )
            for section in sections_to_process
        ]
        
        # Process all sections in parallel
        all_analyses = await asyncio.gather(*tasks)
        
        # Map results back to section IDs
        for section, analysis in zip(sections_to_process, all_analyses):
            if analysis and (analysis.topics or analysis.keywords):
                results[section['sectionId']] = analysis
        
        total_topics = sum(len(a.topics) for a in results.values())
        total_keywords = sum(len(a.keywords) for a in results.values())
        
        logger.info(
            f"Extracted from {len(results)} sections: "
            f"{total_topics} topics, {total_keywords} keywords"
        )
        
        return results

