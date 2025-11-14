"""
Configuration management for the pipeline.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
load_dotenv()


class Config:
    """Pipeline configuration."""
    
    # Neo4j Configuration
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
    NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
    
    # OpenAI Configuration (for future use)
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
    # Note: gpt-4o-mini is recommended for structured extraction (fast, cheap, no reasoning overhead)
    EXTRACT_MODEL = os.getenv("EXTRACT_MODEL", "gpt-4o-mini")
    AGENT_MODEL = os.getenv("AGENT_MODEL", "gpt-4o")
    
    # Processing Configuration
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "512"))  # tokens
    CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "50"))  # tokens
    
    # Paths
    PDF_DIRECTORY = Path(os.getenv("PDF_DIRECTORY", "downloaded_pdfs"))
    
    @classmethod
    def validate(cls):
        """Validate required configuration."""
        if not cls.NEO4J_PASSWORD:
            raise ValueError("NEO4J_PASSWORD must be set in .env file")
        
        if not cls.PDF_DIRECTORY.exists():
            raise ValueError(f"PDF directory not found: {cls.PDF_DIRECTORY}")
        
        return True

