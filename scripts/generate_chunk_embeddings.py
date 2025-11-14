"""
Generate embeddings for all chunks with enriched context (document + section metadata).
"""
import sys
from pathlib import Path
import logging
from typing import List, Dict
import time
import asyncio
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.config import Config
from pipeline.neo4j_ingester import Neo4jIngester

try:
    from openai import OpenAI, AsyncOpenAI
except ImportError:
    raise ImportError("Please install openai: pip install openai")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_chunks_with_context(ingester: Neo4jIngester, limit: int = None) -> List[Dict]:
    """
    Fetch all chunks with their document and section metadata.
    
    Returns list of dicts with:
    - chunkId, content
    - documentType, documentNumber, documentYear
    - sectionNumber, sectionTitle
    """
    query = """
    MATCH (c:Chunk)<-[:HAS_CHUNK]-(s:Section)<-[:HAS_SECTION|HAS_SUBSECTION*0..]-(parent:Section)<-[:HAS_SECTION]-(d:Document)
    WHERE NOT EXISTS((s)<-[:HAS_SUBSECTION]-())
    WITH c, s, d
    RETURN 
        c.chunkId as chunkId,
        c.content as content,
        d.type as documentType,
        d.number as documentNumber,
        d.year as documentYear,
        s.sectionNumber as sectionNumber,
        s.title as sectionTitle
    ORDER BY d.documentId, s.order, c.chunkIndex
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    with ingester.driver.session(database=ingester.database) as session:
        result = session.run(query)
        chunks = [dict(record) for record in result]
    
    logger.info(f"Fetched {len(chunks)} chunks with context from Neo4j")
    return chunks


def create_enriched_text(chunk: Dict) -> str:
    """
    Create enriched text for embedding with document and section metadata.
    
    Format:
    Documento: {type} {number}/{year}
    Sezione: {sectionNumber} - {sectionTitle}
    
    {content}
    """
    doc_identifier = f"{chunk['documentType']} {chunk['documentNumber']}/{chunk['documentYear']}"
    section_identifier = f"{chunk['sectionNumber']} - {chunk['sectionTitle']}" if chunk['sectionTitle'] else chunk['sectionNumber']
    
    enriched = f"""Documento: {doc_identifier}
Sezione: {section_identifier}

{chunk['content']}"""
    
    return enriched


class EmbeddingGenerator:
    """Generate embeddings for chunks using OpenAI."""
    
    def __init__(self):
        self.client = OpenAI(api_key=Config.OPENAI_API_KEY)
        self.model = Config.EMBEDDING_MODEL
        logger.info(f"Initialized EmbeddingGenerator with model: {self.model}")
    
    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors (1536 dimensions for text-embedding-3-small)
        """
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=texts
            )
            
            # Extract embeddings in order
            embeddings = [item.embedding for item in response.data]
            return embeddings
            
        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            return []
    
    def process_chunks_sequential(
        self,
        chunks: List[Dict],
        batch_size: int = 500
    ) -> Dict[str, List[float]]:
        """
        Process chunks sequentially in batches.
        
        Args:
            chunks: List of chunk dicts
            batch_size: Number of chunks per batch (OpenAI supports up to 2048)
            
        Returns:
            Dict mapping chunkId -> embedding vector
        """
        embeddings_by_chunk = {}
        
        # Create enriched texts
        logger.info("Creating enriched texts...")
        enriched_texts = [create_enriched_text(chunk) for chunk in chunks]
        
        # Process in batches
        total_batches = (len(chunks) + batch_size - 1) // batch_size
        logger.info(f"Processing {len(chunks)} chunks in {total_batches} batches of {batch_size}...")
        
        for i in tqdm(range(0, len(chunks), batch_size), desc="Generating embeddings"):
            batch_chunks = chunks[i:i + batch_size]
            batch_texts = enriched_texts[i:i + batch_size]
            
            # Generate embeddings
            embeddings = self.generate_embeddings_batch(batch_texts)
            
            if len(embeddings) != len(batch_chunks):
                logger.error(f"Batch {i//batch_size + 1}: Expected {len(batch_chunks)} embeddings, got {len(embeddings)}")
                continue
            
            # Map to chunk IDs
            for chunk, embedding in zip(batch_chunks, embeddings):
                embeddings_by_chunk[chunk['chunkId']] = embedding
        
        return embeddings_by_chunk


class AsyncEmbeddingGenerator:
    """Async version for parallel batch processing."""
    
    def __init__(self, max_concurrent: int = 5):
        self.client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)
        self.model = Config.EMBEDDING_MODEL
        self.semaphore = asyncio.Semaphore(max_concurrent)
        logger.info(f"Initialized AsyncEmbeddingGenerator with model: {self.model}, max_concurrent: {max_concurrent}")
    
    async def generate_embeddings_batch_async(self, texts: List[str]) -> List[List[float]]:
        """Async version of generate_embeddings_batch."""
        async with self.semaphore:
            try:
                response = await self.client.embeddings.create(
                    model=self.model,
                    input=texts
                )
                embeddings = [item.embedding for item in response.data]
                return embeddings
            except Exception as e:
                logger.error(f"Error generating embeddings: {e}")
                return []
    
    async def process_chunks_parallel(
        self,
        chunks: List[Dict],
        batch_size: int = 500
    ) -> Dict[str, List[float]]:
        """
        Process chunks in parallel batches.
        
        Args:
            chunks: List of chunk dicts
            batch_size: Number of chunks per batch
            
        Returns:
            Dict mapping chunkId -> embedding vector
        """
        embeddings_by_chunk = {}
        
        # Create enriched texts
        logger.info("Creating enriched texts...")
        enriched_texts = [create_enriched_text(chunk) for chunk in chunks]
        
        # Split into batches
        batches = []
        for i in range(0, len(chunks), batch_size):
            batch_chunks = chunks[i:i + batch_size]
            batch_texts = enriched_texts[i:i + batch_size]
            batches.append((batch_chunks, batch_texts))
        
        logger.info(f"Processing {len(chunks)} chunks in {len(batches)} batches (parallel)...")
        
        # Process all batches in parallel
        async def process_batch(batch_data):
            batch_chunks, batch_texts = batch_data
            embeddings = await self.generate_embeddings_batch_async(batch_texts)
            
            if len(embeddings) != len(batch_chunks):
                logger.error(f"Expected {len(batch_chunks)} embeddings, got {len(embeddings)}")
                return {}
            
            return {chunk['chunkId']: embedding for chunk, embedding in zip(batch_chunks, embeddings)}
        
        # Run all batches with progress bar
        tasks = [process_batch(batch) for batch in batches]
        results = await tqdm_asyncio.gather(*tasks, desc="Generating embeddings (parallel)")
        
        # Merge results
        for result in results:
            embeddings_by_chunk.update(result)
        
        return embeddings_by_chunk


def update_chunk_embeddings(ingester: Neo4jIngester, embeddings_by_chunk: Dict[str, List[float]]) -> int:
    """
    Update chunks in Neo4j with their embeddings.
    
    Args:
        ingester: Neo4jIngester instance
        embeddings_by_chunk: Dict mapping chunkId -> embedding vector
        
    Returns:
        Number of chunks updated
    """
    logger.info(f"Updating {len(embeddings_by_chunk)} chunks with embeddings...")
    
    query = """
    UNWIND $batch as item
    MATCH (c:Chunk {chunkId: item.chunkId})
    SET c.embedding = item.embedding
    """
    
    # Prepare batch data
    batch_data = [
        {'chunkId': chunk_id, 'embedding': embedding}
        for chunk_id, embedding in embeddings_by_chunk.items()
    ]
    
    # Update in batches of 1000
    batch_size = 1000
    updated = 0
    
    with ingester.driver.session(database=ingester.database) as session:
        for i in tqdm(range(0, len(batch_data), batch_size), desc="Updating Neo4j"):
            batch = batch_data[i:i + batch_size]
            session.run(query, batch=batch)
            updated += len(batch)
    
    logger.info(f"Updated {updated} chunks with embeddings")
    return updated


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate embeddings for chunks")
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of chunks to process (for testing)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=500,
        help='Number of chunks per API batch (default: 500, max: 2048)'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Use parallel processing (5-10x faster, recommended)'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=5,
        help='Maximum concurrent API calls when using --parallel (default: 5, be careful with rate limits)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Generate embeddings but do not update Neo4j'
    )
    
    args = parser.parse_args()
    
    print("=" * 100)
    print("CHUNK EMBEDDING GENERATION")
    print("=" * 100)
    
    # Validate config
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Initialize components
    print("\n📋 Initializing components...")
    ingester = Neo4jIngester()
    
    try:
        # Fetch chunks
        print("\n📚 Fetching chunks from Neo4j...")
        if args.limit:
            print(f"   (Limited to {args.limit} chunks)")
        
        chunks = fetch_chunks_with_context(ingester, limit=args.limit)
        
        if not chunks:
            print("❌ No chunks found in database!")
            sys.exit(1)
        
        print(f"   Found {len(chunks)} chunks to process")
        
        # Generate embeddings
        print(f"\n🔍 Generating embeddings...")
        print(f"   Model: {Config.EMBEDDING_MODEL}")
        print(f"   Batch size: {args.batch_size}")
        print(f"   Enrichment: Document type/number/year + Section number/title")
        
        if args.parallel:
            print(f"   Using PARALLEL processing (max {args.max_concurrent} concurrent batches)")
        else:
            print(f"   Using SEQUENTIAL processing (use --parallel for 5-10x speedup)")
        
        start_time = time.time()
        
        if args.parallel:
            # Parallel processing
            generator = AsyncEmbeddingGenerator(max_concurrent=args.max_concurrent)
            embeddings_by_chunk = asyncio.run(
                generator.process_chunks_parallel(chunks, batch_size=args.batch_size)
            )
        else:
            # Sequential processing
            generator = EmbeddingGenerator()
            embeddings_by_chunk = generator.process_chunks_sequential(chunks, batch_size=args.batch_size)
        
        generation_time = time.time() - start_time
        
        # Statistics
        total_embeddings = len(embeddings_by_chunk)
        
        print(f"\n📊 Generation Results:")
        print(f"   Chunks processed:        {len(chunks):,}")
        print(f"   Embeddings generated:    {total_embeddings:,}")
        print(f"   Success rate:            {100*total_embeddings/len(chunks):.1f}%" if len(chunks) > 0 else "N/A")
        print(f"   Generation time:         {generation_time:.1f}s ({generation_time/60:.1f} min)")
        print(f"   Time per chunk:          {generation_time/len(chunks):.3f}s" if len(chunks) > 0 else "N/A")
        
        # Sample embedding
        if embeddings_by_chunk:
            sample_chunk_id = list(embeddings_by_chunk.keys())[0]
            sample_embedding = embeddings_by_chunk[sample_chunk_id]
            print(f"\n📝 Sample Embedding:")
            print(f"   Chunk ID: {sample_chunk_id}")
            print(f"   Dimensions: {len(sample_embedding)}")
            print(f"   First 5 values: {sample_embedding[:5]}")
        
        # Update Neo4j
        if not args.dry_run:
            print(f"\n💾 Updating chunks in Neo4j...")
            update_start = time.time()
            updated = update_chunk_embeddings(ingester, embeddings_by_chunk)
            update_time = time.time() - update_start
            print(f"   ✅ Updated {updated:,} chunks in {update_time:.1f}s")
        else:
            print(f"\n⚠️  DRY RUN: Skipping Neo4j update")
        
        # Final summary
        print("\n" + "=" * 100)
        print("✅ EMBEDDING GENERATION COMPLETE")
        print("=" * 100)
        
        if not args.dry_run:
            print("\n💡 Next Steps:")
            print("  1. Open Neo4j Browser: http://localhost:7474")
            print("  2. Verify embeddings:")
            print("     MATCH (c:Chunk) WHERE c.embedding IS NOT NULL RETURN count(c)")
            print("  3. Test vector search:")
            print("     CALL db.index.vector.queryNodes('chunk_embedding_vector', 5, $queryVector)")
            print("  4. Test fulltext search:")
            print("     CALL db.index.fulltext.queryNodes('chunk_content_fulltext', 'ecobonus')")
        
        # Cost estimate
        if not args.dry_run:
            # text-embedding-3-small: $0.020 / 1M tokens
            avg_tokens_per_chunk = 600  # ~512 content + ~50 metadata
            total_tokens = len(chunks) * avg_tokens_per_chunk
            estimated_cost = (total_tokens / 1_000_000) * 0.020
            print(f"\n💰 Estimated Cost:")
            print(f"   Total tokens: ~{total_tokens:,}")
            print(f"   Cost: ~${estimated_cost:.2f}")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Process interrupted by user")
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
    finally:
        ingester.close()


if __name__ == "__main__":
    main()

