"""
Extract legal references from all chunks using LLM and ingest into Neo4j.
"""
import sys
from pathlib import Path
import logging
from typing import List, Dict
import time
import asyncio

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.config import Config
from pipeline.legal_reference_extractor import LegalReferenceExtractor
from pipeline.legal_reference_extractor_async import AsyncLegalReferenceExtractor
from pipeline.neo4j_ingester import Neo4jIngester
from pipeline.models import LegalReference

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_chunks_from_neo4j(ingester: Neo4jIngester, limit: int = None) -> List[Dict]:
    """
    Fetch all chunks from Neo4j.
    
    Args:
        ingester: Neo4jIngester instance
        limit: Optional limit on number of chunks to fetch (for testing)
        
    Returns:
        List of chunk dicts with 'chunkId' and 'content' keys
    """
    query = """
    MATCH (c:Chunk)
    RETURN c.chunkId as chunkId, 
           c.content as content,
           c.documentId as documentId,
           c.pageNumber as pageNumber
    ORDER BY c.documentId, c.chunkIndex
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    with ingester.driver.session(database=ingester.database) as session:
        result = session.run(query)
        chunks = [dict(record) for record in result]
    
    logger.info(f"Fetched {len(chunks)} chunks from Neo4j")
    return chunks


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract legal references from chunks")
    parser.add_argument(
        '--limit', 
        type=int, 
        help='Limit number of chunks to process (for testing)'
    )
    parser.add_argument(
        '--sample',
        type=int,
        help='Process only every Nth chunk (for quick testing)',
        default=1
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Extract references but do not ingest into Neo4j'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Use parallel processing (5-10x faster, recommended for large datasets)'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=10,
        help='Maximum concurrent API calls when using --parallel (default: 10)'
    )
    
    args = parser.parse_args()
    
    print("=" * 100)
    print("LEGAL REFERENCE EXTRACTION")
    print("=" * 100)
    
    # Validate configuration
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Initialize components
    print("\n📋 Initializing components...")
    if args.parallel:
        print(f"   Using PARALLEL processing (max {args.max_concurrent} concurrent requests)")
        extractor = AsyncLegalReferenceExtractor(max_concurrent=args.max_concurrent)
    else:
        print("   Using SEQUENTIAL processing (use --parallel for 5-10x speedup)")
        extractor = LegalReferenceExtractor()
    ingester = Neo4jIngester()
    
    try:
        # Create constraints if needed
        print("📦 Creating/verifying constraints...")
        ingester.create_constraints()
        
        # Fetch chunks
        print(f"\n📚 Fetching chunks from Neo4j...")
        if args.limit:
            print(f"   (Limited to {args.limit} chunks)")
        
        chunks = fetch_chunks_from_neo4j(ingester, limit=args.limit)
        
        if not chunks:
            print("❌ No chunks found in database!")
            sys.exit(1)
        
        # Apply sampling if specified
        if args.sample > 1:
            chunks = chunks[::args.sample]
            print(f"   Sampling: Processing every {args.sample}th chunk ({len(chunks)} total)")
        
        print(f"   Found {len(chunks)} chunks to process")
        
        # Extract references
        print(f"\n🔍 Extracting legal references...")
        print(f"   Model: {Config.EXTRACT_MODEL}")
        if args.parallel:
            print(f"   Using parallel processing with {args.max_concurrent} concurrent requests...")
        else:
            print(f"   This may take a while (processing sequentially)...")
        
        start_time = time.time()
        
        if args.parallel:
            # Use async parallel processing
            references_by_chunk = asyncio.run(
                extractor.batch_extract_from_chunks_async(chunks)
            )
        else:
            # Use sequential processing
            references_by_chunk = extractor.batch_extract_from_chunks(chunks)
        
        extraction_time = time.time() - start_time
        
        # Statistics
        total_refs = sum(len(refs) for refs in references_by_chunk.values())
        chunks_with_refs = len(references_by_chunk)
        
        print(f"\n📊 Extraction Results:")
        print(f"   Total chunks processed:         {len(chunks):,}")
        print(f"   Chunks with references:         {chunks_with_refs:,} ({100*chunks_with_refs/len(chunks):.1f}%)")
        print(f"   Total references extracted:     {total_refs:,}")
        print(f"   Avg references per chunk (all): {total_refs/len(chunks):.2f}")
        if chunks_with_refs > 0:
            print(f"   Avg references per chunk (with refs): {total_refs/chunks_with_refs:.2f}")
        print(f"   Extraction time:                {extraction_time:.1f}s")
        print(f"   Time per chunk:                 {extraction_time/len(chunks):.2f}s")
        
        # Show sample references
        if references_by_chunk:
            print(f"\n📝 Sample References (first 10):")
            sample_count = 0
            for chunk_id, refs in list(references_by_chunk.items())[:10]:
                for ref_dict in refs:
                    print(f"   - {ref_dict['type']:20s} | {ref_dict['citation'][:70]}")
                    sample_count += 1
                    if sample_count >= 10:
                        break
                if sample_count >= 10:
                    break
        
        # Ingest into Neo4j
        if not args.dry_run:
            print(f"\n💾 Ingesting references into Neo4j...")
            
            # Convert dicts to LegalReference objects
            references_by_chunk_objects = {}
            for chunk_id, refs in references_by_chunk.items():
                references_by_chunk_objects[chunk_id] = [
                    LegalReference(**ref_dict) for ref_dict in refs
                ]
            
            ingestion_start = time.time()
            ingested_count = ingester.ingest_legal_references(references_by_chunk_objects)
            ingestion_time = time.time() - ingestion_start
            
            print(f"   ✅ Ingested {ingested_count:,} references in {ingestion_time:.1f}s")
        else:
            print(f"\n⚠️  DRY RUN: Skipping ingestion into Neo4j")
        
        # Final summary
        print("\n" + "=" * 100)
        print("✅ EXTRACTION COMPLETE")
        print("=" * 100)
        
        if not args.dry_run:
            print("\n💡 Next Steps:")
            print("  1. Open Neo4j Browser: http://localhost:7474")
            print("  2. Query references: MATCH (lr:LegalReference) RETURN count(lr)")
            print("  3. Find chunks with references: MATCH (c:Chunk)-[:REFERENCES_LAW]->(lr) RETURN c, lr LIMIT 10")
            print("  4. Group by reference type: MATCH (lr:LegalReference) RETURN lr.type, count(lr) ORDER BY count(lr) DESC")
        
    finally:
        ingester.close()


if __name__ == "__main__":
    main()

