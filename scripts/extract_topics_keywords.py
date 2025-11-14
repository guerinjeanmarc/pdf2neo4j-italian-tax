"""
Extract topics and keywords from all sections using LLM and ingest into Neo4j.
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
from pipeline.topic_keyword_extractor import TopicKeywordExtractor, AsyncTopicKeywordExtractor
from pipeline.neo4j_ingester import Neo4jIngester

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_sections_from_neo4j(ingester: Neo4jIngester, limit: int = None) -> List[Dict]:
    """
    Fetch all sections from Neo4j.
    
    Args:
        ingester: Neo4jIngester instance
        limit: Optional limit on number of sections to fetch (for testing)
        
    Returns:
        List of section dicts with 'sectionId', 'content', and 'title' keys
    """
    query = """
    MATCH (s:Section)
    RETURN s.sectionId as sectionId, 
           s.content as content,
           s.title as title,
           s.documentId as documentId,
           s.sectionNumber as sectionNumber
    ORDER BY s.documentId, s.order
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    with ingester.driver.session(database=ingester.database) as session:
        result = session.run(query)
        sections = [dict(record) for record in result]
    
    logger.info(f"Fetched {len(sections)} sections from Neo4j")
    return sections


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract topics and keywords from sections")
    parser.add_argument(
        '--limit', 
        type=int, 
        help='Limit number of sections to process (for testing)'
    )
    parser.add_argument(
        '--sample',
        type=int,
        help='Process only every Nth section (for quick testing)',
        default=1
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Extract topics/keywords but do not ingest into Neo4j'
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
    print("TOPIC & KEYWORD EXTRACTION FROM SECTIONS")
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
        extractor = AsyncTopicKeywordExtractor(max_concurrent=args.max_concurrent)
    else:
        print("   Using SEQUENTIAL processing (use --parallel for 5-10x speedup)")
        extractor = TopicKeywordExtractor()
    ingester = Neo4jIngester()
    
    try:
        # Create constraints if needed
        print("📦 Creating/verifying constraints...")
        ingester.create_constraints()
        
        # Fetch sections
        print(f"\n📚 Fetching sections from Neo4j...")
        if args.limit:
            print(f"   (Limited to {args.limit} sections)")
        
        sections = fetch_sections_from_neo4j(ingester, limit=args.limit)
        
        if not sections:
            print("❌ No sections found in database!")
            sys.exit(1)
        
        # Apply sampling if specified
        if args.sample > 1:
            sections = sections[::args.sample]
            print(f"   Sampling: Processing every {args.sample}th section ({len(sections)} total)")
        
        print(f"   Found {len(sections)} sections to process")
        
        # Extract topics and keywords
        print(f"\n🔍 Extracting topics and keywords...")
        print(f"   Model: {Config.EXTRACT_MODEL}")
        if args.parallel:
            print(f"   Using parallel processing with {args.max_concurrent} concurrent requests...")
        else:
            print(f"   This may take a while (processing sequentially)...")
        
        start_time = time.time()
        
        if args.parallel:
            # Use async parallel processing
            section_analyses = asyncio.run(
                extractor.batch_extract_from_sections_async(sections)
            )
        else:
            # Use sequential processing
            section_analyses = extractor.batch_extract_from_sections(sections)
        
        extraction_time = time.time() - start_time
        
        # Statistics
        sections_with_data = len(section_analyses)
        total_topics = sum(len(a.topics) for a in section_analyses.values())
        total_keywords = sum(len(a.keywords) for a in section_analyses.values())
        
        print(f"\n📊 Extraction Results:")
        print(f"   Total sections processed:       {len(sections):,}")
        print(f"   Sections with topics/keywords:  {sections_with_data:,} ({100*sections_with_data/len(sections):.1f}%)")
        print(f"   Total topics extracted:         {total_topics:,}")
        print(f"   Total keywords extracted:       {total_keywords:,}")
        print(f"   Avg topics per section (all):   {total_topics/len(sections):.2f}")
        print(f"   Avg keywords per section (all): {total_keywords/len(sections):.2f}")
        if sections_with_data > 0:
            print(f"   Avg topics per section (with data):   {total_topics/sections_with_data:.2f}")
            print(f"   Avg keywords per section (with data): {total_keywords/sections_with_data:.2f}")
        print(f"   Extraction time:                {extraction_time:.1f}s ({extraction_time/60:.1f} min)")
        print(f"   Time per section:               {extraction_time/len(sections):.2f}s")
        
        # Show sample topics and keywords
        if section_analyses:
            print(f"\n📝 Sample Topics (first 10 unique):")
            unique_topics = set()
            for analysis in section_analyses.values():
                for topic in analysis.topics:
                    unique_topics.add(topic.name)
                    if len(unique_topics) >= 10:
                        break
                if len(unique_topics) >= 10:
                    break
            for topic in list(unique_topics)[:10]:
                print(f"   - {topic}")
            
            print(f"\n📝 Sample Keywords (first 15 unique):")
            unique_keywords = set()
            for analysis in section_analyses.values():
                for keyword in analysis.keywords:
                    unique_keywords.add(keyword.keyword)
                    if len(unique_keywords) >= 15:
                        break
                if len(unique_keywords) >= 15:
                    break
            for keyword in list(unique_keywords)[:15]:
                print(f"   - {keyword}")
        
        # Ingest into Neo4j
        if not args.dry_run:
            print(f"\n💾 Ingesting topics and keywords into Neo4j...")
            
            # Convert to dict format expected by ingester
            section_analyses_dict = {}
            for section_id, analysis in section_analyses.items():
                section_analyses_dict[section_id] = {
                    'topics': [
                        {
                            'name': t.name,
                            'description': t.description,
                            'relevance_score': t.relevance_score
                        }
                        for t in analysis.topics
                    ],
                    'keywords': [
                        {
                            'keyword': k.keyword,
                            'relevance_score': k.relevance_score
                        }
                        for k in analysis.keywords
                    ]
                }
            
            ingestion_start = time.time()
            topics_created, keywords_created, relationships_created = ingester.ingest_topics_and_keywords(
                section_analyses_dict
            )
            ingestion_time = time.time() - ingestion_start
            
            print(f"   ✅ Created {topics_created:,} unique topics")
            print(f"   ✅ Created {keywords_created:,} unique keywords")
            print(f"   ✅ Created {relationships_created:,} relationships")
            print(f"   ⏱️  Ingestion time: {ingestion_time:.1f}s")
        else:
            print(f"\n⚠️  DRY RUN: Skipping ingestion into Neo4j")
        
        # Final summary
        print("\n" + "=" * 100)
        print("✅ EXTRACTION COMPLETE")
        print("=" * 100)
        
        if not args.dry_run:
            print("\n💡 Next Steps:")
            print("  1. Open Neo4j Browser: http://localhost:7474")
            print("  2. Query topics: MATCH (t:Topic) RETURN count(t)")
            print("  3. Query keywords: MATCH (k:Keyword) RETURN count(k)")
            print("  4. Find sections by topic:")
            print("     MATCH (s:Section)-[r:DISCUSSES_TOPIC]->(t:Topic {name: 'Ecobonus'})")
            print("     RETURN s.title, r.relevanceScore ORDER BY r.relevanceScore DESC LIMIT 10")
            print("  5. Find most discussed topics:")
            print("     MATCH (t:Topic)<-[r:DISCUSSES_TOPIC]-(s:Section)")
            print("     RETURN t.name, count(s) as sections ORDER BY sections DESC LIMIT 20")
        
    finally:
        ingester.close()


if __name__ == "__main__":
    main()

