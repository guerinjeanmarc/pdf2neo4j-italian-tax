"""
Link LegalReference nodes to Document nodes based on canonical IDs.

After running legal reference extraction with the updated canonical ID format,
this script creates REFERS_TO relationships between LegalReference and Document nodes
when their IDs match.
"""
import sys
from pathlib import Path
import logging
from typing import Dict, List, Tuple

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.config import Config
from pipeline.neo4j_ingester import Neo4jIngester

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_all_legal_references(ingester: Neo4jIngester) -> List[Dict]:
    """
    Fetch all LegalReference nodes from Neo4j.
    
    Returns:
        List of dicts with referenceId, type, number, year, citation
    """
    query = """
    MATCH (lr:LegalReference)
    RETURN 
        lr.referenceId as referenceId,
        lr.type as type,
        lr.number as number,
        lr.year as year,
        lr.citation as citation
    ORDER BY lr.referenceId
    """
    
    with ingester.driver.session(database=ingester.database) as session:
        result = session.run(query)
        references = [dict(record) for record in result]
    
    return references


def fetch_all_documents(ingester: Neo4jIngester) -> Dict[str, str]:
    """
    Fetch all Document nodes and their IDs.
    
    Returns:
        Dict mapping documentId -> document type
    """
    query = """
    MATCH (d:Document)
    RETURN d.documentId as documentId, d.type as type
    ORDER BY d.documentId
    """
    
    with ingester.driver.session(database=ingester.database) as session:
        result = session.run(query)
        documents = {record['documentId']: record['type'] for record in result}
    
    return documents


def categorize_references(
    references: List[Dict],
    documents: Dict[str, str]
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Categorize references into matched, unmatched, and external.
    
    Returns:
        Tuple of (matched, unmatched, external) reference lists
    """
    matched = []
    unmatched = []
    external = []
    
    for ref in references:
        ref_id = ref['referenceId']
        
        # Check if it's an external reference (law/decree)
        if ref_id.startswith('EXT_') or ref_id.startswith('REF_'):
            external.append(ref)
        # Check if it matches a document
        elif ref_id in documents:
            matched.append(ref)
        # Our document type, but not in database
        else:
            unmatched.append(ref)
    
    return matched, unmatched, external


def create_refers_to_relationships(
    ingester: Neo4jIngester,
    matched_references: List[Dict]
) -> int:
    """
    Create REFERS_TO relationships between LegalReferences and Documents.
    
    Args:
        ingester: Neo4jIngester instance
        matched_references: List of references that match documents
        
    Returns:
        Number of relationships created
    """
    query = """
    UNWIND $batch as item
    MATCH (lr:LegalReference {referenceId: item.referenceId})
    MATCH (d:Document {documentId: item.referenceId})
    MERGE (lr)-[r:REFERS_TO]->(d)
    RETURN count(r) as created
    """
    
    # Prepare batch data (just the referenceIds)
    batch_data = [{'referenceId': ref['referenceId']} for ref in matched_references]
    
    with ingester.driver.session(database=ingester.database) as session:
        result = session.run(query, batch=batch_data)
        created = result.single()['created']
    
    return created


def print_statistics(
    matched: List[Dict],
    unmatched: List[Dict],
    external: List[Dict],
    total: int
):
    """Print detailed statistics about the linking results."""
    print("\n" + "=" * 100)
    print("📊 ENTITY LINKING STATISTICS")
    print("=" * 100)
    
    print(f"\n📚 Total Legal References: {total:,}")
    print(f"\n✅ Matched (linked to documents in DB):")
    print(f"   Count: {len(matched):,} ({100*len(matched)/total:.1f}%)")
    
    print(f"\n⚠️  Unmatched (document type, but not in DB):")
    print(f"   Count: {len(unmatched):,} ({100*len(unmatched)/total:.1f}%)")
    
    print(f"\nℹ️  External (laws/decrees not in corpus):")
    print(f"   Count: {len(external):,} ({100*len(external)/total:.1f}%)")
    
    # Break down by type
    print(f"\n📋 Breakdown by Reference Type:")
    
    type_stats = {}
    for ref in matched + unmatched + external:
        ref_type = ref.get('type', 'unknown')
        if ref_type not in type_stats:
            type_stats[ref_type] = {'matched': 0, 'unmatched': 0, 'external': 0}
    
    for ref in matched:
        type_stats[ref['type']]['matched'] += 1
    for ref in unmatched:
        type_stats[ref['type']]['unmatched'] += 1
    for ref in external:
        type_stats[ref['type']]['external'] += 1
    
    for ref_type, counts in sorted(type_stats.items()):
        total_type = counts['matched'] + counts['unmatched'] + counts['external']
        print(f"\n   {ref_type}:")
        print(f"      Total: {total_type}")
        print(f"      ✅ Matched: {counts['matched']}")
        print(f"      ⚠️  Unmatched: {counts['unmatched']}")
        print(f"      ℹ️  External: {counts['external']}")
    
    # Show some examples
    if matched:
        print(f"\n✅ Example Matches (first 10):")
        for ref in matched[:10]:
            print(f"   {ref['referenceId']:<20} ← {ref['citation'][:70]}")
    
    if unmatched:
        print(f"\n⚠️  Example Unmatched (first 10):")
        for ref in unmatched[:10]:
            print(f"   {ref['referenceId']:<20} ← {ref['citation'][:70]}")
    
    if external:
        print(f"\nℹ️  Example External (first 10):")
        for ref in external[:10]:
            print(f"   {ref['referenceId']:<20} ← {ref['citation'][:70]}")


def main():
    print("=" * 100)
    print("ENTITY LINKING: LegalReference → Document")
    print("=" * 100)
    
    # Validate config
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Initialize Neo4j connection
    print("\n📋 Initializing Neo4j connection...")
    ingester = Neo4jIngester()
    
    try:
        # Fetch all legal references
        print("\n📚 Fetching legal references...")
        references = fetch_all_legal_references(ingester)
        print(f"   Found {len(references):,} legal references")
        
        if not references:
            print("❌ No legal references found in database!")
            print("   Run: python scripts/extract_legal_references.py first")
            sys.exit(1)
        
        # Fetch all documents
        print("\n📄 Fetching documents...")
        documents = fetch_all_documents(ingester)
        print(f"   Found {len(documents):,} documents")
        print(f"   Document types: {set(documents.values())}")
        
        # Categorize references
        print("\n🔗 Matching references to documents...")
        matched, unmatched, external = categorize_references(references, documents)
        
        # Print statistics
        print_statistics(matched, unmatched, external, len(references))
        
        # Create relationships
        if matched:
            print(f"\n💾 Creating REFERS_TO relationships...")
            created = create_refers_to_relationships(ingester, matched)
            print(f"   ✅ Created {created:,} REFERS_TO relationships")
        else:
            print(f"\n⚠️  No matches found, no relationships to create")
        
        # Final summary
        print("\n" + "=" * 100)
        print("✅ ENTITY LINKING COMPLETE")
        print("=" * 100)
        
        print("\n💡 Next Steps:")
        print("  1. Verify relationships in Neo4j Browser:")
        print("     MATCH (lr:LegalReference)-[:REFERS_TO]->(d:Document)")
        print("     RETURN lr.referenceId, lr.citation, d.documentId, d.title")
        print("     LIMIT 10")
        print("\n  2. Find chunks that reference specific documents:")
        print("     MATCH (c:Chunk)-[:REFERENCES_LAW]->(lr)-[:REFERS_TO]->(d:Document)")
        print("     WHERE d.documentId = 'CIR_20_2011'")
        print("     RETURN c.content, lr.citation")
        print("\n  3. Count references per document:")
        print("     MATCH (lr:LegalReference)-[:REFERS_TO]->(d:Document)")
        print("     RETURN d.documentId, d.title, count(lr) as references")
        print("     ORDER BY references DESC")
        
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        ingester.close()


if __name__ == "__main__":
    main()

