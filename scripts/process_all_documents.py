"""
Process all PDF documents in the downloaded_pdfs directory.
"""
import sys
from pathlib import Path
import logging
from typing import List
import time

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.pipeline import DocumentProcessingPipeline
from pipeline.config import Config
from pipeline.models import ProcessingResult
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Process PDF documents into Neo4j")
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of documents to process (for testing)'
    )
    args = parser.parse_args()
    
    print("=" * 100)
    if args.limit:
        print(f"DOCUMENT PROCESSING PIPELINE - SAMPLE ({args.limit} documents)")
    else:
        print("DOCUMENT PROCESSING PIPELINE - FULL CORPUS")
    print("=" * 100)
    
    # Validate configuration
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Initialize pipeline
    pipeline = DocumentProcessingPipeline()
    pipeline.setup()
    
    # Get all PDF files
    pdf_dir = Config.PDF_DIRECTORY
    all_pdfs = sorted(list(pdf_dir.glob("*.pdf")))
    
    # Apply limit if specified
    if args.limit:
        all_pdfs = all_pdfs[:args.limit]
        print(f"\n📁 Found {len(all_pdfs)} PDF files (limited to {args.limit} for testing)")
    else:
        print(f"\n📁 Found {len(all_pdfs)} PDF files in {pdf_dir}")
    print(f"🚀 Starting processing...\n")
    
    # Process all documents with progress bar
    results: List[ProcessingResult] = []
    start_time = time.time()
    
    # Group results by document type
    circolare_count = sum(1 for p in all_pdfs if "Circolare" in p.name or "CIRCOLARE" in p.name)
    risoluzione_count = sum(1 for p in all_pdfs if "Risoluzione" in p.name or "Ris_" in p.name)
    risposta_count = sum(1 for p in all_pdfs if "Risposta" in p.name)
    
    print(f"📊 Document types:")
    print(f"   - Circolare: {circolare_count}")
    print(f"   - Risoluzione: {risoluzione_count}")
    print(f"   - Risposta: {risposta_count}")
    print(f"   - Total: {len(all_pdfs)}\n")
    
    # Process with progress bar
    with tqdm(all_pdfs, desc="Processing documents", unit="doc") as pbar:
        for pdf_path in pbar:
            pbar.set_description(f"Processing {pdf_path.name[:40]}")
            result = pipeline.process_document(pdf_path)
            results.append(result)
            
            # Update progress bar with latest stats
            if result.success:
                pbar.set_postfix({
                    'sections': result.sections_count,
                    'chunks': result.chunks_count,
                    'time': f"{result.processing_time:.2f}s"
                })
    
    total_time = time.time() - start_time
    pipeline.close()
    
    # Generate summary statistics
    print("\n" + "=" * 100)
    print("PROCESSING SUMMARY")
    print("=" * 100)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    print(f"\n📈 Overall Statistics:")
    print(f"   Total documents:     {len(results)}")
    print(f"   ✅ Successful:       {len(successful)} ({100*len(successful)/len(results):.1f}%)")
    print(f"   ❌ Failed:           {len(failed)} ({100*len(failed)/len(results):.1f}%)")
    print(f"   Total time:          {total_time:.2f}s")
    print(f"   Avg time/doc:        {total_time/len(results):.2f}s")
    
    if successful:
        total_sections = sum(r.sections_count for r in successful)
        total_chunks = sum(r.chunks_count for r in successful)
        total_processing_time = sum(r.processing_time for r in successful)
        
        print(f"\n📊 Content Statistics:")
        print(f"   Total sections:      {total_sections:,}")
        print(f"   Total chunks:        {total_chunks:,}")
        print(f"   Avg sections/doc:    {total_sections/len(successful):.1f}")
        print(f"   Avg chunks/doc:      {total_chunks/len(successful):.1f}")
        
        # TOC statistics
        toc_docs = [r for r in successful if r.toc_extracted]
        if toc_docs:
            total_toc_pages = sum(r.toc_pages for r in toc_docs)
            print(f"\n📑 TOC Statistics:")
            print(f"   Documents with TOC:  {len(toc_docs)} ({100*len(toc_docs)/len(successful):.1f}%)")
            print(f"   Total TOC pages:     {total_toc_pages}")
            print(f"   Avg TOC pages/doc:   {total_toc_pages/len(toc_docs):.1f}")
        
        # Document type breakdown
        print(f"\n📂 By Document Type:")
        for doc_type in ["Circolare", "Risoluzione", "Risposta"]:
            type_results = [r for r in successful if doc_type in r.filename or doc_type.upper() in r.filename]
            if type_results:
                type_sections = sum(r.sections_count for r in type_results)
                type_chunks = sum(r.chunks_count for r in type_results)
                print(f"   {doc_type:12s}: {len(type_results):3d} docs, {type_sections:5,} sections, {type_chunks:5,} chunks")
    
    if failed:
        print(f"\n❌ Failed Documents ({len(failed)}):")
        for r in failed[:20]:  # Show first 20
            print(f"   - {r.filename[:70]:70s} | {r.error}")
        if len(failed) > 20:
            print(f"   ... and {len(failed) - 20} more")
    
    print("\n" + "=" * 100)
    print("✅ Processing complete!")
    print("=" * 100)
    
    print("\n💡 Next steps:")
    print("  1. Open Neo4j Browser: http://localhost:7474")
    print("  2. Run query: MATCH (d:Document) RETURN d.type, count(d) ORDER BY d.type")
    print("  3. Check TOC nodes: MATCH (t:TableOfContents) RETURN t LIMIT 10")
    print("  4. Verify sections: MATCH (s:Section) RETURN count(s)")
    print("  5. Check chunks: MATCH (c:Chunk) RETURN count(c)")


if __name__ == "__main__":
    main()

