#!/usr/bin/env python3
"""
Test the document processing pipeline on a few sample documents.
"""
import sys
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.pipeline import DocumentProcessingPipeline
from pipeline.config import Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Test pipeline on sample documents."""
    print("=" * 80)
    print("DOCUMENT PROCESSING PIPELINE - TEST")
    print("=" * 80)
    
    try:
        # Validate configuration
        Config.validate()
        
        # Initialize pipeline
        pipeline = DocumentProcessingPipeline()
        
        # Setup database
        pipeline.setup()
        
        # Select test documents
        test_docs = [
            "Circolare_CPB_17092024.pdf",
            "Circolare_20_del_13_05_2011_Circolare_20e_1.pdf",
            "Risposta_676_07.10.2021.pdf",
            "Risoluzione_n._3__18_01_22_1.pdf",
        ]
        
        # Process each test document
        results = []
        pdf_dir = Config.PDF_DIRECTORY
        
        for doc_name in test_docs:
            pdf_path = pdf_dir / doc_name
            if pdf_path.exists():
                result = pipeline.process_document(pdf_path)
                results.append(result)
            else:
                print(f"⚠️  Warning: {doc_name} not found, skipping...")
        
        # Print summary
        pipeline.print_summary(results)
        
        # Close pipeline
        pipeline.close()
        
        print("\n✅ Test completed!")
        print("\n💡 Next steps:")
        print("  1. Open Neo4j Browser: http://localhost:7474")
        print("  2. Run query: MATCH (d:Document) RETURN d LIMIT 10")
        print("  3. Verify sections and chunks are created")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

