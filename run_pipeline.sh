#!/bin/bash

echo "=========================================="
echo "🚀 FULL PIPELINE EXECUTION"
echo "=========================================="
echo ""

# Activate virtual environment
source .venv/bin/activate

# Step 1: Clear database (optional - comment out if you want to keep existing data)
echo "🗑️  Step 1: Clearing database..."
python -c "
from pipeline.neo4j_ingester import Neo4jIngester
ingester = Neo4jIngester()
ingester.clear_database()
ingester.close()
print('✅ Database cleared!')
"
echo ""

# Step 2: Process all PDFs
echo "📄 Step 2: Processing PDFs → Neo4j..."
python scripts/process_all_documents.py
if [ $? -ne 0 ]; then
    echo "❌ Step 2 failed!"
    exit 1
fi
echo ""

# Step 2.5: Generate chunk embeddings
echo "🧠 Step 2.5: Generating chunk embeddings..."
python scripts/generate_chunk_embeddings.py --parallel --max-concurrent 5
if [ $? -ne 0 ]; then
    echo "❌ Step 2.5 failed!"
    exit 1
fi
echo ""

# Step 3: Extract legal references
echo "⚖️  Step 3: Extracting legal references..."
python scripts/extract_legal_references.py --parallel
if [ $? -ne 0 ]; then
    echo "❌ Step 3 failed!"
    exit 1
fi
echo ""

# Step 3.5: Link legal references to documents
echo "🔗 Step 3.5: Linking legal references to documents..."
python scripts/link_references_to_documents.py
if [ $? -ne 0 ]; then
    echo "❌ Step 3.5 failed!"
    exit 1
fi
echo ""

# Step 4: Extract topics and keywords
echo "🏷️  Step 4: Extracting topics and keywords..."
python scripts/extract_topics_keywords.py --parallel
if [ $? -ne 0 ]; then
    echo "❌ Step 4 failed!"
    exit 1
fi
echo ""

echo "=========================================="
echo "✅ PIPELINE COMPLETE!"
echo "=========================================="
echo ""
echo "💡 Next Steps:"
echo "  1. Open Neo4j Browser: http://localhost:7474"
echo "  2. Run verification queries:"
echo "     MATCH (n) RETURN labels(n)[0] as NodeType, count(n) as Count ORDER BY Count DESC"
echo "  3. Explore the graph"
echo "  4. Build your RAG agent"
echo ""

