"""
Neo4j database ingestion.
"""
from typing import List
import logging
import json

try:
    from neo4j import GraphDatabase
except ImportError:
    raise ImportError("Please install neo4j driver: pip install neo4j")

from .models import Document, Section, Chunk, TableOfContents, LegalReference, Topic, Keyword
from .config import Config

logger = logging.getLogger(__name__)


class Neo4jIngester:
    """Ingest documents, sections, and chunks into Neo4j."""
    
    def __init__(self, uri: str = None, username: str = None, password: str = None, database: str = None):
        """
        Initialize Neo4j connection.
        
        Args:
            uri: Neo4j connection URI
            username: Neo4j username
            password: Neo4j password
            database: Neo4j database name
        """
        self.uri = uri or Config.NEO4J_URI
        self.username = username or Config.NEO4J_USERNAME
        self.password = password or Config.NEO4J_PASSWORD
        self.database = database or Config.NEO4J_DATABASE
        
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.username, self.password)
        )
        
        logger.info(f"Connected to Neo4j at {self.uri}")
    
    def close(self):
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def create_constraints(self):
        """Create uniqueness constraints and indexes."""
        constraints = [
            # Unique constraints on key properties
            "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.documentId IS UNIQUE",
            "CREATE CONSTRAINT toc_id IF NOT EXISTS FOR (t:TableOfContents) REQUIRE t.tocId IS UNIQUE",
            "CREATE CONSTRAINT section_id IF NOT EXISTS FOR (s:Section) REQUIRE s.sectionId IS UNIQUE",
            "CREATE CONSTRAINT chunk_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.chunkId IS UNIQUE",
            "CREATE CONSTRAINT reference_id IF NOT EXISTS FOR (lr:LegalReference) REQUIRE lr.referenceId IS UNIQUE",
            "CREATE CONSTRAINT topic_id IF NOT EXISTS FOR (t:Topic) REQUIRE t.topicId IS UNIQUE",
            "CREATE CONSTRAINT keyword_id IF NOT EXISTS FOR (k:Keyword) REQUIRE k.keywordId IS UNIQUE",

            # Indexes for common queries
            "CREATE INDEX document_type IF NOT EXISTS FOR (d:Document) ON (d.type)",
            "CREATE INDEX document_year IF NOT EXISTS FOR (d:Document) ON (d.year)",
            "CREATE INDEX section_type IF NOT EXISTS FOR (s:Section) ON (s.sectionType)",
            "CREATE INDEX reference_type IF NOT EXISTS FOR (lr:LegalReference) ON (lr.type)",
            "CREATE INDEX topic_normalized_name IF NOT EXISTS FOR (t:Topic) ON (t.normalizedName)",
            "CREATE INDEX keyword_normalized_name IF NOT EXISTS FOR (k:Keyword) ON (k.normalizedName)",
        ]

        with self.driver.session(database=self.database) as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"Created: {constraint.split('IF')[0].strip()}")
                except Exception as e:
                    logger.debug(f"Constraint may already exist: {e}")
            
            # Fulltext index for keyword search on chunk content
            try:
                session.run(
                    "CREATE FULLTEXT INDEX chunk_content_fulltext IF NOT EXISTS "
                    "FOR (c:Chunk) ON EACH [c.content]"
                )
                logger.info("Created fulltext index on Chunk.content")
            except Exception as e:
                logger.debug(f"Fulltext index may already exist: {e}")
            
            # Vector index for semantic search on chunk embeddings
            # Note: This requires Neo4j 5.11+ and embeddings to be present
            try:
                session.run("""
                    CREATE VECTOR INDEX chunk_embedding_vector IF NOT EXISTS
                    FOR (c:Chunk) ON (c.embedding)
                    OPTIONS {
                        indexConfig: {
                            `vector.dimensions`: 1536,
                            `vector.similarity_function`: 'cosine'
                        }
                    }
                """)
                logger.info("Created vector index on Chunk.embedding")
            except Exception as e:
                logger.debug(f"Vector index may already exist or requires Neo4j 5.11+: {e}")
    
    def ingest_document(
        self, 
        document: Document, 
        sections: List[Section], 
        chunks: List[Chunk],
        toc: TableOfContents = None
    ) -> bool:
        """
        Ingest a complete document with its sections, chunks, and optional TOC.
        
        Args:
            document: Document object
            sections: List of Section objects
            chunks: List of Chunk objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.driver.session(database=self.database) as session:
                # Use a transaction for atomicity
                with session.begin_transaction() as tx:
                    # 1. Create Document node
                    self._create_document_node(tx, document)
                    
                    # 2. Create TOC node and relationship (if TOC exists)
                    if toc:
                        self._create_toc_node(tx, toc)
                        self._create_has_toc_relationship(tx, document.documentId, toc.tocId)
                    
                    # 3. Create Section nodes and relationships
                    for section in sections:
                        self._create_section_node(tx, section)
                        self._create_has_section_relationship(tx, document.documentId, section)
                        
                        # Create HAS_SUBSECTION relationships for hierarchy
                        if section.parentSectionId:
                            self._create_has_subsection_relationship(
                                tx, 
                                section.parentSectionId, 
                                section
                            )
                    
                    # 4. Create NEXT_SECTION relationships (sequential order)
                    self._create_next_section_relationships(tx, sections)
                    
                    # 5. Create Chunk nodes and relationships
                    for chunk in chunks:
                        self._create_chunk_node(tx, chunk)
                        self._create_has_chunk_relationship(tx, chunk)
                    
                    # 6. Create NEXT_CHUNK relationships (sequential order within section)
                    self._create_next_chunk_relationships(tx, chunks)
                    
                    tx.commit()
                
                logger.info(f"Ingested: {document.documentId} ({len(sections)} sections, {len(chunks)} chunks)")
                return True
                
        except Exception as e:
            logger.error(f"Failed to ingest {document.documentId}: {e}")
            return False
    
    def _create_document_node(self, tx, document: Document):
        """Create a Document node."""
        query = """
        MERGE (d:Document {documentId: $documentId})
        SET d.type = $type,
            d.number = $number,
            d.year = $year,
            d.title = $title,
            d.oggetto = $oggetto,
            d.url = $url,
            d.pageCount = $pageCount,
            d.metadata = $metadata
        """
        tx.run(query, 
            documentId=document.documentId,
            type=document.type,
            number=document.number,
            year=document.year,
            title=document.title,
            oggetto=document.oggetto,
            url=document.url,
            pageCount=document.pageCount,
            metadata=json.dumps(document.metadata)  # Convert dict to JSON string
        )
    
    def _create_section_node(self, tx, section: Section):
        """Create a Section node."""
        query = """
        MERGE (s:Section {sectionId: $sectionId})
        SET s.documentId = $documentId,
            s.sectionNumber = $sectionNumber,
            s.title = $title,
            s.content = $content,
            s.sectionType = $sectionType,
            s.level = $level,
            s.pageNumber = $pageNumber,
            s.order = $order
        """
        tx.run(query,
            sectionId=section.sectionId,
            documentId=section.documentId,
            sectionNumber=section.sectionNumber,
            title=section.title,
            content=section.content,
            sectionType=section.sectionType,
            level=section.level,
            pageNumber=section.pageNumber,
            order=section.order
        )
    
    def _create_chunk_node(self, tx, chunk: Chunk):
        """Create a Chunk node."""
        query = """
        MERGE (c:Chunk {chunkId: $chunkId})
        SET c.documentId = $documentId,
            c.sectionId = $sectionId,
            c.content = $content,
            c.chunkIndex = $chunkIndex,
            c.pageNumber = $pageNumber,
            c.metadata = $metadata
        """
        tx.run(query,
            chunkId=chunk.chunkId,
            documentId=chunk.documentId,
            sectionId=chunk.sectionId,
            content=chunk.content,
            chunkIndex=chunk.chunkIndex,
            pageNumber=chunk.pageNumber,
            metadata=json.dumps(chunk.metadata)  # Convert dict to JSON string
        )
    
    def _create_toc_node(self, tx, toc: TableOfContents):
        """Create a TableOfContents node."""
        query = """
        MERGE (t:TableOfContents {tocId: $tocId})
        SET t.documentId = $documentId,
            t.rawText = $rawText,
            t.startPage = $startPage,
            t.endPage = $endPage,
            t.hasHeader = $hasHeader,
            t.headerText = $headerText,
            t.entryCount = $entryCount,
            t.detectionMethod = $detectionMethod
        """
        tx.run(query,
            tocId=toc.tocId,
            documentId=toc.documentId,
            rawText=toc.rawText,
            startPage=toc.startPage,
            endPage=toc.endPage,
            hasHeader=toc.hasHeader,
            headerText=toc.headerText,
            entryCount=toc.entryCount,
            detectionMethod=toc.detectionMethod
        )
    
    def _create_has_toc_relationship(self, tx, document_id: str, toc_id: str):
        """Create HAS_TOC relationship from Document to TableOfContents."""
        query = """
        MATCH (d:Document {documentId: $documentId})
        MATCH (t:TableOfContents {tocId: $tocId})
        MERGE (d)-[:HAS_TOC]->(t)
        """
        tx.run(query, documentId=document_id, tocId=toc_id)
    
    def _create_has_section_relationship(self, tx, document_id: str, section: Section):
        """Create HAS_SECTION relationship from Document to Section."""
        query = """
        MATCH (d:Document {documentId: $documentId})
        MATCH (s:Section {sectionId: $sectionId})
        MERGE (d)-[:HAS_SECTION {order: $order}]->(s)
        """
        tx.run(query,
            documentId=document_id,
            sectionId=section.sectionId,
            order=section.order
        )
    
    def _create_has_subsection_relationship(self, tx, parent_section_id: str, child_section: Section):
        """Create HAS_SUBSECTION relationship between sections."""
        query = """
        MATCH (parent:Section {sectionId: $parentId})
        MATCH (child:Section {sectionId: $childId})
        MERGE (parent)-[:HAS_SUBSECTION {order: $order}]->(child)
        """
        tx.run(query,
            parentId=parent_section_id,
            childId=child_section.sectionId,
            order=child_section.order
        )
    
    def _create_has_chunk_relationship(self, tx, chunk: Chunk):
        """Create HAS_CHUNK relationship from Section to Chunk."""
        query = """
        MATCH (s:Section {sectionId: $sectionId})
        MATCH (c:Chunk {chunkId: $chunkId})
        MERGE (s)-[:HAS_CHUNK {order: $order}]->(c)
        """
        tx.run(query,
            sectionId=chunk.sectionId,
            chunkId=chunk.chunkId,
            order=chunk.chunkIndex
        )
    
    def _create_next_section_relationships(self, tx, sections: List[Section]):
        """Create NEXT_SECTION relationships for sequential reading order."""
        if len(sections) < 2:
            return
        
        # Sort by order
        sorted_sections = sorted(sections, key=lambda s: s.order)
        
        for i in range(len(sorted_sections) - 1):
            current = sorted_sections[i]
            next_section = sorted_sections[i + 1]
            
            query = """
            MATCH (current:Section {sectionId: $currentId})
            MATCH (next:Section {sectionId: $nextId})
            MERGE (current)-[:NEXT_SECTION {order: $order}]->(next)
            """
            tx.run(query,
                currentId=current.sectionId,
                nextId=next_section.sectionId,
                order=i
            )
    
    def _create_next_chunk_relationships(self, tx, chunks: List[Chunk]):
        """Create NEXT_CHUNK relationships for sequential reading order within sections."""
        if len(chunks) < 2:
            return
        
        # Group chunks by section
        chunks_by_section = {}
        for chunk in chunks:
            if chunk.sectionId not in chunks_by_section:
                chunks_by_section[chunk.sectionId] = []
            chunks_by_section[chunk.sectionId].append(chunk)
        
        # Create NEXT_CHUNK relationships within each section
        for section_id, section_chunks in chunks_by_section.items():
            # Sort by chunk index
            sorted_chunks = sorted(section_chunks, key=lambda c: c.chunkIndex)
            
            for i in range(len(sorted_chunks) - 1):
                current = sorted_chunks[i]
                next_chunk = sorted_chunks[i + 1]
                
                query = """
                MATCH (current:Chunk {chunkId: $currentId})
                MATCH (next:Chunk {chunkId: $nextId})
                MERGE (current)-[:NEXT_CHUNK {order: $order}]->(next)
                """
                tx.run(query,
                    currentId=current.chunkId,
                    nextId=next_chunk.chunkId,
                    order=i
                )
    
    def ingest_legal_references(
        self,
        references_by_chunk: dict[str, List[LegalReference]]
    ) -> int:
        """
        Ingest legal references and create relationships to chunks.
        
        Args:
            references_by_chunk: Dict mapping chunk_id -> list of LegalReference objects
            
        Returns:
            Total number of references ingested
        """
        total_refs = 0
        
        try:
            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    for chunk_id, references in references_by_chunk.items():
                        for ref in references:
                            # Create LegalReference node
                            self._create_legal_reference_node(tx, ref)
                            
                            # Create REFERENCES_LAW relationship from Chunk
                            self._create_references_law_relationship(tx, chunk_id, ref.referenceId)
                            
                            total_refs += 1
                    
                    tx.commit()
            
            logger.info(f"Ingested {total_refs} legal references")
            return total_refs
            
        except Exception as e:
            logger.error(f"Failed to ingest legal references: {e}")
            return 0
    
    def _create_legal_reference_node(self, tx, ref: LegalReference):
        """Create a LegalReference node."""
        query = """
        MERGE (lr:LegalReference {referenceId: $referenceId})
        SET lr.type = $type,
            lr.citation = $citation,
            lr.number = $number,
            lr.year = $year,
            lr.articleNumber = $article,
            lr.description = $description
        """
        tx.run(query,
            referenceId=ref.referenceId,
            type=ref.type,
            citation=ref.citation,
            number=ref.number,
            year=ref.year,
            article=ref.article,
            description=ref.description
        )
    
    def _create_references_law_relationship(self, tx, chunk_id: str, reference_id: str):
        """Create REFERENCES_LAW relationship from Chunk to LegalReference."""
        query = """
        MATCH (c:Chunk {chunkId: $chunkId})
        MATCH (lr:LegalReference {referenceId: $referenceId})
        MERGE (c)-[:REFERENCES_LAW]->(lr)
        """
        tx.run(query,
            chunkId=chunk_id,
            referenceId=reference_id
        )
    
    def ingest_topics_and_keywords(
        self,
        section_analyses: dict[str, dict]
    ) -> tuple[int, int, int]:
        """
        Ingest topics and keywords and create relationships to sections.
        
        Args:
            section_analyses: Dict mapping section_id -> {
                'topics': [{'name': str, 'description': str, 'relevance_score': float}],
                'keywords': [{'keyword': str, 'relevance_score': float}]
            }
            
        Returns:
            Tuple of (topics_created, keywords_created, relationships_created)
        """
        topics_created = 0
        keywords_created = 0
        relationships_created = 0
        
        try:
            with self.driver.session(database=self.database) as session:
                with session.begin_transaction() as tx:
                    for section_id, analysis in section_analyses.items():
                        # Process topics
                        for topic_data in analysis.get('topics', []):
                            # Create or get topic
                            topic = Topic(
                                topicId=self._normalize_id(topic_data['name']),
                                name=topic_data['name'],
                                description=topic_data['description'],
                                normalizedName=topic_data['name'].lower().strip()
                            )
                            
                            # Create topic node (MERGE for deduplication)
                            created = self._create_topic_node(tx, topic)
                            if created:
                                topics_created += 1
                            
                            # Create relationship
                            self._create_discusses_topic_relationship(
                                tx,
                                section_id,
                                topic.topicId,
                                topic_data['relevance_score']
                            )
                            relationships_created += 1
                        
                        # Process keywords
                        for keyword_data in analysis.get('keywords', []):
                            # Create or get keyword
                            keyword = Keyword(
                                keywordId=self._normalize_id(keyword_data['keyword']),
                                name=keyword_data['keyword'],
                                normalizedName=keyword_data['keyword'].lower().strip()
                            )
                            
                            # Create keyword node (MERGE for deduplication)
                            created = self._create_keyword_node(tx, keyword)
                            if created:
                                keywords_created += 1
                            
                            # Create relationship
                            self._create_discusses_keyword_relationship(
                                tx,
                                section_id,
                                keyword.keywordId,
                                keyword_data['relevance_score']
                            )
                            relationships_created += 1
                    
                    tx.commit()
            
            logger.info(
                f"Ingested {topics_created} topics, {keywords_created} keywords, "
                f"{relationships_created} relationships"
            )
            return (topics_created, keywords_created, relationships_created)
            
        except Exception as e:
            logger.error(f"Failed to ingest topics/keywords: {e}")
            return (0, 0, 0)
    
    def _normalize_id(self, name: str) -> str:
        """Create a normalized ID from a name."""
        import re
        # Remove special characters, replace spaces with underscores
        normalized = re.sub(r'[^\w\s-]', '', name.lower())
        normalized = re.sub(r'[\s-]+', '_', normalized)
        return f"T_{normalized}" if normalized else f"T_{hash(name)}"
    
    def _create_topic_node(self, tx, topic: Topic) -> bool:
        """
        Create a Topic node (or update if exists).
        
        Returns:
            True if created, False if already existed
        """
        query = """
        MERGE (t:Topic {topicId: $topicId})
        ON CREATE SET 
            t.name = $name,
            t.description = $description,
            t.normalizedName = $normalizedName,
            t.created = true
        ON MATCH SET
            t.created = false
        RETURN t.created as created
        """
        result = tx.run(query,
            topicId=topic.topicId,
            name=topic.name,
            description=topic.description,
            normalizedName=topic.normalizedName
        ).single()
        return result['created'] if result else False
    
    def _create_keyword_node(self, tx, keyword: Keyword) -> bool:
        """
        Create a Keyword node (or update if exists).
        
        Returns:
            True if created, False if already existed
        """
        query = """
        MERGE (k:Keyword {keywordId: $keywordId})
        ON CREATE SET 
            k.name = $name,
            k.normalizedName = $normalizedName,
            k.created = true
        ON MATCH SET
            k.created = false
        RETURN k.created as created
        """
        result = tx.run(query,
            keywordId=keyword.keywordId,
            name=keyword.name,
            normalizedName=keyword.normalizedName
        ).single()
        return result['created'] if result else False
    
    def _create_discusses_topic_relationship(
        self,
        tx,
        section_id: str,
        topic_id: str,
        relevance_score: float
    ):
        """Create DISCUSSES_TOPIC relationship from Section to Topic."""
        query = """
        MATCH (s:Section {sectionId: $sectionId})
        MATCH (t:Topic {topicId: $topicId})
        MERGE (s)-[r:DISCUSSES_TOPIC]->(t)
        SET r.relevanceScore = $relevanceScore
        """
        tx.run(query,
            sectionId=section_id,
            topicId=topic_id,
            relevanceScore=relevance_score
        )
    
    def _create_discusses_keyword_relationship(
        self,
        tx,
        section_id: str,
        keyword_id: str,
        relevance_score: float
    ):
        """Create DISCUSSES_KEYWORD relationship from Section to Keyword."""
        query = """
        MATCH (s:Section {sectionId: $sectionId})
        MATCH (k:Keyword {keywordId: $keywordId})
        MERGE (s)-[r:DISCUSSES_KEYWORD]->(k)
        SET r.relevanceScore = $relevanceScore
        """
        tx.run(query,
            sectionId=section_id,
            keywordId=keyword_id,
            relevanceScore=relevance_score
        )
    
    def clear_database(self):
        """Delete all nodes and relationships. USE WITH CAUTION!"""
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.warning("Database cleared!")

