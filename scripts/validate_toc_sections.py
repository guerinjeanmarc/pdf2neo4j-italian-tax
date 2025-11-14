"""
Validate section extraction by comparing TOC entries with actual Section nodes.
"""
import sys
from pathlib import Path
import re
from typing import List, Dict, Set, Tuple
from collections import defaultdict

sys.path.append(str(Path(__file__).parent.parent))

from neo4j import GraphDatabase
from pipeline.config import Config


class TOCValidator:
    """Validates section extraction against TOC entries."""
    
    def __init__(self):
        self.driver = GraphDatabase.driver(
            Config.NEO4J_URI,
            auth=(Config.NEO4J_USERNAME, Config.NEO4J_PASSWORD)
        )
        self.numbered_section_pattern = re.compile(r'^\s*(\d+(?:\.\d+)*)\s+(.+?)\s+(\d+)\s*$', re.MULTILINE)
        
    def close(self):
        self.driver.close()
    
    def parse_toc_entries(self, toc_raw_text: str) -> List[Dict]:
        """
        Parse TOC raw text to extract section numbers, titles, and page numbers.
        
        Returns:
            List of dicts with keys: section_number, title, page_number
        """
        entries = []
        
        # Pattern: "1.1 Title text ... 5" or "1.1 Title text 5"
        # The section number, title, and page number at the end
        for line in toc_raw_text.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            # Try numbered pattern: "1.1 Some title ... 5"
            match = self.numbered_section_pattern.match(line)
            if match:
                section_number = match.group(1)
                title = match.group(2).strip()
                page_number = int(match.group(3))
                
                # Clean up title (remove trailing dots)
                title = title.rstrip('. ')
                
                entries.append({
                    'section_number': section_number,
                    'title': title,
                    'page_number': page_number
                })
        
        return entries
    
    def get_document_sections(self, document_id: str) -> List[Dict]:
        """
        Get all sections for a document from Neo4j.
        
        Returns:
            List of dicts with section info
        """
        query = """
        MATCH (d:Document {documentId: $documentId})-[:HAS_SECTION]->(s:Section)
        RETURN s.sectionNumber as sectionNumber,
               s.title as title,
               s.pageNumber as pageNumber,
               s.sectionType as sectionType,
               s.order as order,
               size(s.content) as contentLength
        ORDER BY s.order
        """
        
        with self.driver.session(database=Config.NEO4J_DATABASE) as session:
            result = session.run(query, documentId=document_id)
            return [dict(record) for record in result]
    
    def get_documents_with_toc(self) -> List[Dict]:
        """Get all documents that have TOC nodes."""
        query = """
        MATCH (d:Document)-[:HAS_TOC]->(t:TableOfContents)
        RETURN d.documentId as documentId,
               d.type as type,
               d.title as title,
               t.rawText as tocRawText,
               t.entryCount as entryCount,
               t.startPage as startPage,
               t.endPage as endPage
        ORDER BY d.documentId
        """
        
        with self.driver.session(database=Config.NEO4J_DATABASE) as session:
            result = session.run(query)
            return [dict(record) for record in result]
    
    def compare_toc_with_sections(self, document_id: str, toc_raw_text: str) -> Dict:
        """
        Compare TOC entries with actual sections.
        
        Returns:
            Dict with comparison results
        """
        # Parse TOC entries
        toc_entries = self.parse_toc_entries(toc_raw_text)
        
        # Get actual sections
        actual_sections = self.get_document_sections(document_id)
        
        # Create lookups
        toc_by_number = {e['section_number']: e for e in toc_entries}
        sections_by_number = {s['sectionNumber']: s for s in actual_sections}
        
        # Find matches and mismatches
        toc_numbers = set(toc_by_number.keys())
        section_numbers = set(sections_by_number.keys())
        
        matched = toc_numbers & section_numbers
        in_toc_not_in_sections = toc_numbers - section_numbers
        in_sections_not_in_toc = section_numbers - toc_numbers
        
        # Check title similarity for matched sections
        title_mismatches = []
        page_mismatches = []
        
        for section_num in matched:
            toc_entry = toc_by_number[section_num]
            actual_section = sections_by_number[section_num]
            
            # Compare titles (normalize and check if similar)
            toc_title = toc_entry['title'].upper().strip()
            actual_title = actual_section['title'].upper().strip()
            
            # Remove section number from actual title if present
            actual_title_clean = re.sub(r'^\d+(?:\.\d+)*\s+', '', actual_title)
            
            if toc_title not in actual_title_clean and actual_title_clean not in toc_title:
                # Check if at least 50% of words match
                toc_words = set(toc_title.split())
                actual_words = set(actual_title_clean.split())
                if len(toc_words & actual_words) / max(len(toc_words), 1) < 0.5:
                    title_mismatches.append({
                        'section_number': section_num,
                        'toc_title': toc_entry['title'],
                        'actual_title': actual_section['title']
                    })
            
            # Compare page numbers (allow ±1 page difference)
            if abs(toc_entry['page_number'] - actual_section['pageNumber']) > 1:
                page_mismatches.append({
                    'section_number': section_num,
                    'toc_page': toc_entry['page_number'],
                    'actual_page': actual_section['pageNumber'],
                    'diff': actual_section['pageNumber'] - toc_entry['page_number']
                })
        
        return {
            'document_id': document_id,
            'toc_entries_count': len(toc_entries),
            'actual_sections_count': len(actual_sections),
            'matched_count': len(matched),
            'in_toc_not_in_sections': list(in_toc_not_in_sections),
            'in_sections_not_in_toc': list(in_sections_not_in_toc),
            'title_mismatches': title_mismatches,
            'page_mismatches': page_mismatches,
            'match_rate': len(matched) / len(toc_entries) if toc_entries else 0,
            'coverage_rate': len(matched) / len(actual_sections) if actual_sections else 0
        }
    
    def validate_all_documents(self) -> List[Dict]:
        """Validate all documents with TOC."""
        documents = self.get_documents_with_toc()
        results = []
        
        print(f"Validating {len(documents)} documents with TOC...\n")
        print("=" * 100)
        
        for doc in documents:
            print(f"\n📄 {doc['documentId']}")
            print("-" * 100)
            
            comparison = self.compare_toc_with_sections(
                doc['documentId'],
                doc['tocRawText']
            )
            
            results.append(comparison)
            
            # Print summary
            print(f"  TOC entries:        {comparison['toc_entries_count']}")
            print(f"  Actual sections:    {comparison['actual_sections_count']}")
            print(f"  Matched:            {comparison['matched_count']}")
            print(f"  Match rate:         {comparison['match_rate']*100:.1f}%")
            print(f"  Coverage rate:      {comparison['coverage_rate']*100:.1f}%")
            
            if comparison['in_toc_not_in_sections']:
                print(f"  ⚠️  In TOC but not in sections: {len(comparison['in_toc_not_in_sections'])}")
                for sec_num in comparison['in_toc_not_in_sections'][:5]:
                    print(f"     - {sec_num}")
                if len(comparison['in_toc_not_in_sections']) > 5:
                    print(f"     ... and {len(comparison['in_toc_not_in_sections']) - 5} more")
            
            if comparison['in_sections_not_in_toc']:
                print(f"  ⚠️  In sections but not in TOC: {len(comparison['in_sections_not_in_toc'])}")
                for sec_num in comparison['in_sections_not_in_toc'][:5]:
                    print(f"     - {sec_num}")
                if len(comparison['in_sections_not_in_toc']) > 5:
                    print(f"     ... and {len(comparison['in_sections_not_in_toc']) - 5} more")
            
            if comparison['title_mismatches']:
                print(f"  ⚠️  Title mismatches: {len(comparison['title_mismatches'])}")
            
            if comparison['page_mismatches']:
                print(f"  ⚠️  Page mismatches (>±1): {len(comparison['page_mismatches'])}")
                for mismatch in comparison['page_mismatches'][:3]:
                    print(f"     - {mismatch['section_number']}: TOC page {mismatch['toc_page']} → Actual page {mismatch['actual_page']} (diff: {mismatch['diff']:+d})")
                if len(comparison['page_mismatches']) > 3:
                    print(f"     ... and {len(comparison['page_mismatches']) - 3} more")
        
        return results
    
    def generate_summary(self, results: List[Dict]):
        """Generate overall validation summary."""
        print("\n" + "=" * 100)
        print("VALIDATION SUMMARY")
        print("=" * 100)
        
        total_docs = len(results)
        total_toc_entries = sum(r['toc_entries_count'] for r in results)
        total_sections = sum(r['actual_sections_count'] for r in results)
        total_matched = sum(r['matched_count'] for r in results)
        
        avg_match_rate = sum(r['match_rate'] for r in results) / total_docs if total_docs else 0
        avg_coverage_rate = sum(r['coverage_rate'] for r in results) / total_docs if total_docs else 0
        
        print(f"\n📊 Overall Statistics:")
        print(f"   Documents validated:      {total_docs}")
        print(f"   Total TOC entries:        {total_toc_entries:,}")
        print(f"   Total sections extracted: {total_sections:,}")
        print(f"   Total matched:            {total_matched:,}")
        print(f"   Average match rate:       {avg_match_rate*100:.1f}%")
        print(f"   Average coverage rate:    {avg_coverage_rate*100:.1f}%")
        
        # Documents with issues
        docs_with_missing = [r for r in results if r['in_toc_not_in_sections']]
        docs_with_extra = [r for r in results if r['in_sections_not_in_toc']]
        docs_with_title_mismatch = [r for r in results if r['title_mismatches']]
        docs_with_page_mismatch = [r for r in results if r['page_mismatches']]
        
        print(f"\n⚠️  Documents with Issues:")
        print(f"   Missing sections (in TOC, not extracted):  {len(docs_with_missing)}")
        print(f"   Extra sections (not in TOC):               {len(docs_with_extra)}")
        print(f"   Title mismatches:                          {len(docs_with_title_mismatch)}")
        print(f"   Page mismatches (>±1):                     {len(docs_with_page_mismatch)}")
        
        # Best and worst performers
        if results:
            best = max(results, key=lambda r: r['match_rate'])
            worst = min(results, key=lambda r: r['match_rate'])
            
            print(f"\n🏆 Best Match Rate:")
            print(f"   {best['document_id']}: {best['match_rate']*100:.1f}% ({best['matched_count']}/{best['toc_entries_count']})")
            
            print(f"\n⚠️  Worst Match Rate:")
            print(f"   {worst['document_id']}: {worst['match_rate']*100:.1f}% ({worst['matched_count']}/{worst['toc_entries_count']})")
        
        print("\n" + "=" * 100)


def main():
    validator = TOCValidator()
    
    try:
        results = validator.validate_all_documents()
        validator.generate_summary(results)
        
        print("\n💡 Next Steps:")
        print("  1. Review documents with low match rates")
        print("  2. Check if 'in_sections_not_in_toc' are valid sections (PREMESSA, OGGETTO, etc.)")
        print("  3. Investigate 'in_toc_not_in_sections' to see if sections were missed")
        print("  4. Page mismatches are usually OK (TOC page numbers can be approximate)")
        
    finally:
        validator.close()


if __name__ == "__main__":
    main()

