"""
Simple TOC comparison - shows side-by-side comparison of TOC vs extracted sections.
"""
import sys
from pathlib import Path
import re
from typing import List, Dict

sys.path.append(str(Path(__file__).parent.parent))

from neo4j import GraphDatabase
from pipeline.config import Config


def parse_toc_entries(toc_raw_text: str) -> List[Dict]:
    """
    Parse TOC raw text to extract section numbers and titles.
    Handles multiple formats:
    - "1.1. Title text .......14"
    - "PREMESSA ........12"
    - "2.1 Title ....5"
    """
    entries = []
    
    # Pattern: optional section number, title, dots, page number
    # Examples:
    # "1.1. Lineamenti dell'imposta .......14"
    # "PREMESSA ........................................12"
    # "2.1 Condizioni per poter accedere ........................5"
    pattern = re.compile(
        r'^\s*'                        # Leading whitespace
        r'(?:(\d+(?:\.\d+)*\.?)\s*)?'  # Optional numbered section (e.g., "1.1" or "1.1.")
        r'([A-ZÀÈÌÒÙÁÉÍÓÚ][^\n\.]{3,}?)'  # Title (starts with capital, at least 3 chars)
        r'\s+'                         # Whitespace
        r'\.{3,}'                      # At least 3 dots
        r'\s*'                         # Optional whitespace
        r'(\d+)'                       # Page number
        r'\s*$',                       # End of line
        re.MULTILINE | re.IGNORECASE
    )
    
    # Also try named sections without numbers
    named_pattern = re.compile(
        r'^\s*'
        r'(PREMESSA|INDICE|SOMMARIO|OGGETTO|PARTE\s+[IVX]+|CONCLUSIONI?)'
        r'\s+'
        r'\.{3,}'
        r'\s*'
        r'(\d+)'
        r'\s*$',
        re.MULTILINE | re.IGNORECASE
    )
    
    for match in pattern.finditer(toc_raw_text):
        section_number = match.group(1) if match.group(1) else ''
        title = match.group(2).strip()
        page_number = int(match.group(3))
        
        if section_number:
            section_number = section_number.rstrip('.')
        
        entries.append({
            'section_number': section_number or title.upper(),
            'title': title,
            'page_number': page_number,
            'raw_line': match.group(0).strip()
        })
    
    # Also try named sections
    for match in named_pattern.finditer(toc_raw_text):
        section_name = match.group(1).strip()
        page_number = int(match.group(2))
        
        # Avoid duplicates
        if not any(e['section_number'] == section_name for e in entries):
            entries.append({
                'section_number': section_name,
                'title': section_name,
                'page_number': page_number,
                'raw_line': match.group(0).strip()
            })
    
    return entries


def compare_document(driver, document_id: str):
    """Compare TOC entries with extracted sections for a single document."""
    
    query = """
    MATCH (d:Document {documentId: $documentId})-[:HAS_TOC]->(t:TableOfContents)
    MATCH (d)-[:HAS_SECTION]->(s:Section)
    RETURN d.title as docTitle,
           t.rawText as tocRawText,
           t.entryCount as entryCount,
           collect({
               sectionNumber: s.sectionNumber,
               title: s.title,
               pageNumber: s.pageNumber,
               order: s.order,
               contentLength: size(s.content)
           }) as sections
    """
    
    with driver.session(database=Config.NEO4J_DATABASE) as session:
        result = session.run(query, documentId=document_id)
        record = result.single()
        
        if not record:
            print(f"❌ Document {document_id} not found or has no TOC")
            return
        
        print("\n" + "=" * 100)
        print(f"Document: {document_id}")
        print(f"Title: {record['docTitle']}")
        print("=" * 100)
        
        # Parse TOC
        toc_entries = parse_toc_entries(record['tocRawText'])
        sections = record['sections']
        
        print(f"\n📊 Overview:")
        print(f"   TOC entries (parsed):    {len(toc_entries)}")
        print(f"   TOC entry count (meta):  {record['entryCount']}")
        print(f"   Sections extracted:      {len(sections)}")
        
        # Create lookup tables
        toc_by_number = {e['section_number']: e for e in toc_entries}
        sections_by_number = {s['sectionNumber']: s for s in sections}
        
        # Find matches
        toc_numbers = set(toc_by_number.keys())
        section_numbers = set(sections_by_number.keys())
        matched = toc_numbers & section_numbers
        
        print(f"\n✅ Matched sections:      {len(matched)}")
        print(f"⚠️  In TOC only:           {len(toc_numbers - section_numbers)}")
        print(f"⚠️  In sections only:      {len(section_numbers - toc_numbers)}")
        
        # Show first 10 TOC entries
        print(f"\n📑 TOC Entries (first 10):")
        print("-" * 100)
        for i, entry in enumerate(toc_entries[:10]):
            status = "✅" if entry['section_number'] in matched else "❌"
            print(f"{status} {entry['section_number']:15s} | {entry['title'][:50]:50s} | Page {entry['page_number']:3d}")
        
        if len(toc_entries) > 10:
            print(f"   ... and {len(toc_entries) - 10} more")
        
        # Show first 10 extracted sections
        print(f"\n📄 Extracted Sections (first 10):")
        print("-" * 100)
        sorted_sections = sorted(sections, key=lambda s: s['order'])
        for section in sorted_sections[:10]:
            status = "✅" if section['sectionNumber'] in matched else "❌"
            content_preview = f"({section['contentLength']} chars)"
            print(f"{status} {section['sectionNumber']:15s} | {section['title'][:50]:50s} | Page {section['pageNumber']:3d} {content_preview}")
        
        if len(sections) > 10:
            print(f"   ... and {len(sections) - 10} more")
        
        # Show mismatches
        if toc_numbers - section_numbers:
            print(f"\n⚠️  In TOC but NOT in extracted sections ({len(toc_numbers - section_numbers)}):")
            for sec_num in sorted(list(toc_numbers - section_numbers))[:10]:
                entry = toc_by_number[sec_num]
                print(f"   - {sec_num:15s} | {entry['title'][:60]}")
        
        if section_numbers - toc_numbers:
            print(f"\n⚠️  In extracted sections but NOT in TOC ({len(section_numbers - toc_numbers)}):")
            # These are often valid sections like OGGETTO, PREMESSA that aren't in TOC
            for sec_num in sorted(list(section_numbers - toc_numbers))[:10]:
                section = sections_by_number[sec_num]
                print(f"   - {sec_num:15s} | {section['title'][:60]} | {section['contentLength']} chars")
        
        print("\n" + "=" * 100)


def main():
    if len(sys.argv) < 2:
        print("Usage: python compare_toc_simple.py <document_id>")
        print("\nExample documents:")
        print("  CIR_17092024_2024  (63 TOC entries)")
        print("  CIR_18_2013        (171 TOC entries)")
        print("  CIR_9_2025         (15 TOC entries)")
        print("  CIR_20_2011        (32 TOC entries)")
        sys.exit(1)
    
    document_id = sys.argv[1]
    
    driver = GraphDatabase.driver(
        Config.NEO4J_URI,
        auth=(Config.NEO4J_USERNAME, Config.NEO4J_PASSWORD)
    )
    
    try:
        compare_document(driver, document_id)
    finally:
        driver.close()


if __name__ == "__main__":
    main()

