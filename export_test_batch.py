#!/usr/bin/env python3
"""
Export the verified test batch records to MARC format for Atriuum import.
This script uses the research data from the agentic review process.
"""

import pandas as pd
from marc_exporter import convert_df_to_marc, write_marc_file

# Create DataFrame with verified records from our research
def create_test_batch_dataframe():
    """Create a DataFrame with the 7 verified records from our research."""
    
    data = [
        # Record 1: Treasures by Belva Plain
        {
            'holding_barcode': 'B000001',
            'title': 'Treasures',
            'author': 'Plain, Belva',
            'isbn': '038530603X',
            'price': '60.00',
            'call_number': 'PS3566.L253 T74 1992',
            'local_call_number': 'FIC PLAIN',
            'publication_date': '1992',
            'series_title': '',
            'series_number': '',
            'description': 'A family saga exploring relationships and inheritance across generations.',
            'subject_headings': 'Domestic fiction, Jewish families -- Fiction, Family sagas',
            'notes': 'GENRE: Domestic fiction, Family saga; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $60.00'
        },
        
        # Record 2: Random Winds by Belva Plain (corrected from Beva)
        {
            'holding_barcode': 'B000002',
            'title': 'Random Winds',
            'author': 'Plain, Belva',  # Corrected from "Beva"
            'isbn': '0440038549',
            'price': '40.00',
            'call_number': 'PS3566.L26 R36',
            'local_call_number': 'FIC PLAIN',
            'publication_date': '1980',
            'series_title': '',
            'series_number': '',
            'description': 'Random Winds follows three generations of a medical family through triumph and tragedy. The story spans from the early 20th century through World War II, exploring themes of ambition, love, and the medical profession.',
            'subject_headings': 'Domestic fiction, Family -- Fiction, Physicians -- Fiction, Man-woman relationships -- Fiction, Medical novels',
            'notes': 'GENRE: Domestic fiction, Family saga, Medical fiction; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $40.00'
        },
        
        # Record 3: Mindfulness: The Path to the Deathless by Ajahn Sumedho
        {
            'holding_barcode': 'B000003',
            'title': 'Mindfulness: The Path to the Deathless',
            'author': 'Sumedho, Ajahn',
            'isbn': '094667223X',
            'price': '95.00',
            'call_number': 'BQ5630.M55 S86',
            'local_call_number': '294.3 SUM',
            'publication_date': '1987',
            'series_title': 'Wheel Publication',
            'series_number': '365/366',
            'description': 'Ajahn Sumedho, a Western Buddhist monk in the Thai Forest Tradition, presents profound teachings on mindfulness meditation and the path to liberation. This book offers practical guidance on developing awareness and understanding the nature of mind.',
            'subject_headings': 'Buddhism -- Doctrines, Meditation -- Buddhism, Spiritual life -- Buddhism, Theravada Buddhism, Mindfulness (Psychology)',
            'notes': 'GENRE: Buddhist religious text, Meditation guide; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $95.00'
        },
        
        # Record 4: Guard of Honor by James Gould Cozzens
        {
            'holding_barcode': 'B000004',
            'title': 'Guard of Honor',
            'author': 'Cozzens, James Gould',
            'isbn': '0156375301',  # 1979 edition ISBN
            'price': '85.00',
            'call_number': 'PS3505.O99 G8',
            'local_call_number': 'FIC COZZENS',
            'publication_date': '1948',
            'series_title': '',
            'series_number': '',
            'description': 'Winner of the 1949 Pulitzer Prize, this novel examines three days at a Florida Army Air Forces base during World War II. The story explores complex racial tensions, military bureaucracy, and moral dilemmas through multiple perspectives.',
            'subject_headings': 'World War, 1939-1945 -- Fiction, Military bases -- Fiction, United States. Army Air Forces -- Fiction, Race relations -- Fiction, Pulitzer Prize winner -- 1949',
            'notes': 'GENRE: Literary fiction, Military fiction, Pulitzer Prize winner; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $85.00; AWARD: Pulitzer Prize 1949'
        },
        
        # Record 6: California Veterans Resource Book (8th Edition)
        {
            'holding_barcode': 'B000006',
            'title': 'California Veterans Resource Book',
            'author': 'California Department of Veterans Affairs',
            'isbn': '',  # Government documents often lack ISBN
            'price': '40.00',
            'call_number': 'UB357 .C35',
            'local_call_number': '355 CAL',
            'publication_date': '2023',  # Estimated based on 8th edition
            'series_title': '',
            'series_number': '8th Edition',
            'description': 'Comprehensive guide to benefits, services, and resources available to California veterans. Includes information on education benefits, healthcare, disability compensation, home loans, employment services, and state-specific programs.',
            'subject_headings': 'Veterans -- California -- Handbooks, manuals, etc., Veterans -- Services for -- California, California. Department of Veterans Affairs, Veterans -- Benefits -- California, Military discharge -- California',
            'notes': 'GENRE: Government document, Reference manual; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $40.00; TYPE: Government publication'
        },
        
        # Record 7: The Great American Baseball Card Book (ISBN: 091664202X)
        {
            'holding_barcode': 'B000007',
            'title': 'The Great American Baseball Card Flipping, Trading and Bubble Gum Book',
            'author': 'Boyd, Brendan C. and Harris, Fred C.',
            'isbn': '091664202X',
            'price': '150.00',
            'call_number': 'GV875.A1 B69',
            'local_call_number': '796.357 BOY',
            'publication_date': '1973',
            'series_title': '',
            'series_number': '',
            'description': 'Classic reference work exploring the history and cultural significance of baseball cards. Features detailed analysis of iconic cards, player biographies, and the evolution of baseball card design.',
            'subject_headings': 'Baseball cards -- Collectors and collecting, Baseball -- United States -- History, Sports memorabilia -- United States, Trading cards -- Collectors and collecting, Baseball players -- United States -- Biography',
            'notes': 'GENRE: Sports reference, Collectibles, Baseball history; LANGUAGE: English; MATERIAL: Standard print; INSURANCE_VALUE: $150.00; TYPE: Collectible reference'
        }
    ]
    
    return pd.DataFrame(data)

def main():
    """Main function to export the test batch to MARC format."""
    print("Creating test batch DataFrame...")
    
    # Create DataFrame with verified records
    df = create_test_batch_dataframe()
    
    print(f"Created DataFrame with {len(df)} verified records:")
    for i, row in df.iterrows():
        print(f"  {i+1}. {row['title']} by {row['author']}")
    
    # Convert to MARC records
    print("\nConverting to MARC records...")
    marc_records = convert_df_to_marc(df)
    
    # Write to MARC file
    output_file = "test_batch_export.marc"
    write_marc_file(marc_records, output_file)
    
    print(f"\n✅ Successfully exported {len(marc_records)} records to {output_file}")
    print("\nRecords exported:")
    for i, record in enumerate(marc_records):
        title = record['245'].value() if '245' in record else "No title"
        author = record['100'].value() if '100' in record else "No author"
        print(f"  {i+1}. {title} - {author}")
    
    return True

if __name__ == "__main__":
    main()