#!/usr/bin/env python3
"""
Generate barcode labels for the test batch records.
This script uses the label_generator module to create PDF labels.
"""

from label_generator import generate_pdf_sheet
import pandas as pd

def create_label_data_from_test_batch():
    """Create label data from our test batch records."""
    
    # Create the same DataFrame as in export_test_batch.py
    data = [
        # Record 1: Treasures by Belva Plain
        {
            'Title': 'Treasures',
            "Author's Name": 'Plain, Belva',
            'Publication Year': '1992',
            'Series Title': '',
            'Series Volume': '',
            'Call Number': 'PS3566.L253 T74 1992',
            'Holdings Barcode': 'B000001'
        },
        
        # Record 2: Random Winds by Belva Plain
        {
            'Title': 'Random Winds',
            "Author's Name": 'Plain, Belva',
            'Publication Year': '1980',
            'Series Title': '',
            'Series Volume': '',
            'Call Number': 'PS3566.L253 R3',
            'Holdings Barcode': 'B000002'
        },
        
        # Record 3: Mindfulness: The Path to the Deathless
        {
            'Title': 'Mindfulness: The Path to the Deathless',
            "Author's Name": 'Sumedho, Ajahn',
            'Publication Year': '1987',
            'Series Title': 'Wheel Publication',
            'Series Volume': '365/366',
            'Call Number': 'BQ5630.M55 S86 1987',
            'Holdings Barcode': 'B000003'
        },
        
        # Record 4: Guard of Honor by James Gould Cozzens
        {
            'Title': 'Guard of Honor',
            "Author's Name": 'Cozzens, James Gould',
            'Publication Year': '1948',
            'Series Title': '',
            'Series Volume': '',
            'Call Number': 'PS3505.O98 G8',
            'Holdings Barcode': 'B000004'
        },
        
        # Record 6: California Veterans Resource Book
        {
            'Title': 'California Veterans Resource Book',
            "Author's Name": 'California Department of Veterans Affairs',
            'Publication Year': '2023',
            'Series Title': '',
            'Series Volume': '8th Edition',
            'Call Number': 'UB 356.C2',
            'Holdings Barcode': 'B000006'
        },
        
        # Record 7: The Great American Baseball Card Book
        {
            'Title': 'The Great American Baseball Card Flipping, Trading and Bubble Gum Book',
            "Author's Name": 'Boyd, Brendan C. and Harris, Fred C.',
            'Publication Year': '1973',
            'Series Title': '',
            'Series Volume': '',
            'Call Number': 'GV875.A1 B68',
            'Holdings Barcode': 'B000007'
        }
    ]
    
    return data

def main():
    """Main function to generate barcode labels."""
    print("Creating label data from test batch...")
    
    # Create label data
    label_data = create_label_data_from_test_batch()
    
    print(f"Created label data for {len(label_data)} records:")
    for i, book in enumerate(label_data):
        print(f"  {i+1}. {book['Title']} - {book['Holdings Barcode']}")
    
    # Generate PDF labels
    print("\nGenerating PDF labels...")
    pdf_data = generate_pdf_sheet(label_data)
    
    # Save to file
    output_file = "test_batch_labels.pdf"
    with open(output_file, "wb") as f:
        f.write(pdf_data)
    
    print(f"\n✅ Successfully generated labels for {len(label_data)} records")
    print(f"📄 Labels saved to: {output_file}")
    
    return True

if __name__ == "__main__":
    main()