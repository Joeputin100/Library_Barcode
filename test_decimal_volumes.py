#!/usr/bin/env python3
"""
Test decimal volume number extraction with mixed number conversion
"""

from vertex_grounded_research import extract_volume_number

def test_decimal_volumes():
    """Test decimal volume extraction and mixed number conversion"""
    
    test_cases = [
        # Standard whole numbers
        "Vol. 25",
        "Volume 1", 
        "v. 16",
        "Book 3",
        "#5",
        
        # Decimal volumes that should convert to mixed numbers
        "Vol. 3.5",      # Should become "3½"
        "Volume 0.5",    # Should become "½"
        "v. 7.5",        # Should become "7½"
        "Book 12.5",     # Should become "12½"
        "#2.5",          # Should become "2½"
        
        # Other decimals (should remain as decimals)
        "Vol. 3.1",      # Should remain "3.1"
        "Volume 2.75",   # Should remain "2.75"
        "v. 1.25",       # Should remain "1.25"
        
        # Edge cases
        "Dungeons & Dragons, Dungeon Master's Guide (v. 3.5)",  # Should become "3½"
        "Some series v. 0.5 special edition",                   # Should become "½"
        "Book 4.5: Between volumes",                            # Should become "4½"
    ]
    
    print("Testing Decimal Volume Extraction:")
    print("=" * 60)
    
    for test_case in test_cases:
        volume = extract_volume_number(test_case)
        print(f"'{test_case}' -> Volume: {volume}")

if __name__ == "__main__":
    test_decimal_volumes()