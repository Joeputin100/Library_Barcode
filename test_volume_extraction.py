#!/usr/bin/env python3
"""
Test volume number extraction function
"""

from vertex_grounded_research import extract_volume_number

def test_volume_extraction():
    """Test volume extraction with various patterns"""
    
    test_cases = [
        "Noragami: Stray God, Vol. 25 [Source: WorldCat, Library of Congress]",
        "Naruto, Vol. 1",
        "My Hero Academia, Vol. 16", 
        "My Hero Academia; v. 1 (Source: WorldCat)",
        "One-Punch Man, Vol. 10 (Source: WorldCat, Library of Congress)",
        "Blue Exorcist, Vol. 16",
        "Demon Slave, Vol. 1 (light novel) (Source: Library of Congress, WorldCat)",
        "Reincarnated as a Sword. Volume 1",
        "Reborn as a Vending Machine, I Now Wander the Dungeon, Vol. 6 (light novel)",
        "Demon Slayer: Kimetsu no Yaiba, Vol. 16",
        "Demon Slayer: Kimetsu No Yaiba, Vol. 16 (English Edition)",
        "Vampire Knight, Vol. 18",
        "Attack on Titan 18 (with source: Library of Congress, WorldCat, VIZ Media)",
        "Naruto, Vol. 70",
        "Dragon Ball Z, Vol. 1 (VIZBIG Edition)",
        "Naruto, Vol. 48",
        "Naruto, Vol. 50",
        "Book 16 of some series",
        "v1 of something",
        "1st Volume of something"
    ]
    
    print("Testing Volume Extraction:")
    print("=" * 50)
    
    for test_case in test_cases:
        volume = extract_volume_number(test_case)
        print(f"'{test_case}' -> Volume: {volume}")

if __name__ == "__main__":
    test_volume_extraction()