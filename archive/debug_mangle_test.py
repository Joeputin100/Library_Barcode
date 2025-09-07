"""
Debug Mangle integration issue
"""

import subprocess
import tempfile
import os

def test_mangle_direct():
    """Test Mangle directly"""
    
    # Create test data
    test_data = """
marc_record(/B12345, "Sample Book", "John Doe", "FIC DOE", "123456", "9781234567890").
google_books_data(/B12345, "Sample Book Enhanced", "Johnathan Doe", "Fiction,Mystery", "FIC", "Sample Series", "1", "2023", "Enhanced description").
vertex_ai_data(/B12345, "Mystery", 0.85, "https://example.com", "Good reviews", "Mystery,Thriller", "Sample Series Info", "2023").
"""
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.mg', delete=False) as f:
        f.write(test_data)
        data_file = f.name
    
    try:
        # Build command
        cmd = [
            'go', 'run', 'interpreter/mg/mg.go',
            '-exec', 'enriched_book(Barcode, Title, Author, Classification)',
            '-load', f'mangle_final_rules.mg,{data_file}'
        ]
        
        print(f"Command: {' '.join(cmd)}")
        print(f"Working directory: mangle")
        
        # Run command
        result = subprocess.run(
            cmd,
            cwd='mangle',
            capture_output=True,
            text=True
        )
        
        print(f"Return code: {result.returncode}")
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")
        
        return result.returncode == 0
        
    finally:
        os.unlink(data_file)

if __name__ == "__main__":
    success = test_mangle_direct()
    print(f"Test {'PASSED' if success else 'FAILED'}")