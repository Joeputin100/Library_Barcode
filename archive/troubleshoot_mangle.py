import sys
print(f"Python version: {sys.version}")
print(f"Python path: {sys.path}")

try:
    from mangle import Mangle
    print("✅ Mangle import successful")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Trying to reinstall...")
    
    # Try reinstalling
    import subprocess
    result = subprocess.run([sys.executable, "-m", "pip", "install", "mangle", "--no-build-isolation"], 
                          capture_output=True, text=True)
    print("Install output:", result.stdout)
    if result.stderr:
        print("Install errors:", result.stderr)
