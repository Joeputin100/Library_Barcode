import mangle

print("Contents of mangle module:")
for item in dir(mangle):
    if not item.startswith('_'):  # Skip private items
        print(f"  {item}")

# Check if Mangle class exists
if hasattr(mangle, 'Mangle'):
    print("✅ Mangle class found")
    m = mangle.Mangle()
else:
    print("❌ Mangle class not found")
