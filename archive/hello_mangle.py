from mangle import mangle

print("Testing basic Mangle functionality...")

# Create instance - this should work if installation was successful
m = mangle()
print("✅ Mangle instance created")

# Add a simple fact
m.add_fact("test", "success")
print("✅ Fact added")

# Try a simple query
try:
    result = list(m.query("test", "success"))
    print(f"✅ Query result: {result}")
    print("🎉 Mangle is working!")
except Exception as e:
    print(f"❌ Query failed: {e}")