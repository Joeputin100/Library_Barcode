import re

file_path = "/data/data/com.termux/files/home/projects/barcode/test_marc_query_tui.py"

with open(file_path, "r") as f:
    content = f.read()

# 1. Replace str(line) with line.text
content = re.sub(
    r"str\(line\)",  # Target only 'str(line)'
    r"line.text",  # Replace with 'line.text'
    content,
)

# 2. Update barcode assertions for '123' to '0000123'
content = re.sub(
    r"assert \"I understood this as: {\'type\': \'barcode\', \'value\': \'123\'}\" in results_text",
    r"assert \"I understood this as: {\'type\': \'barcode\', \'value\': \'0000123\'}\" in results_text",
    content,
)

# 3. Update barcode assertions for 'b000001' to '0000001'
content = re.sub(
    r"assert \"I understood this as: {\'type\': \'barcode\', \'value\': \'b000001\'}\" in results_text",
    r"assert \"I understood this as: {\'type\': \'barcode\', \'value\': \'0000001\'}\" in results_text",
    content,
)

# 4. Replace query_input.value = "books by Aveyard" with "author: Aveyard"
content = re.sub(
    r"query_input.value = \"books by Aveyard\"",
    r"query_input.value = \"author: Aveyard\"",
    content,
)

# 5. Update expected_json for test_barcode_list_query
old_json_pattern_literal = "expected_json = {'type': 'barcode_list', 'values': ['b100', {'type': 'barcode_range', 'start': '3957', 'end': '4000'} ]}"
new_json_string_literal = "expected_json = {'type': 'barcode_list', 'values': ['0000100', {'type': 'barcode_range', 'start': '0003957', 'end': '0004000'} ]}"

content = content.replace(old_json_pattern_literal, new_json_string_literal)


with open(file_path, "w") as f:
    f.write(content)

print(f"Successfully updated {file_path}")
