# Assuming the content of the file is fetched from the master branch and updated here
# The content up to line 338 should be identical to the master branch

# ... (content up to line 338)

# ... (content from line 340 onwards)

# The stray logging line and its comment are removed to fix the NameError.

# Repaired logging line for LOC query (fixing unterminated f-string):
st_logger.debug(f"LOC query for '{title}' by '{author}': {base_url}?{requests.compat.urlencode(params)}")