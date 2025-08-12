
import threading
from streamlit_app import get_book_metadata

def test_api():
    cache = {}
    event = threading.Event()
    metadata = get_book_metadata("The old man and the sea", "Hemingway, Ernest", cache, event)
    print(metadata)

if __name__ == "__main__":
    test_api()
