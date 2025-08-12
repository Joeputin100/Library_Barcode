import requests
import xml.etree.ElementTree as ET

def query_loc_by_isbn(isbn: str):
    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.isbn="{isbn}"'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    params = {
        "version": "1.1",
        "operation": "searchRetrieve",
        "query": query,
        "maximumRecords": "1",
        "recordSchema": "marcxml"
    }

    print(f"Querying LOC API for ISBN: {isbn}")
    print(f"API URL: {base_url}")
    print(f"Query: {query}")

    try:
        response = requests.get(base_url, params=params, timeout=30, headers=headers)
        response.raise_for_status() # Raise an exception for HTTP errors
        raw_xml = response.content.decode('utf-8')
        print("\n--- Raw MARCXML Response ---")
        print(raw_xml)
        print("\n----------------------------")

        # Optional: Parse and print some basic info to confirm
        root = ET.fromstring(raw_xml)
        ns_zs = {'zs': 'http://www.loc.gov/zing/srw/'}
        ns_marc = {'marc': 'http://www.loc.gov/MARC21/slim'}

        num_records = root.find('.//zs:numberOfRecords', ns_zs)
        if num_records is not None and int(num_records.text) > 0:
            title_node = root.find('.//marc:datafield[@tag="245"]/marc:subfield[@code="a"]', ns_marc)
            author_node = root.find('.//marc:datafield[@tag="100"]/marc:subfield[@code="a"]', ns_marc)
            print(f"Found Record: Title=\"{title_node.text.strip() if title_node is not None else 'N/A'}\", Author=\"{author_node.text.strip() if author_node is not None else 'N/A'}\"")
        else:
            print("No records found for this ISBN.")

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
    except ET.ParseError as e:
        print(f"XML parsing error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    isbn_to_query = "9781632367983"
    query_loc_by_isbn(isbn_to_query)
