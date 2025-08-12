
import requests

def test_loc_api():
    base_url = "http://lx2.loc.gov:210/LCDB"
    query = 'bath.title="A spectacle of corruption." and bath.author="Liss, David"'
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        print(response.text)
    except requests.exceptions.RequestException as e:
        print(f"API call failed with error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    test_loc_api()
