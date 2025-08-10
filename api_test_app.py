
import streamlit as st
import requests

def main():
    st.title("Simple LOC API Test")

    title = "A spectacle of corruption."
    author = "Liss, David"

    base_url = "http://lx2.loc.gov:210/LCDB"
    query = f'bath.title="{title}" and bath.author="{author}"'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}

    with st.expander("API Query", expanded=True):
        st.code(query)

    try:
        response = requests.get(base_url, params=params, timeout=30, headers=headers)
        response.raise_for_status()
        
        with st.expander("Connection Log", expanded=True):
            st.success("API connection successful.")
            st.write("Status Code:", response.status_code)

        with st.expander("Raw API Response", expanded=True):
            st.code(response.text)

    except requests.exceptions.RequestException as e:
        with st.expander("Connection Log", expanded=True):
            st.error(f"API request failed: {e}")

if __name__ == "__main__":
    main()
