import sys
import csv
import io
import re
import requests
import time
import json
import os

# --- Constants & Cache (simplified for this debugger script) ---
SUGGESTION_FLAG = "🐒"
CACHE_FILE = "loc_cache.json" # This script will still use the cache for efficiency


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)


def get_raw_loc_response(title, author, cache):
    safe_title = re.sub(r'[^a-zA-Z0-9\s\.:\\]', '', title)
    safe_author = re.sub(r'[^a-zA-Z0-9\s,]', '', author)
    cache_key = f"{safe_title}|{safe_author}".lower()

    # Check cache first
    if cache_key in cache and 'raw_response' in cache[cache_key]:
        sys.stderr.write(f"DEBUG: Cache hit for {title} by {author}\n")
        return cache[cache_key]['raw_response'], cache[cache_key].get('query', 'N/A')

    base_url = "http://lx2.loc.gov:210/LCDB"
    if safe_author:
        query = f'bath.title="{safe_title}" and bath.author="{safe_author}"'
    else:
        query = f'bath.title="{safe_title}"'

    params = {"version": "1.1", "operation": "searchRetrieve", "query": query, "maximumRecords": "1", "recordSchema": "marcxml"}

    raw_response = ""

    retry_delays = [5, 30, 60]
    for i in range(len(retry_delays) + 1):
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            raw_response = response.content.decode('utf-8')

            # Store in cache
            cache[cache_key] = {'raw_response': raw_response, 'query': query}
            return raw_response, query
        except requests.exceptions.RequestException as e:
            sys.stderr.write(f"ERROR: Request failed for {title} by {author}: {e}\n")
            if i < len(retry_delays):
                time.sleep(retry_delays[i])
            else:
                break
        except Exception as e:
            sys.stderr.write(f"ERROR: An unexpected error occurred for {title} by {author}: {e}\n")
            break

    # Store error in cache if all retries fail
    cache[cache_key] = {'raw_response': 'ERROR: ' + str(e), 'query': query}
    return "ERROR: " + str(e), query


def main():
    csv_content = '''Line Number,"Title","Author's Name","Call Number","Copyright","Holdings Barcode","Publication Date","Series Title","Series Volume"
1,"Bonji Yagkanatu (Paperback)","","","c2024.","B000173","2024.","",""
2,"The old man and the sea","Hemingway, Ernest","","c1995.","B000172","1995.","",""
3,"The old man and the sea","Hemingway, Ernest","","c1995.","B000174","1995.","",""
4,"Are We Living in the Last Days? : The Second Coming of Jesus Christ and Interpreting the Book of Revelation","Killens, Chauncey S.","","c2023.","B000184","2023.","",""
5,"The girl from Playa Blanca","Lachtman, Ofelia Dumas","","c1995.","B000177","1995.","",""
6,"A spectacle of corruption.","Liss, David","","c2004.","B000179","2004.","Benjamin Weaver","2"
7,"Huésped","Meyer, Stephenie","","c2008.","B000171","2008.","",""
8,"Jack & Jill (Alex Cross)","Patterson, James","","c1997.","B000183","1997-11.","Alex Cross","3"
9,"Slow Bullets","Reynolds, Alastair","","c2015.","B000180","2015.","",""
10,"Nonviolent communication : a language of life","Rosenberg, Marshall B.","","c2015.","B000169","2015.","",""
11,"Rhythm of War: Book Four of the Stormlight Archive","Sanderson, Brandon","","c2021.","B000170","Oct.","The Stormlight Archive","4"
12,"The Way of Kings: Book One of the Stormlight Archive","Sanderson, Brandon","","c2014.","B000185","March.","The Storm light Archive","1"
13,"The Genius Prince's Guide to Raising a Nation Out of Debt (Hey How About Treason?), Vol. 3","Toba, Toru","","c2020.","B000163","2020.","The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?)","3"
14,"The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?), Vol. 4","Toba, Toru","","c2020.","B000164","2020.","The Genius Princes Guide to Raising a Nation Out of Debt (Hey, How About Treason?)","4"
15,"The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?), Vol. 5","Toba, Toru","","c2020.","B000165","2020.","The Genius Princes Guide to Raising a Nation Out of Debt (Hey, How About Treason?)","5"
16,"The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?), Vol. 6","Toba, Toru","","c2021.","B000166","2021.","The Genius Princes Guide to Raising a Nation Out of Debt (Hey, How About Treason?)","6"
17,"The Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How About Treason?), Vol. 7","Toba, Toru","","c2021.","B000167","2021.","The Genius Princes Guide to Raising a Nation Out of Debt (Hey, How About Treason?)","7"
18,"Genius Prince's Guide to Raising a Nation Out of Debt (Hey, How about Treason?), Vol. 8 (light Novel)","Toba, Toru","","c2021.","B000168","2021.","The Genius Princes Guide to Raising a Nation Out of Debt (Hey, How About Treason?)","8"
19,"The power of now : a guide to spiritual enlightenment","Tolle, Eckhart","","c2004.","B000181","2004.","",""
20,"Trauma-Informed Approach to Library Services","Tolley, Rebecca","","c2020.","B000182","2020.","",""
21,"Beneath Devil's Bridge : a novel","White, Loreth Anne","","c2021.","B000175","May.","",""
22,"In the Deep","White, Loreth Anne","","c2020.","B000176","2020.","",""
23,"The devil's arithmetic","Yolen, Jane","","c1990.","B000178","1990.","",""'''
    csvfile = io.StringIO(csv_content)
    reader = csv.DictReader(csvfile)

    cache = load_cache()

    for row in reader:
        title = row.get('Title', '').strip()
        author = row.get("Author's Name", '').strip()

        sys.stderr.write(f'--- Processing: {title} by {author} ---\n')
        raw_response, query = get_raw_loc_response(title, author, cache)
        sys.stderr.write(f"QUERY: {query}\n")
        sys.stderr.write(f"RAW RESPONSE: {raw_response}\n\n")

    save_cache(cache)


if __name__ == "__main__":
    main()
