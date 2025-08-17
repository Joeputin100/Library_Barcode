import re

SUGGESTION_FLAG = "🐒"


def clean_title(title):
    """Cleans title by moving leading articles to the end."""
    if not isinstance(title, str):
        return ""
    articles = ['The ', 'A ', 'An ']
    for article in articles:
        if title.startswith(article):
            return title[len(article):] + ", " + title[:len(article) - 1]
    return title


def capitalize_title_mla(title):
    """Capitalizes a title according to MLA standards."""
    if not isinstance(title, str) or not title:
        return ""

    words = title.lower().split()
    minor_words = [
        'a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'in', 'of', 'off', 'out', 'up', 'so', 'yet'
    ]

    capitalized_words = []
    for i, word in enumerate(words):
        if i == 0 or i == len(words) - 1 or word not in minor_words:
            capitalized_words.append(word.capitalize())
        else:
            capitalized_words.append(word)

    return " ".join(capitalized_words)


def clean_author(author):
    """Cleans author name to Last, First Middle."""
    if not isinstance(author, str):
        return ""
    parts = author.split(',')
    if len(parts) == 2:
        return f"{parts[0].strip()}, {parts[1].strip()}"
    return author


def lcc_to_ddc(lcc):
    """Converts an LCC call number to a DDC range or 'FIC'."""
    if not isinstance(lcc, str) or not lcc:
        return ""

    if lcc == "FIC":
        return "FIC"

    if lcc.startswith(('PZ', 'PQ', 'PR', 'PS', 'PT')):
        return "FIC"

    LCC_TO_DDC_MAP = {
        'AC': '080-089', 'AE': '030-039', 'AG': '030-039', 'AI': '050-059', 'AM': '060-069',
        'AS': '060-069', 'AY': '031-032', 'B': '100-109', 'BC': '160-169', 'BD': '110-119',
        'BF': '150-159', 'BH': '111, 701', 'BJ': '170-179', 'BL': '200-299', 'BM': '200-299',
        'BP': '200-299', 'BQ': '200-299', 'BR': '200-299', 'BS': '200-299', 'BT': '200-299',
        'BV': '200-299', 'BX': '200-299', 'CB': '909', 'CC': '930-939', 'CD': '091, 930',
        'CE': '529', 'CJ': '737', 'CN': '411, 930', 'CR': '929.6', 'CS': '929.1-929.5',
        'CT': '920-929', 'D': '909', 'DA': '940-999', 'DB': '940-999', 'DC': '940-999',
        'DD': '940-999', 'DE': '940-999', 'DF': '940-999', 'DG': '940-999', 'DH': '940-999',
        'DJ': '940-999', 'DK': '940-999', 'DL': '940-999', 'DP': '940-999', 'DQ': '940-999',
        'DR': '940-999', 'DS': '940-999', 'DT': '940-999', 'DU': '940-999', 'DX': '940-999',
        'E': '970-979', 'F': '973-999', 'G': '910-919', 'GB': '910.02', 'GC': '551.46',
        'GE': '333.7, 577', 'GF': '304.2, 301-309', 'GN': '301-309', 'GR': '398',
        'GT': '390-399', 'GV': '790-799', 'H': '300-309', 'HA': '310-319', 'HB': '330-339',
        'HC': '330-339', 'HD': '330-339', 'HE': '380-389', 'HF': '330-339', 'HG': '330-339',
        'HH': '330-339', 'HJ': '330-339', 'HM': '301-309', 'HN': '301-309', 'HQ': '301-309',
        'HS': '301-309', 'HT': '301-309', 'HV': '301-309', 'HX': '335', 'J': '320',
        'JA': '320.01', 'JC': '320.1-320.5', 'JF': '320-329', 'JJ': '320-329', 'JK': '320-329',
        'JL': '320-329', 'JN': '320-329', 'JQ': '320-329', 'JS': '320-329', 'JV': '320-329',
        'JX': '341-349', 'K': '340', 'KBM': '340-349', 'KBP': '340-349', 'KBR': '340-349',
        'KBS': '340-349', 'KBT': '340-349', 'KBU': '340-349', 'L': '370', 'LA': '370-375',
        'LB': '370-375', 'LC': '371-379', 'LD': '378', 'LE': '378', 'LF': '378', 'LG': '378',
        'M': '780', 'ML': '780.9', 'MT': '781-789', 'N': '700-709', 'NA': '720-729',
        'NB': '730-779', 'NC': '730-779', 'ND': '730-779', 'NE': '730-779', 'NK': '730-779',
        'NX': '730-779', 'P': '400-409', 'PA': '880-889', 'PB': '410-419', 'PC': '440-469',
        'PD': '430-439', 'PE': '420-429', 'PF': '430-439', 'PG': '891.7', 'PH': '494',
        'PJ': '892', 'PK': '891', 'PL': '895', 'PM': '497, 499', 'PN': '800-809',
        'PQ': '840-849', 'PR': '820-829', 'PS': '810-819', 'PT': '830-839', 'PZ': 'FIC',
        'Q': '500-509', 'QA': '510-519', 'QB': '520-599', 'QC': '520-599', 'QD': '520-599',
        'QE': '520-599', 'QH': '520-599', 'QK': '520-599', 'QL': '520-599', 'QM': '520-599',
        'QP': '520-599', 'QR': '520-599', 'R': '610', 'RA': '610-619', 'RB': '610-619',
        'RC': '610-619', 'RD': '610-619', 'RE': '610-619', 'RF': '610-619', 'RG': '610-619',
        'RJ': '610-619', 'RK': '610-619', 'RL': '610-619', 'RM': '610-619', 'RS': '610-619',
        'RT': '610-619', 'RV': '610-619', 'RX': '610-619', 'RZ': '610-619', 'S': '630',
        'SB': '630-639', 'SD': '630-639', 'SF': '630-639', 'SH': '630-639', 'SK': '630-639',
        'T': '600-609', 'TA': '620-699', 'TC': '620-699', 'TD': '620-699', 'TE': '620-699',
        'TF': '620-699', 'TG': '620-699', 'TH': '620-699', 'TJ': '620-699', 'TK': '620-699',
        'TL': '620-699', 'TN': '620-699', 'TP': '620-699', 'TR': '620-699', 'TS': '620-699',
        'TT': '620-699', 'TX': '620-699', 'U': '355', 'UA': '355-359', 'UB': '355-359',
        'UC': '355-359', 'UD': '355-359', 'UE': '355-359', 'UF': '355-359', 'UG': '355-359',
        'UH': '355-359', 'V': '359', 'VM': '623', 'Z': '010-029'
    }

    for prefix, ddc_range in LCC_TO_DDC_MAP.items():
        if lcc.startswith(prefix):
            return ddc_range.split('-')[0].strip()

    return ""


def clean_call_number(call_num_str, genres, google_genres=None, title="", is_original_data=False):
    if google_genres is None:
        google_genres = []

    if not isinstance(call_num_str, str):
        return ""

    cleaned = call_num_str.strip()
    if not is_original_data:
        cleaned = cleaned.lstrip(SUGGESTION_FLAG)

    fiction_keywords_all = [
        "fiction", "fantasy", "science fiction", "thriller", "mystery", "romance", "horror", "novel", "stories",
        "a novel", "young adult fiction", "historical fiction", "literary fiction"
    ]
    if any(g.lower() in fiction_keywords_all for g in google_genres) or \
       any(genre.lower() in fiction_keywords_all for genre in genres) or \
       any(keyword in title.lower() for keyword in fiction_keywords_all):
        return "FIC"

    ddc_from_lcc = lcc_to_ddc(cleaned)
    if ddc_from_lcc:
        return ddc_from_lcc

    cleaned = re.sub(r'[^a-zA-Z0-9\s\.:]', '', cleaned).strip()

    if cleaned.lower() in [
        "fantasy", "science fiction", "thriller", "mystery", "romance", "horror", "novel", "fiction",
        "young adult fiction", "historical fiction", "literary fiction"
    ]:
        return "FIC"

    if cleaned.upper().startswith("FIC"):
        return "FIC"

    match = re.match(r'^(\d{3}(\.\d{1,3})?)', cleaned)
    if match:
        return match.group(1)

    if re.match(r'^[A-Z]{1,3}\d+(\.\d+)?$', cleaned) or re.match(r'^\d+(\.\d+)?$', cleaned):
        return cleaned

    return ""


def clean_series_number(series_num_str):
    if not isinstance(series_num_str, str):
        return ""

    cleaned = series_num_str.strip().lower()
    cleaned = re.sub(r'\s*of\s*\d+', '', cleaned)
    cleaned = re.sub(r'[\[\]\.,]', '', cleaned)
    cleaned = re.sub(r'\b(book|bk|bk\.|volume|vol|part|pt|v|no|number)\b', '', cleaned)
    cleaned = cleaned.strip()

    word_to_num = {
        'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5',
        'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10',
        'eleven': '11', 'twelve': '12', 'thirteen': '13', 'fourteen': '14', 'fifteen': '15',
        'sixteen': '16', 'seventeen': '17', 'eighteen': '18', 'nineteen': '19', 'twenty': '20'
    }
    for word, digit in word_to_num.items():
        cleaned = cleaned.replace(word, digit)

    match = re.search(r'\d+', cleaned)
    if match:
        return match.group(0)
    return ""


def extract_year(date_string):
    if isinstance(date_string, str):
        match = re.search(r'[\(\) \[©c]?(\d{4})[\) \]]?', date_string)
        if match:
            return match.group(1)
    return ""



