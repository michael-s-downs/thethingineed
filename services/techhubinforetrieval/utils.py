### This code is property of the GGAO ###


import re
import json
import string
import unicodedata

from rapidfuzz import fuzz, process

SIMILARITY_THRESHOLD = 90

STOPWORDS = json.load(open(r"stopwords.json", "r"))


def strip_accents(s: str):
    """Clean accents

    Args:
        s (str): input string

    Returns:
        str: Parsed string
    """
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def normalize(s):
    """ Clean most common non ascii strings

    Args:
        s (str): input string

    Returns:
        str: Parsed string
    """
    s = strip_accents(s)
    s = re.sub(f"[{string.punctuation}¿¡\(\)]", "", s)
    return s.lower()


def set_html_highlight(text: str, index_min: int, index_max: int):
    """Add highlights in identified positions

    Args:
        text (str): Input string
        index_min (_type_): Minimum position to add highlight
        index_max (_type_): Maximum position to add highlight

    Returns:
        str: Highlighted text
    """
    text = text[:index_min] + \
           f" <strong>{text[index_min:index_max]}</strong> " + \
           text[index_max:]
    return text


def get_exact_search(query: str):
    """If the query is exactly in the text return it

    Args:
        query (str): Input query

    Returns:
        list: Exact queries
    """
    query_split = query.replace("?", "").split("\"")

    queries_text = []
    if len(query_split) > 1:
        search = False
        for i in range(len(query_split)):
            if search:
                # Check if there is another quote closing chosen text
                if len(query_split) >= i + 2:
                    queries_text.append(query_split[i].strip())
                    search = False
            else:
                search = True
    return queries_text


def add_highlights(part: list, query: str, text: str):
    """Adds highlights

    Args:
        part (list): QA models return the position of the answer, use it. 
        query (str): Input query
        text (str): Full text to search the query in

    Returns:
        str: Text with highlights
    """
    # Main function
    offsets = part.get("offsets_in_document", [])  # [{"start": 500, "end": 558}]
    for index_in_context in offsets:
        text = set_html_highlight(text, index_min=index_in_context['start'], index_max=index_in_context['end'])

    if not offsets:
        # If query partialy in text
        exact_search = get_exact_search(query)
        if len(exact_search) > 0:
            for search in exact_search:
                matches = re.finditer(search, text, re.I)
                for match in sorted(matches, key=lambda x: x.start(0), reverse=True):
                    text = set_html_highlight(text, index_min=match.start(0), index_max=match.end(0))
        # Query in text
        if query.lower() in text.lower():
            matches = re.finditer(query, text, re.I)
            for match in sorted(matches, key=lambda x: x.start(0), reverse=True):
                text = set_html_highlight(text, index_min=match.start(0), index_max=match.end(0))
        else:
            # If similar query
            matches = process.extractOne(query, text.split("."), scorer=fuzz.ratio)
            if matches[1] > SIMILARITY_THRESHOLD:
                chosen_phrase = matches[0]
                initial_index = text.index(chosen_phrase)
                final_index = initial_index + len(chosen_phrase) + 1
                text = set_html_highlight(text, index_min=initial_index, index_max=final_index)
            else:
                # Search matching words
                query_words = re.sub(" {2,}", " ", query).split()
                query_words = [word for word in query_words if normalize(word) and normalize(word) not in STOPWORDS]
                # Sort by length to avoid partial matching in the regex
                query_words = map(re.escape, sorted(list(set(query_words)), key=lambda x: len(x), reverse=True))
                matches = re.finditer(r"|".join(query_words), text, re.I)
                for match in sorted(matches, key=lambda x: x.start(0), reverse=True):
                    text = set_html_highlight(text, index_min=match.start(0), index_max=match.end(0))

    # Text cleaning
    text = re.sub("[\\\]+n", "\n", text)
    text = re.sub("[\\\]+t", "\t", text)
    text = re.sub("^\s+|\s+$", "", text)
    text = re.sub("\.{2,}" ,"\.", text)

    # Delete dirty texts (Single chars or lines of special chars or multiple linebreaks)
    tmp_text = ""
    while tmp_text != text:
        tmp_text = text
        text = re.sub("\n(.{,2}|[\W]*)\s*\n", "\n", text)

    text = text.replace("\n", "<br>")  # Transform linebreaks to html format
    text = text.replace("\t", "  ")  # Transform tabulations to html format

    return text
