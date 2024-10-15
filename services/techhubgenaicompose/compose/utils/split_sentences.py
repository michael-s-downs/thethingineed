### This code is property of the GGAO ###


from copy import deepcopy
from typing import List, Tuple
import re


# This is copied and simplify from haystack version 1.11.0 - Consider updating the code in the future


def _split_sentences(text: str, language_name="english") -> List[str]:
    """
    Tokenize text into sentences.
    :param text: str, text to tokenize
    :return: list[str], list of sentences
    """
    import nltk

    nltk.download('punkt', quiet=True)

    sentence_tokenizer = nltk.data.load(f'tokenizers/punkt/{language_name}.pickle')

    # The following adjustment of PunktSentenceTokenizer is inspired by:
    # https://stackoverflow.com/questions/33139531/preserve-empty-lines-with-nltks-punkt-tokenizer
    # It is needed for preserving whitespace while splitting text into sentences.
    period_context_fmt = r"""
        %(SentEndChars)s             # a potential sentence ending
        \s*                          # match potential whitespace (is originally in lookahead assertion)
        (?=(?P<after_tok>
            %(NonWord)s              # either other punctuation
            |
            (?P<next_tok>\S+)        # or some other token - original version: \s+(?P<next_tok>\S+)
        ))"""
    re_period_context = re.compile(
        period_context_fmt
        % {
            "NonWord": sentence_tokenizer._lang_vars._re_non_word_chars,
            "SentEndChars": sentence_tokenizer._lang_vars._re_sent_end_chars,
        },
        re.UNICODE | re.VERBOSE,
    )
    sentence_tokenizer._lang_vars._re_period_context = re_period_context

    sentences = sentence_tokenizer.tokenize(text)
    return sentences

def split_by_word_respecting_sent_boundary(text: str, split_length: int, split_overlap: int, language="english") -> Tuple[List[str], List[int], List[int]]:
    """
    Splits the text into parts of split_length words while respecting sentence boundaries.
    """
    sentences = _split_sentences(text, language)

    word_count_slice = 0
    cur_page = 1
    cur_start_idx = 0
    splits_pages = []
    list_splits = []
    splits_start_idxs = []
    current_slice: List[str] = []
    for sen in sentences:
        word_count_sen = len(sen.split())

        if word_count_slice + word_count_sen > split_length:
            # Number of words exceeds split_length -> save current slice and start a new one
            if current_slice:
                list_splits.append(current_slice)
                splits_pages.append(cur_page)
                splits_start_idxs.append(cur_start_idx)

            if split_overlap:
                overlap = []
                processed_sents = []
                word_count_overlap = 0
                current_slice_copy = deepcopy(current_slice)
                for idx, s in reversed(list(enumerate(current_slice))):
                    sen_len = len(s.split())
                    if word_count_overlap < split_overlap:
                        overlap.append(s)
                        word_count_overlap += sen_len
                        current_slice_copy.pop(idx)
                    else:
                        processed_sents = current_slice_copy
                        break
                current_slice = list(reversed(overlap))
                word_count_slice = word_count_overlap
            else:
                processed_sents = current_slice
                current_slice = []
                word_count_slice = 0

            cur_start_idx += len("".join(processed_sents))

        current_slice.append(sen)
        word_count_slice += word_count_sen

    if current_slice:
        list_splits.append(current_slice)
        splits_pages.append(cur_page)
        splits_start_idxs.append(cur_start_idx)

    text_splits = []
    for sl in list_splits:
        txt = "".join(sl)
        if len(txt) > 0:
            text_splits.append(txt)

    return text_splits, splits_pages, splits_start_idxs
