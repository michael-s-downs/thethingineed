### This code is property of the GGAO ###


# Native imports
import re
from copy import deepcopy
from typing import List, Dict

# Installed imports
from haystack.schema import Document


def test_split():
    # with open("test_document.txt", "r", encoding="utf-8") as f:
    #    text = f.read()
    text_1 = """将来、多数のドローンが飛び交い、物流や郵便、警備、災害調査、点検、測量、農業などのさまざまな分野で活用されることが期待され
ています。高密度でドローンが飛び交う世界を想定すると、衝突などの危険を確実に回避するため、すべての機体の飛行計画と飛行状況.
を掌握して、ドローンの運航を統合的に管理する必要があります。さらに、ドローンを安全に運航するためには、気象情報や地形、建物
の3次元地図情報をドローン事業者に提供する必要があります。"""
    jap_preprocessor = JapanesePreProcessor(10, 100)
    list_chunks = jap_preprocessor.split(text_1)
    print(list_chunks)

    assert len(list_chunks) == 4
    assert list_chunks[-1][-2] == text_1[-2]


def test_process_one_doc():
    text_1 = """将来、多数のドローンが飛び交い、物流や郵便、警備、災害調査、点検、測量、農業などのさまざまな分野で活用されることが期待され
ています。高密度でドローンが飛び交う世界を想定すると、衝突などの危険を確実に回避するため、すべての機体の飛行計画と飛行状況.
を掌握して、ドローンの運航を統合的に管理する必要があります。さらに、ドローンを安全に運航するためには、気象情報や地形、建物
の3次元地図情報をドローン事業者に提供する必要があります。"""
    docs_ = [{"content": text_1, "meta": {"casa": "cosa"}}]
    jap_preprocessor = JapanesePreProcessor(10, 100)
    list_chunks = jap_preprocessor.process(docs_)
    print(list_chunks)

    assert len(list_chunks) == 4
    assert list_chunks[-1].content[-2] == text_1[-2]


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    num_tokens = len(string)
    return num_tokens


class JapanesePreProcessor:
    def __init__(self, split_overlap, split_length) -> None:
        self.split_overlap = split_overlap
        self.split_length = split_length
        self.separators = ["\n", ".", "。"]

    def split_sentences(self, text: str) -> List[str]:
        # Splitting the text based on '\n' and '.'
        # string_separators = "".join(separators)
        # Create a regular expression pattern for splitting based on separators

        pattern = f"{'|'.join(map(re.escape,self.separators))}"

        # Split the text based on the pattern
        chunks = re.split(pattern, text)

        # Removing empty chunks
        chunks = [f"{chunk.strip()}." for chunk in chunks if chunk.strip()]

        chunk_lengths = [num_tokens_from_string(chunk_) for chunk_ in chunks]

        return chunks, chunk_lengths

    def split(self, text: str) -> List[str]:
        """
        Splits the text into parts of split_length words while respecting sentence boundaries.
        """

        sentences, sentences_len = self.split_sentences(text)

        word_count_slice = 0
        cur_start_idx = 0
        list_splits = []

        current_slice: List[str] = []
        for sen, word_count_sen in zip(sentences, sentences_len):
            print(f"{word_count_slice}+{word_count_sen}")
            if word_count_slice + word_count_sen > self.split_length:
                # Number of words exceeds split_length -> save current slice and start a new one
                if current_slice:
                    list_splits.append(current_slice)

                if self.split_overlap:
                    overlap = []
                    processed_sents = []
                    word_count_overlap = 0
                    current_slice_copy = deepcopy(current_slice)
                    for idx, s in reversed(list(enumerate(current_slice))):
                        sen_len = len(s)
                        if word_count_overlap < self.split_overlap:
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

        text_splits = []
        for sl in list_splits:
            txt = " ".join(sl)
            if len(txt) > 0:
                text_splits.append(txt)

        return text_splits

    def process_one(self, doc: Dict):
        meta = deepcopy(doc["meta"])
        for slices in self.split(doc["content"]):
            doc = Document(content=slices, meta=meta)
            yield doc

    def process(self, docs: List[Dict]):
        documents = []
        for doc in docs:
            documents.extend(self.process_one(doc))
        return documents
