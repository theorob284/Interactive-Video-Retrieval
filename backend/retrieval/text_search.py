
import re
from functools import lru_cache
from html import unescape

from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from rank_bm25 import BM25Okapi

def clean_text(text):
    text = unescape(text or "")
    return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)

def tokenize(text):
    text = clean_text(text).casefold()
    return re.findall(r"\w+", text, flags=re.UNICODE)

@lru_cache(maxsize=1)
def get_stopwords():
    words = set()

    for language in stopwords.fileids():
        words.update(word.casefold() for word in stopwords.words(language))

    return words

def preprocess_query(query):
    tokens = tokenize(query)
    stop_words = get_stopwords()

    filtered = [token for token in tokens if token not in stop_words]

    return filtered

def normalise_categories(categories):
    if isinstance(categories, str):
        categories = categories.split(";")

    return " ".join(part for category in (categories or []) for part in category.strip().strip("/").split("/") if part not in {"categories", "videos"})

class TextSearch:
    """BM25 retrieval over video-level metadata."""

    def __init__(self, records, k1 = 1.5, b = 0.75):
        self.video_ids = []
        documents = []
        seen_ids = set()

        for record in records:
            video_id = str(record["v3cId"])

            if video_id in seen_ids:
                raise ValueError(f"Duplicate video ID: {video_id}")

            seen_ids.add(video_id)

            title = str(record.get("title") or "")
            description = str(record.get("description") or "")
            tags = record.get("tags") or ""
            categories = record.get("categories") or []
            channel = str(record.get("channel") or "")

            if isinstance(tags, list):
                tags = " ".join(map(str, tags))

            content = " ".join([title, description, str(tags), normalise_categories(categories), channel])

            tokens = tokenize(content)

            if not tokens:
                continue

            self.video_ids.append(video_id)
            documents.append(tokens)

        self.index = (BM25Okapi(documents, k1=k1, b=b) if documents else None)

    def search(self, query: str, top_k: int = 50) -> list[dict]:
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")

        if top_k <= 0 or self.index is None:
            return []

        query_tokens = preprocess_query(query)

        if not query_tokens:
            return []

        scores = self.index.get_scores(query_tokens)

        ranked = sorted(enumerate(scores), key=lambda item: item[1], reverse=True,)

        results = []

        for i, score in ranked:
            if score <= 0:
                continue

            results.append({
                "video_id": self.video_ids[i],
                "score": float(score),
            })

            if len(results) >= top_k:
                break

        return results
