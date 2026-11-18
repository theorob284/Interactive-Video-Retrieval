
import re
from functools import lru_cache
from html import unescape

from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from rank_bm25 import BM25Okapi

import pickle
from pathlib import Path

def clean_text(text):
    text = unescape(text or "")
    return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)


def tokenize(text):
    text = clean_text(text).casefold()
    return re.findall(r"\w+", text, flags=re.UNICODE)


@lru_cache(maxsize=1)
def get_stopwords():
    return set(stopwords.words("english"))


def preprocess_query(query):
    tokens = tokenize(query)
    stop_words = get_stopwords()

    return [token for token in tokens if token not in stop_words]


def normalise_categories(categories):
    if isinstance(categories, str):
        categories = categories.split(";")

    return " ".join(
        part
        for category in (categories or [])
        for part in category.strip().strip("/").split("/")
        if part not in {"categories", "videos"}
    )

def load_transcripts(transcript_dir):
    """Load Whisper transcripts, keyed by V3C video ID."""
    transcripts = {}

    for path in Path(transcript_dir).rglob("*.txt"):
        video_id = path.stem

        transcripts[video_id] = path.read_text(encoding="utf-8",errors="replace",)

    return transcripts

class TextSearch:
    """BM25 retrieval over video-level metadata."""

    def __init__(self, records, k1=1.5, b=0.75, transcripts=None):
        self.video_ids = []
        documents = []
        seen_ids = set()

        transcripts = transcripts or {}

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

            transcript = transcripts.get(video_id, "")

            content = " ".join([
                " ".join([title] * 3),
                " ".join([str(tags)] * 2),
                description,
                normalise_categories(categories),
                channel,
                transcript,
            ])

            tokens = tokenize(content)

            if not tokens:
                continue

            self.video_ids.append(video_id)
            documents.append(tokens)

        self.index = (
            BM25Okapi(documents, k1=k1, b=b)
            if documents else None
        )

    def search(self, query: str, top_k: int = 50) -> list[dict]:
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")

        if top_k <= 0 or self.index is None:
            return []

        query_tokens = preprocess_query(query)

        if not query_tokens:
            return []

        scores = self.index.get_scores(query_tokens)

        ranked = sorted(
            enumerate(scores),
            key=lambda item: item[1],
            reverse=True,
        )

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

    def save(self, index_path):
        """Save the built BM25 index locally."""
        index_path = Path(index_path)
        index_path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "video_ids": self.video_ids,
            "index": self.index,
        }

        with index_path.open("wb") as file:
            pickle.dump(data, file, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls, index_path):
        """Load a previously built BM25 index."""
        with Path(index_path).open("rb") as file:
            data = pickle.load(file)

        search_engine = cls.__new__(cls)
        search_engine.video_ids = data["video_ids"]
        search_engine.index = data["index"]

        return search_engine
