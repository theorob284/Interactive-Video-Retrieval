import pickle
import re
from functools import lru_cache
from html import unescape
from pathlib import Path

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
    return set(stopwords.words("english"))

def preprocess_query(query):
    tokens = tokenize(query)
    stop_words = get_stopwords()

    return [token for token in tokens if token not in stop_words]

def normalise_categories(categories):
    if isinstance(categories, str):
        categories = categories.split(";")

    return " ".join(part for category in (categories or []) for part in category.strip().strip("/").split("/") if part not in {"categories", "videos"})

def load_transcripts(transcript_dir):
    """Load Whisper transcripts, keyed by V3C video ID."""
    transcripts = {}

    for path in Path(transcript_dir).rglob("*.txt"):
        video_id = path.stem
        transcripts[video_id] = path.read_text(encoding="utf-8",errors="replace",)

    return transcripts

class TextSearch:
    """BM25 retrieval over video metadata or Whisper transcripts."""

    def __init__(self, records, k1=1.5, b=0.75, transcripts=None, source="metadata"):
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

            if source == "metadata":
                content = " ".join([title, description, str(tags), normalise_categories(categories), channel,])
            elif source == "transcript":
                content = transcripts.get(video_id, "")
            else:
                raise ValueError(f"Unknown index source: {source}")

            tokens = tokenize(content)

            if not tokens:
                continue

            self.video_ids.append(video_id)
            documents.append(tokens)

        self.index = (BM25Okapi(documents, k1=k1, b=b)if documents else None)

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

            results.append({"video_id": self.video_ids[i], "score": float(score),})

            if len(results) >= top_k:
                break

        return results

    def save(self, index_path):
        """Save the built BM25 index locally."""
        index_path = Path(index_path)
        index_path.parent.mkdir(parents=True, exist_ok=True)

        data = {"video_ids": self.video_ids, "index": self.index,}

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

# Transcript timestamps are used to locate relevant moments after the existing BM25 indexes have retrieved videos.

TRANSCRIPT_DIR = (Path(__file__).resolve().parents[2] / "data" / "assignment2_data" / "transcripts")
SRT_TIME_PATTERN = re.compile(r"(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*"r"(\d{2}:\d{2}:\d{2},\d{3})")
IGNORED_LABELS = {"music", "[music]", "(music)", "silence", "[silence]", "(silence)",}

def srt_time_to_ms(timestamp):
    hours, minutes, remainder = timestamp.split(":")
    seconds, milliseconds = remainder.split(",")

    return (int(hours) * 3600000 + int(minutes) * 60000 + int(seconds) * 1000 + int(milliseconds))

@lru_cache(maxsize=128)
def load_srt_segments(video_id):
    """Load and cache timestamped speech for one video."""

    matches = list(TRANSCRIPT_DIR.rglob(f"{video_id}.srt"))

    if not matches:
        return []

    content = matches[0].read_text(encoding="utf-8-sig", errors="replace",).replace("\r\n", "\n")

    segments = []

    for block in re.split(r"\n\s*\n", content):
        lines = block.strip().splitlines()

        for index, line in enumerate(lines):
            match = SRT_TIME_PATTERN.search(line)

            if not match:
                continue

            text = " ".join(lines[index + 1:]).strip()
            text = re.sub(r"<[^>]+>", " ", text)
            text = " ".join(text.split())

            if not text or text.casefold() in IGNORED_LABELS:
                break

            start_ms = srt_time_to_ms(match.group(1))
            end_ms = srt_time_to_ms(match.group(2))

            if end_ms > start_ms:
                segments.append({
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "text": text,
                })

            break

    return sorted(segments, key=lambda s: s["start_ms"])

def find_relevant_passages(video_id, query, top_k=3, window_ms=20000, step_ms=10000,):
    """Find relevant timestamped speech passages in a video."""

    segments = load_srt_segments(video_id)

    if not segments or top_k <= 0:
        return []

    query_tokens = preprocess_query(query)

    if not query_tokens:
        return []

    query_terms = set(query_tokens)

    video_end = max(s["end_ms"] for s in segments)
    passages = []
    documents = []

    for start in range(0, video_end, step_ms):
        end = min(start + window_ms, video_end)

        matching = [s["text"] for s in segments if s["start_ms"] < end and s["end_ms"] > start]

        if not matching:
            continue

        text = " ".join(matching)
        tokens = tokenize(text)

        if not tokens:
            continue

        matched_segments = []

        for segment in segments:
            if (segment["start_ms"] >= end or segment["end_ms"] <= start):
                continue

            segment_tokens = set(tokenize(segment["text"]))
            matched_terms = query_terms & segment_tokens

            if matched_terms:
                matched_segments.append({
                    "start_ms": segment["start_ms"],
                    "end_ms": segment["end_ms"],
                    "coverage": len(matched_terms) / len(query_terms),
                })

        passages.append({
            "video_id": video_id,
            "start_ms": start,
            "end_ms": end,
            "text": text,
            "matched_segments": matched_segments,
        })

        documents.append(tokens)

    if not documents:
        return []

    index = BM25Okapi(documents)
    scores = index.get_scores(query_tokens)
    adjusted_scores = []

    for i, score in enumerate(scores):
        if score <= 0:
            continue

        passage_terms = set(documents[i])

        coverage = (len(query_terms & passage_terms) / len(query_terms))
        adjusted_score = float(score) * (1 + 0.3 * coverage)
        adjusted_scores.append((i, adjusted_score, coverage))

    ranked = sorted(adjusted_scores, key=lambda item: item[1], reverse=True,)
    results = []

    for i, score, coverage in ranked[:top_k]:
        results.append({**passages[i], "score": score, "coverage": coverage,})

    return results
