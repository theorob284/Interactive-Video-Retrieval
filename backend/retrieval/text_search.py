import json
import re
from pathlib import Path

from rank_bm25 import BM25Okapi


def tokenize(text: str) -> list[str]:
    return re.findall(r"\w+", text.lower())

class TextSearch:
    def __init__(self, records: list[dict]):
        self.video_ids = []
        documents = []

        for record in records:
            video_id = record["v3cId"]

            content = " ".join([
                record.get("title", ""),
                record.get("description", ""),
                record.get("tags", ""),
            ])

            self.video_ids.append(video_id)
            documents.append(tokenize(content))

        self.index = BM25Okapi(documents)

    def search(self, query: str, top_k: int = 50):
        scores = self.index.get_scores(tokenize(query))

        ranked = sorted(
            enumerate(scores),
            key=lambda item: item[1],
            reverse=True
        )[:top_k]

        return [
            {
                "video_id": self.video_ids[i],
                "score": float(score),
            }
            for i, score in ranked
            if score > 0
        ]