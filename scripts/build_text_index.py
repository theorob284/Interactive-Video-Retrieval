
import json
import sys
from pathlib import Path

# Allow imports from the project root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.retrieval.text_search import TextSearch, load_transcripts


METADATA_PATH = ROOT / "data/assignment2_data/videos_metadata.json"
TRANSCRIPT_DIR = ROOT / "data/assignment2_data/transcripts"
INDEX_PATH = ROOT / "indexes/text_bm25.pkl"


def main():
    print("Loading video metadata...")

    records = json.loads(
        METADATA_PATH.read_text(encoding="utf-8")
    )

    print(f"Loaded {len(records)} videos.")

    print("Loading Whisper transcripts...")
    transcripts = load_transcripts(TRANSCRIPT_DIR)

    print(f"Loaded {len(transcripts)} transcripts.")

    video_ids = {str(record["v3cId"]) for record in records}
    matched = video_ids & set(transcripts)

    print(f"Matching transcripts: {len(matched)}/{len(video_ids)}")

    print("Building combined BM25 index...")

    search_engine = TextSearch(
        records,
        transcripts=transcripts,
    )

    print("Saving index...")
    search_engine.save(INDEX_PATH)

    print(f"Index saved to: {INDEX_PATH}")
    print(f"Indexed videos: {len(search_engine.video_ids)}")


if __name__ == "__main__":
    main()
