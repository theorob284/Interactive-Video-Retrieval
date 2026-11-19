
import json
import sys
from pathlib import Path

# Allow imports from the project root.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.retrieval.text_search import TextSearch, load_transcripts

METADATA_PATH = ROOT / "data/assignment2_data/videos_metadata.json"
TRANSCRIPT_DIR = ROOT / "data/assignment2_data/transcripts"

METADATA_INDEX = ROOT / "indexes/metadata_bm25.pkl"
TRANSCRIPT_INDEX = ROOT / "indexes/transcript_bm25.pkl"

def main():
    print("Loading metadata...")
    records = json.loads(METADATA_PATH.read_text(encoding="utf-8"))

    print("Loading transcripts...")
    transcripts = load_transcripts(TRANSCRIPT_DIR)

    video_ids = {str(record["v3cId"]) for record in records}
    matched = video_ids & set(transcripts)

    print(f"Videos: {len(video_ids)}")
    print(f"Matching transcripts: {len(matched)}")

    print("\nBuilding metadata BM25...")
    metadata_engine = TextSearch(records, source="metadata",)
    metadata_engine.save(METADATA_INDEX)

    print("Building transcript BM25...")
    transcript_engine = TextSearch(records, transcripts=transcripts, source="transcript",)
    transcript_engine.save(TRANSCRIPT_INDEX)

    print("\nIndexes saved:")
    print(METADATA_INDEX)
    print(TRANSCRIPT_INDEX)

    print(f"Metadata documents: {len(metadata_engine.video_ids)}")
    print(f"Transcript documents: {len(transcript_engine.video_ids)}")

if __name__ == "__main__":
    main()
