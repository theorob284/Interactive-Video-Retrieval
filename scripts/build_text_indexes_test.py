import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from retrieval.text_search import TextSearch, load_transcripts

DATA_DIR = ROOT / "data" / "assignment2_data"
METADATA_PATH = DATA_DIR / "videos_metadata.json"
TRANSCRIPT_DIR = DATA_DIR / "transcripts"
INDEX_DIR = ROOT / "indexes"

def build_and_report(filename, records, source, transcripts=None):
    start = time.perf_counter()
    engine = TextSearch(records, source=source, transcripts=transcripts)
    build_seconds = time.perf_counter() - start

    index_path = INDEX_DIR / filename
    engine.save(index_path)
    size_mb = index_path.stat().st_size / 1_000_000

    print(f"{filename}: {len(engine.video_ids):,} videos | {size_mb:.2f} MB | build {build_seconds:.2f} s")

def main():
    with METADATA_PATH.open(encoding="utf-8") as file:
        records = json.load(file)

    INDEX_DIR.mkdir(parents=True, exist_ok=True)

    build_and_report("metadata_bm25.pkl", records, source="metadata")

    start = time.perf_counter()
    transcripts = load_transcripts(TRANSCRIPT_DIR)
    load_seconds = time.perf_counter() - start
    print(f"Loaded {len(transcripts):,} transcript TXT files in {load_seconds:.2f} s")

    if not transcripts:
        raise RuntimeError(f"No transcript TXT files found in {TRANSCRIPT_DIR}")

    build_and_report("transcript_bm25.pkl",records, source="transcript", transcripts=transcripts,)

if __name__ == "__main__":
    main()
