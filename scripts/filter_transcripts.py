
import argparse
import json
from pathlib import Path

METADATA_PATH = Path("data/assignment2_data/videos_metadata.json")
TRANSCRIPT_DIR = Path("data/assignment2_data/transcripts")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true", help="Delete transcripts that do not match dataset video IDs.",)
    args = parser.parse_args()

    records = json.loads(METADATA_PATH.read_text(encoding="utf-8"))

    video_ids = {str(record["v3cId"]).zfill(5) for record in records}

    transcript_files = [path for path in TRANSCRIPT_DIR.rglob("*") if path.is_file() and path.suffix.lower() in {".txt", ".srt", ".vtt"}]

    matching = [path for path in transcript_files if path.stem in video_ids]

    unnecessary = [path for path in transcript_files if path.stem not in video_ids]

    matched_ids = {path.stem for path in matching}
    missing_ids = video_ids - matched_ids

    total_size = sum(path.stat().st_size for path in transcript_files)
    unnecessary_size = sum(path.stat().st_size for path in unnecessary)

    print(f"Videos in dataset: {len(video_ids)}")
    print(f"Total transcript files: {len(transcript_files)}")
    print(f"Matching transcript files: {len(matching)}")
    print(f"Unnecessary transcript files: {len(unnecessary)}")
    print(f"Videos with transcripts: {len(matched_ids)}")
    print(f"Videos without transcripts: {len(missing_ids)}")
    print(f"Total transcript size: {total_size / 1024**2:.2f} MB")
    print(f"Space recoverable: {unnecessary_size / 1024**2:.2f} MB")

    if missing_ids:
        print(f"Example missing IDs: {sorted(missing_ids)[:10]}")

    if args.delete:
        for path in unnecessary:
            path.unlink()

        print(f"\nDeleted {len(unnecessary)} unnecessary transcript files.")
    else:
        print("\nDRY RUN: No files deleted.")
        print("Use --delete to remove unnecessary transcript files.")


if __name__ == "__main__":
    main()
