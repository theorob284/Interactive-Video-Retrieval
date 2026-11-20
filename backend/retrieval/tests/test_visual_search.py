from pathlib import Path
from retrieval.visual_search import search

ROOT = Path(__file__).resolve().parents[3]
INDEX_PATH = ROOT / "indexes" / "phashIndex.json"
QUERY_VIDEO = ROOT / "data" / "assignment2_data" / "videos_480p" / "00031.mp4"

def main():
    if not INDEX_PATH.is_file():
        raise FileNotFoundError("Visual search index not found")

    results = search(videoPath=str(QUERY_VIDEO), topK=50, indexPath=str(INDEX_PATH), frameNo=12,)
    print("Visual Search Results")

    for result in results[:10]:
        print(result)

    assert results, "No results"

if __name__ == "__main__":
    main()
