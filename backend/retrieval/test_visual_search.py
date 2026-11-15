
from pathlib import Path
from visual_search import search

ROOT = Path(__file__).resolve().parents[2]

indexPath = ROOT / "indexes" / "phashIndex.json"

queryVideo = (ROOT / "data" / "assignment2_data" / "videos_480p" / "00031.mp4")

if not indexPath.is_file():
    raise FileNotFoundError("Visual search index not found")

results = search(videoPath=str(queryVideo), topK=50, indexPath=str(indexPath), frameNo=12)

print("Visual Search Results")

for result in results[:10]:
    print(result)

assert results, "No results"
