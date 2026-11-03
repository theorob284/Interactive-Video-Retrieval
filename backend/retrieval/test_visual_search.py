from pathlib import Path
import cv2

from visual_search import buildIndex, search

ROOT = Path(__file__).resolve().parents[1]

thumbnailDirectory = (
    ROOT.parent / "data" / "assignment2_data" / "thumbnails"
)

indexPath = ROOT.parent / "indexes" / "phashIndex.json"

queryVideo = (
    ROOT.parent / "data" / "assignment2_data"
    / "videos_480p" / "00031.mp4"
)

buildIndex(str(thumbnailDirectory), str(indexPath))

results = search(
    videoPath=str(queryVideo),
    topK=50,
    indexPath=str(indexPath),
    frameNo=12
)

for result in results[:10]:
    print(result)

assert results, "No results"