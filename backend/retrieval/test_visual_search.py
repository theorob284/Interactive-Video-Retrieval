from pathlib import Path
import cv2

from visual_search import buildIndex, search

ROOT = Path(__file__).resolve().parents[1]

thumbnailDirectory = ROOT / "data" / "synthetic_thumbnails"
queryImage = ROOT / "data" / "synthetic_queries" / "shot00031_1_query.png"
queryVideo = queryImage.with_suffix(".mp4")
indexPath = ROOT.parent / "indexes" / "phashIndex.json"

# Build a 2-second test clip from the query image
img = cv2.imread(str(queryImage))
h, w = img.shape[:2]
writer = cv2.VideoWriter(
    str(queryVideo), cv2.VideoWriter_fourcc(*"mp4v"), 25, (w, h)
)
for _ in range(50):
    writer.write(img)
writer.release()

buildIndex(str(thumbnailDirectory), str(indexPath))

results = search(
    videoPath=str(queryVideo),
    topK=50,
    indexPath=str(indexPath),
)

for result in results[:10]:
    print(result)

assert results and results[0]["shotID"] == "shot00031_1", "expected shot00031_1 first"