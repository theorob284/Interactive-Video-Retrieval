
from pathlib import Path

from visual_search import buildIndex, search


ROOT = Path(__file__).resolve().parents[1]

thumbnailDirectory = ROOT / "data" / "synthetic_thumbnails"
queryImage = ROOT / "data" / "synthetic_queries" / "shot00031_1_query.png"

indexPath = ROOT.parent / "indexes" / "phashIndex.json"

# Build the visual index
buildIndex(
    str(thumbnailDirectory),
    str(indexPath),
)

# Search for visually similar shots
results = search(
    imagePath=str(queryImage),
    topK=50,
    indexPath=str(indexPath),
)

# Display the top 10 results
for result in results[:10]:
    print(result)
