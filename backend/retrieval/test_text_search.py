import json
from pathlib import Path

from text_search import TextSearch


ROOT = Path(__file__).resolve().parents[2]

metadata_path = (
    ROOT / "data" / "assignment2_data" / "videos_metadata.json"
)

with metadata_path.open(encoding="utf-8") as file:
    records = json.load(file)

search_engine = TextSearch(records)

results = search_engine.search("music recording studio")

for result in results[:10]:
    print(result)