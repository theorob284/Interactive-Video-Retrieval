import json
from pathlib import Path

from retrieval.text_search import TextSearch

ROOT = Path(__file__).resolve().parents[3]
DATA_PATH = ROOT / "data" / "assignment2_data" / "videos_metadata.json"

with DATA_PATH.open(encoding="utf-8") as file:
    records = json.load(file)

metadata_by_id = {record["v3cId"]: record for record in records}

search_engine = TextSearch.load(ROOT / "indexes" / "metadata_bm25.pkl")

print("BM25 Text Search")
print("Enter a query, or type 'exit' to quit.")

while True:
    query = input("\nSearch: ").strip()

    if query.lower() == "exit":
        break

    results = search_engine.search(query, top_k=10)

    if not results:
        print("No matching videos found.")
        continue

    for rank, result in enumerate(results, start=1):
        video = metadata_by_id[result["video_id"]]

        print(f"{rank:2}: {result['video_id']} | {result['score']:.2f} | {video['title']}")