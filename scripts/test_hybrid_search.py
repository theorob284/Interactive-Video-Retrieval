
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.retrieval.text_search import TextSearch
from backend.retrieval.hybrid_search import HybridSearch

metadata = TextSearch.load(ROOT / "indexes/metadata_bm25.pkl")
transcripts = TextSearch.load(ROOT / "indexes/transcript_bm25.pkl")

hybrid = HybridSearch(
    metadata,
    transcripts,
    metadata_weight=1.0,
    transcript_weight=1.0,
)

print("BM25 Retrieval Comparison")
print("Type 'exit' to quit.")

while True:
    query = input("\nSearch: ").strip()

    if query.lower() == "exit":
        break

    for name, engine in [
        ("Metadata", metadata),
        ("Transcripts", transcripts),
        ("Hybrid", hybrid),
    ]:
        print(f"\n{name}:")

        results = engine.search(query, top_k=10)

        for rank, result in enumerate(results, start=1):
            print(
                f'{rank:2}. {result["video_id"]} '
                f'| {result["score"]:.4f}'
            )
