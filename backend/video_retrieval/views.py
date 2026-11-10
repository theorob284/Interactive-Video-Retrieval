import json
from functools import lru_cache
from pathlib import Path

from django.http import JsonResponse
from django.views.decorators.http import require_GET

from retrieval.text_search import TextSearch


DATA_PATH = (Path(__file__).resolve().parents[2]/ "data"/"assignment2_data"/"videos_metadata.json")

@lru_cache(maxsize=1)
def get_search_engine():
    """Load metadata and initialise BM25 once."""
    with DATA_PATH.open(encoding="utf-8") as file:
        records = json.load(file)

    engine = TextSearch(records)

    metadata = {record["v3cId"]: record for record in records}

    return engine, metadata


@require_GET
def text_search(request):
    query = request.GET.get("q", "").strip()

    if not query:
        return JsonResponse({"results": []})

    engine, metadata = get_search_engine()

    matches = engine.search(query, top_k=20)

    results = []

    for match in matches:
        video = metadata[match["video_id"]]

        results.append({
            "video_id": match["video_id"],
            "title": video.get("title", ""),
            "score": match["score"],
        })

    return JsonResponse({"results": results})
