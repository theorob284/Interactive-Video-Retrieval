import json
from functools import lru_cache
from pathlib import Path

from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from video_retrieval.models import Video

from retrieval.text_search import TextSearch
from retrieval.visual_search import search as visual_search

import tempfile

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VISUAL_INDEX = PROJECT_ROOT / "indexes" / "phashIndex.json"
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

    engine, _ = get_search_engine()

    matches = engine.search(query, top_k=20)

    video_ids = [match["video_id"] for match in matches]

    videos = Video.objects.filter(video_id__in=video_ids)
    video_lookup = {video.video_id: video for video in videos}

    results = []

    for match in matches:
        video = video_lookup.get(match["video_id"])

        if video is None:
            continue

        results.append({
            "video_id": video.video_id,
            "title": video.title,
            "score": match["score"],
            "shot_count": video.shot_count,
        })

    return JsonResponse({"results": results})

@ensure_csrf_cookie
@require_GET
def csrf_token(request):
    return JsonResponse({"message": "CSRF cookie set"})

@require_POST
def visual_search_view(request):
    uploaded_file = request.FILES.get("file")

    if not uploaded_file:
        return JsonResponse({"error": "No image or video provided"}, status=400)

    allowed_extensions = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov"}
    suffix = Path(uploaded_file.name).suffix.lower()

    if suffix not in allowed_extensions:
        return JsonResponse({"error": "Unsupported file type"}, status=400)

    if not VISUAL_INDEX.is_file():
        return JsonResponse({"error": "Visual search index has not been created"}, status=503)

    # Save the uploaded file temporarily so OpenCV/Pillow can read it.
    with tempfile.TemporaryDirectory() as temp_dir:
        query_path = Path(temp_dir) / f"query{suffix}"

        with query_path.open("wb") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        results = visual_search(imagePath=str(query_path), indexPath=str(VISUAL_INDEX), topK=20)

    return JsonResponse({"results": results})