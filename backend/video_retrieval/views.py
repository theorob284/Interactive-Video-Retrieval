import json
from functools import lru_cache
from pathlib import Path
import tempfile
from zipfile import ZipFile, BadZipFile
import re
import os

from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import JsonResponse, HttpResponse, Http404, FileResponse, StreamingHttpResponse
from django.views.decorators.http import require_GET, require_POST
from django.shortcuts import get_object_or_404

from video_retrieval.models import Video, Shot
from video_retrieval.dres_client import build_submission, build_avs_submission, submit_to_dres

from retrieval.text_search import TextSearch
from retrieval.visual_search import search as visual_search
from retrieval.avs_search import expandVidResults, removeDupes, diversify

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VISUAL_INDEX = PROJECT_ROOT / "indexes" / "phashIndex.json"
DATA_PATH = (Path(__file__).resolve().parents[2]/ "data"/"assignment2_data"/"videos_metadata.json")
TEXT_INDEX = PROJECT_ROOT / "indexes" / "text_bm25.pkl"


@lru_cache(maxsize=1)
def get_search_engine():
    """Load the saved BM25 index once and cache it."""

    engine = TextSearch.load(TEXT_INDEX)

    with DATA_PATH.open(encoding="utf-8") as file:
        records = json.load(file)

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

    thumbnail_lookup = {
        video.video_id: (f"/api/shots/shot{video.video_id}_{max(1, (video.shot_count + 1) // 2)}/thumbnail/") for video in videos
    }

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
            "thumbnail_url": thumbnail_lookup.get(video.video_id)
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

    with tempfile.TemporaryDirectory() as temp_dir:
        query_path = Path(temp_dir) / f"query{suffix}"

        with query_path.open("wb") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        try:
            results = visual_search(imagePath=str(query_path), indexPath=str(VISUAL_INDEX), topK=20)
        except ValueError as error:
            return JsonResponse({"error": str(error)}, status=400)

    enriched_results = []

    for result in results:

        if "shotID" in result:
            # Image search: result already identifies a shot.
            shot = Shot.objects.filter(shot_id=result["shotID"]).first()

        else:
            # Video search: locate an official shot overlapping the estimated matching segment.
            video_id = result["videoID"]
            start_ms = round(result["start"] * 1000)
            end_ms = round(result["end"] * 1000)

            candidates = Shot.objects.filter(video_id=video_id, start_ms__lt=end_ms, end_ms__gt=start_ms)

            shot = max(candidates, key=lambda s: min(s.end_ms, end_ms) - max(s.start_ms, start_ms), default=None)

        if shot is None:
            continue

        enriched_results.append({
            **result,
            "shotID": shot.shot_id,
            "video_id": shot.video_id,
            "start_ms": shot.start_ms,
            "end_ms": shot.end_ms,
            "thumbnail_url": f"/api/shots/{shot.shot_id}/thumbnail/",
        })

    return JsonResponse({"results": enriched_results})

@require_GET
def video_shots(request, video_id):
    video = get_object_or_404(Video, video_id=video_id)

    shots = video.shots.all()

    results = [
        {
            "shot_id": shot.shot_id,
            "start_ms": shot.start_ms,
            "end_ms": shot.end_ms,
            "thumbnail_url": f"/api/shots/{shot.shot_id}/thumbnail/",
        }
        for shot in shots
    ]

    return JsonResponse({
        "video_id": video.video_id,
        "title": video.title,
        "shots": results,
    })

@require_GET
def shot_thumbnail(request, shot_id):
    shot = get_object_or_404(Shot, shot_id=shot_id)

    archive_path = (
        PROJECT_ROOT / "data" / "assignment2_data" / "thumbnails" / f"{shot.video_id}.zip"
    )

    if not archive_path.is_file():
        raise Http404("Thumbnail archive not found")

    image_name = f"{shot.shot_id}.png"

    try:
        with ZipFile(archive_path) as archive:
            try:
                image_data = archive.read(image_name)
            except KeyError:
                # Allow for images stored inside subdirectories.
                matches = [name for name in archive.namelist() if Path(name).name == image_name]
                if not matches:
                    raise Http404("Thumbnail not found")

                image_data = archive.read(matches[0])
    except BadZipFile:
        raise Http404("Invalid thumbnail archive")

    response = HttpResponse(image_data, content_type="image/png")
    response["Cache-Control"] = "public, max-age=86400"
    return response

@require_GET
def video_file(request, video_id):
    video = get_object_or_404(Video, video_id=video_id)

    path = PROJECT_ROOT / "data" / "assignment2_data" / "videos_480p" / f"{video.video_id}.mp4"

    file_size = path.stat().st_size
    range_header = request.headers.get("Range")

    # Normal request: serve the complete video.
    if not range_header:
        response = FileResponse(path.open("rb"), content_type="video/mp4")
        response["Accept-Ranges"] = "bytes"
        return response

    match = re.fullmatch(r"bytes=(\d*)-(\d*)", range_header)

    if not match:
        response = HttpResponse(status=416)
        response["Content-Range"] = f"bytes */{file_size}"
        return response

    start_text, end_text = match.groups()

    if not start_text and not end_text:
        response = HttpResponse(status=416)
        response["Content-Range"] = f"bytes */{file_size}"
        return response

    if start_text:
        start = int(start_text)
        end = int(end_text) if end_text else file_size - 1
        end = min(end, file_size - 1)
    else:
        suffix_length = int(end_text)
        start = max(0, file_size - suffix_length)
        end = file_size - 1

    if start >= file_size or start > end or file_size == 0:
        response = HttpResponse(status=416)
        response["Content-Range"] = f"bytes */{file_size}"
        return response

    length = end - start + 1

    def stream_file():
        with path.open("rb") as file:
            file.seek(start)
            remaining = length

            while remaining > 0:
                chunk = file.read(min(64 * 1024, remaining))

                if not chunk:
                    break

                remaining -= len(chunk)
                yield chunk

    response = StreamingHttpResponse(stream_file(), status=206, content_type="video/mp4")

    response["Accept-Ranges"] = "bytes"
    response["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    response["Content-Length"] = str(length)

    return response

@require_POST
def submit_shot(request):
    try:
        data = json.loads(request.body)
    except (ValueError, UnicodeDecodeError):
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    if not isinstance(data, dict):
        return JsonResponse({"error": "Expected a JSON object"}, status=400)

    shot_id = data.get("shot_id")

    if not isinstance(shot_id, str) or not shot_id:
        return JsonResponse({"error": "Missing shot ID"}, status=400)

    shot = get_object_or_404(Shot, shot_id=shot_id)

    payload = build_submission(shot)

    # Preview only - no request sent to DRES.
    if data.get("submit") is not True:
        return JsonResponse({
            "mode": "preview",
            "payload": payload,
        })

    # Protect against accidental live submissions.
    if os.getenv("DRES_ENABLE_LIVE", "false").lower() != "true":
        return JsonResponse({"error": "Live DRES submission is disabled"}, status=403)

    required = (
        "DRES_BASE_URL",
        "DRES_EVALUATION_ID",
        "DRES_SESSION_TOKEN",
    )

    if any(not os.getenv(name) for name in required):
        return JsonResponse({"error": "DRES configuration is incomplete"}, status=503)

    try:
        status, body = submit_to_dres(payload)
    except (ConnectionError, TimeoutError):
        return JsonResponse({"error": "Unable to reach DRES"}, status=502)

    try:
        dres_response = json.loads(body)
    except ValueError:
        dres_response = {"message": body[:1000]}

    return JsonResponse({
        "mode": "live",
        "accepted": status in (200, 202),
        "dres_status": status,
        "response": dres_response,
    })


@require_POST
def submit_avs(request):
    try:
        data = json.loads(request.body)
    except (ValueError, UnicodeDecodeError):
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    if not isinstance(data, dict):
        return JsonResponse({"error": "Expected a JSON object"}, status=400)

    shot_ids = data.get("shot_ids")

    if not isinstance(shot_ids, list) or not shot_ids or not all(isinstance(s, str) and s for s in shot_ids):
        return JsonResponse({"error": "Provide a list of shot IDs"}, status=400)

    if len(shot_ids) != len(set(shot_ids)):
        return JsonResponse({"error": "Duplicate shots selected"}, status=400)

    shot_lookup = {shot.shot_id: shot for shot in Shot.objects.filter(shot_id__in=shot_ids)}

    if len(shot_lookup) != len(shot_ids):
        return JsonResponse({"error": "One or more shots were not found"}, status=404)

    shots = [shot_lookup[shot_id] for shot_id in shot_ids]
    payload = build_avs_submission(shots)

    # Preview without submitting to DRES.
    if data.get("submit") is not True:
        return JsonResponse({
            "mode": "preview",
            "payload": payload,
        })

    if os.getenv("DRES_ENABLE_LIVE", "false").lower() != "true":
        return JsonResponse({"error": "Live DRES submission is disabled"}, status=403)

    required = (
        "DRES_BASE_URL",
        "DRES_EVALUATION_ID",
        "DRES_SESSION_TOKEN",
    )

    if any(not os.getenv(name) for name in required):
        return JsonResponse({"error": "DRES configuration is incomplete"}, status=503)

    try:
        status, body = submit_to_dres(payload)
    except (ConnectionError, TimeoutError):
        return JsonResponse({"error": "Unable to reach DRES"}, status=502)

    try:
        dres_response = json.loads(body)
    except ValueError:
        dres_response = {"message": body[:1000]}

    return JsonResponse({
        "mode": "live",
        "accepted": status in (200, 202),
        "dres_status": status,
        "response": dres_response,
    })

@require_GET
def avs_search(request):
    query = request.GET.get("q", "").strip()

    if not query:
        return JsonResponse({"results": []})

    # Reuse existing BM25
    engine, _ = get_search_engine()
    vidResults = engine.search(query, top_k=20)

    video_ids = [result["video_id"] for result in vidResults]

    # Retrieve shot boundaries from Django instead of TSV files
    shots = [
        {
            "shotID": shot.shot_id,
            "startTime": shot.start_ms / 1000,
            "endTime": shot.end_ms / 1000,
        } for shot in Shot.objects.filter(video_id__in=video_ids).order_by("video_id", "shot_number")
    ]

    results = expandVidResults(vidResults, shots)
    results = removeDupes(results)
    results = diversify(results, max_per_video=2)

    # Convert results into the format React already uses
    formatted = [
        {
            "shot_id": result["shotID"],
            "video_id": result["shotID"][4:].split("_")[0],
            "start_ms": round(result["startTime"] * 1000),
            "end_ms": round(result["endTime"] * 1000),
            "score": result["score"],
            "thumbnail_url": f'/api/shots/{result["shotID"]}/thumbnail/',
        }
        for result in results[:40]
    ]

    return JsonResponse({"results": formatted})
