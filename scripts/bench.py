import os
import sys
import time
from pathlib import Path
from zipfile import ZipFile

import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = next((folder for folder in (HERE, HERE.parent) if (folder / "backend" / "manage.py").is_file()),None,)
if ROOT is None:
    raise RuntimeError("Place bench.py in the project root or scripts/ folder")

sys.path.insert(0, str(ROOT / "backend"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django 

django.setup()

from django.test import Client, override_settings
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile
from video_retrieval import views

# Five timed repetitions of each of the ten text queries (50 requests per task).
REPS = 5
QUERIES = [
    "music recording studio",
    "person riding a bicycle",
    "city at night",
    "cooking food",
    "mountain landscape",
    "dog playing",
    "crowd cheering",
    "underwater fish",
    "car driving on a road",
    "guitar player",
]

DATA = ROOT / "data" / "assignment2_data"
CLIENT = Client()

def get_url(view):
    return reverse(view)

def checked(response, label):
    if response.status_code != 200:
        raise RuntimeError(f"{label} returned HTTP {response.status_code}: {response.content[:400]!r}")
    data = response.json()
    if "results" not in data:
        raise RuntimeError(f"{label} did not return a results list: {data}")
    return data


def benchmark(label, run, inputs, reps=REPS):
    if not inputs:
        print(f"{label}: skipped (no test inputs)")
        return

    print(f"\nWarming up {label}...", flush=True)
    checked(run(inputs[0]), label)

    times = []
    for repetition in range(reps):
        for item in inputs:
            start = time.perf_counter()
            response = run(item)
            duration_ms = (time.perf_counter() - start) * 1000
            checked(response, label)
            times.append(duration_ms)

    print(f"{label:<20} median={np.median(times):8.2f} ms p95={np.percentile(times, 95):8.2f} ms n={len(times)}")


def sample_images(count=10):
    thumbnail_dir = DATA / "thumbnails"
    for archive_path in sorted(thumbnail_dir.glob("*.zip")):
        with ZipFile(archive_path) as archive:
            names = [name for name in archive.namelist() if Path(name).suffix.lower() in {".png", ".jpg", ".jpeg"}]
            if len(names) >= count:
                return [(Path(name).name, archive.read(name)) for name in names[:count]]
    return []

def image_request(sample):
    filename, contents = sample
    upload = SimpleUploadedFile(filename, contents, content_type="image/png")
    return CLIENT.post(get_url(views.visual_search_view), {"file": upload})

def clip_request(clip_path):
    upload = SimpleUploadedFile(clip_path.name, clip_path.read_bytes(), content_type="video/mp4")
    return CLIENT.post(get_url(views.visual_search_view), {"file": upload})

@override_settings(ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"])
def main():
    text_url = get_url(views.text_search)
    avs_url = get_url(views.avs_search)

    print("Warm-query Django API benchmarks (milliseconds)")
    print(f"Repetitions per query: {REPS}")
    print("Cold-start model loading and frontend/network delays are excluded.")

    benchmark("KIS-T (hybrid)", lambda query: CLIENT.get(text_url, {"q": query}),QUERIES,)
    benchmark("AVS (OpenCLIP)",lambda query: CLIENT.get(avs_url, {"q": query, "engine": "clip"}),QUERIES,)

    images = sample_images()
    if images:
        benchmark("KIS-V image", image_request, images)
    else:
        print("KIS-V image: skipped (no thumbnail ZIP with 10 images found)")

    clip_dirs = [ROOT / "bench_clips", HERE / "bench_clips"]
    clip_dir = next((path for path in clip_dirs if path.is_dir()), None)
    clips = sorted(clip_dir.glob("*.mp4")) if clip_dir else []
    if clips:
        benchmark("KIS-V clip", clip_request, clips)
    else:
        print("KIS-V clip: skipped (put sample .mp4 files in bench_clips/)")


if __name__ == "__main__":
    main()
