# Search Technologies — Assignment 2

Interactive video retrieval system using Django, React and Python.

## Project Structure

```text
video-retrieval/
├── backend/
│   ├── config/                   # Django configuration
│   ├── videos/
│   │   └── models.py             # Database models
│   ├── retrieval/
│   │   ├── text_search.py        # BM25 (Student 1)
│   │   ├── test_text_search.py   # BM25 testing
│   │   ├── visual_search.py      # KIS-V (Student 2)
│   │   └── hybrid_search.py      # Integration (later)
│   └── manage.py
├── frontend/                     # React (later)
├── scripts/
│   └── student_download.py
├── data/                         # Downloaded dataset (Git ignored)
├── indexes/                      # Generated indexes (Git ignored)
├── pyproject.toml
└── uv.lock
```

## Getting Started

Clone the repository and install dependencies:

```bash
uv sync
```

To run Django:

```bash
uv run python backend/manage.py migrate
uv run python backend/manage.py runserver
```

## Downloading the Dataset

The supplied `student_download.py` script downloads the dataset from DCU's Onedata server.

Run commands from the project root.

**List available packages:**

```bash
uv run python scripts/student_download.py --list
```

**Download metadata and shot boundaries:**

```bash
uv run python scripts/student_download.py --packages metadata,msb --dest data/assignment2_data
```

**Download sample thumbnails and videos:**

```bash
uv run python scripts/student_download.py --packages thumbnails,videos_480p --videos 31-40 --dest data/assignment2_data
```

**Download all recommended packages:**

```bash
uv run python scripts/student_download.py --packages metadata,msb,thumbnails,videos_480p --dest data/assignment2_data
```

Downloads can be resumed by rerunning the same command. If connections are failing, add `--jobs 2` to reduce concurrent downloads.

See `--help` for all downloader options.

## Retrieval Development

### Student 1 — Text Retrieval

Location: `backend/retrieval/text_search.py`

BM25 is implemented and working against video metadata.

Test using:

```bash
uv run python backend/retrieval/test_text_search.py
```

Currently returns ranked video IDs and BM25 scores.

### Student 2 — Visual Retrieval

Location: `backend/retrieval/visual_search.py`

Start by implementing pHash-based image similarity using the downloaded thumbnails.

Suggested technologies: `opencv-python-headless`, `ImageHash`, `Pillow`, `NumPy`.

The intended interface is:

```python
def search_visual(image_path: str, top_k: int = 50):
    """
    Return ranked shot IDs and distances.

    Example:
    [
        {"shot_id": "shot00031_1", "distance": 4},
        {"shot_id": "shot00031_5", "distance": 7}
    ]
    """
```

Shot IDs follow `shot<v3cId>_<shot_number>`, e.g. `shot00031_1`.

Keep visual retrieval independent of Django for now. You can create separate scripts for indexing and testing, and store generated features in `indexes/`.

We'll integrate both search methods through Django once they're working independently.

## Notes

- Keep `data/`, `indexes/` and `.venv/` out of Git.
- Video IDs are five-digit strings (e.g. `00031`).
- Shot boundary timestamps are in seconds; DRES requires milliseconds.
- Do not commit downloaded video files or thumbnails.
