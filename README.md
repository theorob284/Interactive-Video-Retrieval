# Search Technologies — Assignment 2

Interactive video retrieval system using Django, React (Vite) and Python.

## Project Structure

```text
interactive-video-retrieval/
├── backend/
│   ├── config/                   # Django configuration
│   ├── videos/
│   │   ├── models.py             # Database models
│   │   ├── views.py              # API endpoints
│   │   └── urls.py               # API routes
│   ├── retrieval/
│   │   ├── text_search.py        # BM25 (Student 1)
│   │   ├── test_text_search.py   # BM25 testing
│   │   ├── visual_search.py      # KIS-V (Student 2)
│   │   └── hybrid_search.py      # Integration (later)
│   └── manage.py
├── frontend/                     # React + Vite
├── scripts/
│   └── student_download.py
├── data/                         # Downloaded dataset (Git ignored)
├── indexes/                      # Generated indexes (Git ignored)
├── pyproject.toml
└── uv.lock
```

## Getting Started

Clone the repository and open a terminal in the project root.

**Install Python dependencies:**

```bash
uv sync
```

**Install React dependencies:**

```bash
cd frontend
npm install
cd ..
```

**Set up the Django database:**

```bash
uv run python backend/manage.py migrate
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

## Running the Application

Open **two separate terminals** in VS Code.

**Terminal 1 — Django Backend**

From the project root:

```bash
uv run python backend/manage.py runserver
```

Django runs at http://127.0.0.1:8000.

**Terminal 2 — React Frontend**

```bash
cd frontend
npm run dev
```

React runs at http://localhost:5173.

Open **http://localhost:5173** to use the application.

Both servers must be running for frontend search to work.

The React frontend communicates with Django through `/api/`, using the Vite development proxy.

**Current text search API:**

```text
GET http://127.0.0.1:8000/api/search/text/?q=snowboarding
```

This returns ranked video IDs, titles and BM25 scores in JSON format.

## Retrieval Development

### Student 1 — Text Retrieval (KIS-T)

Location: `backend/retrieval/text_search.py`

BM25 is implemented and working against video metadata.

The implementation includes HTML cleaning, tokenisation and query-time stopword removal. It indexes titles, descriptions, tags, categories and channel information.

Test independently using:

```bash
uv run python backend/retrieval/test_text_search.py
```

Currently returns ranked video IDs and BM25 scores.

BM25 is also connected to the Django search API for use by the React frontend.

**Remaining work:** Import shot metadata, display thumbnails, implement video playback and shot selection, and integrate DRES submissions.

### Student 2 — Visual Retrieval (KIS-V)

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

- Run Python commands from the project root.
- Run `npm run dev` from `frontend/`.
- Keep `data/`, `indexes/`, `.venv/` and `node_modules/` out of Git.
- Commit `pyproject.toml`, `uv.lock`, `package.json`, `package-lock.json` and Django migrations.
- Video IDs are five-digit strings (e.g. `00031`).
- Shot boundary timestamps are in seconds; DRES requires milliseconds.
- Do not commit downloaded video files or thumbnails.
- Stop either development server using `Ctrl+C`.