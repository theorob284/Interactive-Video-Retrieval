# Search Technologies — Assignment 2

Interactive video retrieval system built with **Django**, **React (Vite)** and **Python**. It supports three search modes:

- **Text Search (KIS-T):** find a particular video segment from a written description using BM25.
- **Visual Search (KIS-V):** find a matching segment using a reference image or video clip.
- **Ad-hoc Video Search (AVS):** find and select multiple relevant shots for a general information need.

Results can be inspected using shot thumbnails and local video playback. The application also supports DRES submission previews and live submissions when configured.

## Project layout

```text
interactive-video-retrieval/
├── backend/
│   ├── config/                  # Django settings and project configuration
│   ├── video_retrieval/
│   │   ├── models.py            # Video and Shot database models
│   │   ├── views.py             # Search, playback and DRES API views
│   │   ├── urls.py              # API routes
│   │   └── dres_client.py       # DRES payloads and HTTP client
│   ├── retrieval/
│   │   ├── text_search.py       # BM25 retrieval
│   │   ├── visual_search.py     # pHash indexing and image/video retrieval
│   │   ├── avs_search.py        # AVS result expansion and diversification
│   │   └── test_text_search.py  # BM25 tests
│   └── manage.py
├── frontend/
│   ├── src/                     # React pages and reusable components
│   ├── public/                  # Static homepage assets
│   ├── package.json
│   └── package-lock.json
├── scripts/
│   └── student_download.py     # Dataset downloader
├── data/                        # Local dataset (not committed)
│   └── assignment2_data/
├── indexes/                     # Generated visual index (not committed)
│   └── phashIndex.json
├── .env.example                 # Example DRES configuration
├── pyproject.toml               # Python dependencies
├── uv.lock                      # Locked Python dependencies
└── start_dev.ps1                # Starts development servers on Windows
```

## Fresh setup after cloning or pulling

These commands assume **Windows PowerShell**, with Python/`uv` and Node.js/`npm` installed. Run commands from the **repository root** unless stated otherwise.

### 1. Install dependencies

```powershell
uv sync
cd frontend
npm install
cd ..
```

`uv sync` recreates the Python environment according to `pyproject.toml` and `uv.lock`. If packages disappear after a pull, check whether they are declared in the project dependencies; do not rely on packages installed manually in a previous environment.

### 2. Configure DRES (optional for local searching)

Copy `.env.example` to `.env` if one does not already exist:

```powershell
Copy-Item .env.example .env
```

Fill in the DRES settings in `.env` when credentials are available (`DRES_BASE_URL`, `DRES_EVALUATION_ID`, `DRES_SESSION_TOKEN`). Keep `DRES_ENABLE_LIVE=false` until you deliberately intend to test a live submission. **Never commit `.env` or credentials.**

### 3. Download the dataset (if not already present)

The assignment uses a curated set of **1,000 V3C videos**. Download the recommended packages to the location expected by the application:

```powershell
uv run python scripts/student_download.py --packages metadata,msb,thumbnails,videos_480p --dest data/assignment2_data

# Extract .tsv files from zip
Expand-Archive -Path "data/assignment2_data/msb.zip" -DestinationPath "data/assignment2_data/msb" -Force

# Extract downloaded V3C1 transcripts from download folder into correct place
New-Item -ItemType Directory -Force "data/assignment2_data/transcripts"

Expand-Archive `
    -Path "$HOME\Downloads\V3C1.zip" `
    -DestinationPath "data/assignment2_data/transcripts" `
    -Force
```

Useful downloader commands:

```powershell
# View available packages
uv run python scripts/student_download.py --list

# Resume a download with fewer concurrent jobs
uv run python scripts/student_download.py --packages metadata,msb,thumbnails,videos_480p --dest data/assignment2_data --jobs 2

# Verify files already downloaded
uv run python scripts/student_download.py --verify-only --dest data/assignment2_data

# Extract test png images
New-Item -ItemType Directory -Force test_images

uv run python -c "import zipfile, pathlib; z=zipfile.ZipFile('data/assignment2_data/thumbnails/00031.zip'); n=next(x for x in z.namelist() if x.endswith('.png')); pathlib.Path('test_images/sample.png').write_bytes(z.read(n)); print('Extracted:', n)"
```

Downloads can be resumed by rerunning the command; existing valid files are skipped. Downloaded video and thumbnail files remain on disk and are served by Django — they are **not uploaded into the database**.

### 4. Prepare the Django database

```powershell
uv run python backend/manage.py migrate
uv run python backend/manage.py import_videos
```

`import_videos` loads video metadata and shot boundaries into the local SQLite database. The dataset must already be present before importing. On an existing environment with a populated database, you do **not** need to import again after every pull.

Check the database:

```powershell
uv run python backend/manage.py shell -c "from video_retrieval.models import Video, Shot; print('Videos:', Video.objects.count()); print('Shots:', Shot.objects.count())"
```

For the complete assignment dataset, expect **1,000 videos** and **141,156 shots**.

### 5. Build the visual search index (fresh environment only)

KIS-V uses `indexes/phashIndex.json`, generated from the downloaded thumbnail ZIP archives and official shot timings. The index is ignored by Git, so a newly cloned environment must generate it locally (or obtain a verified copy separately).

The current dataset extraction layout has TSVs under:

```text
data/assignment2_data/msb/msb/*.tsv
```

If the MSB files have not been extracted yet, extract the downloaded `msb.zip` first and confirm the directory containing the `.tsv` files. Then run:

```powershell
uv run python -c "from backend.retrieval.visual_search import buildIndex; buildIndex('data/assignment2_data/thumbnails', 'indexes/phashIndex.json', 'data/assignment2_data/msb/msb')"
```

This version of `buildIndex` reads thumbnail images directly from ZIP archives. The third argument is necessary for video-clip search because it attaches shot start/end times.

Verify the generated index:

```powershell
uv run python -c "import json; d=json.load(open('indexes/phashIndex.json')); print('Shots indexed:',len(d)); print('Videos indexed:',len(set(x['videoID'] for x in d))); print('Shots with timestamps:',sum('start' in x and 'end' in x for x in d))"
```

Expected counts for the complete dataset: **141,156 indexed shots**, **1,000 videos**, and **141,156 shots with timestamps**. Only rebuild the index when the indexed dataset or indexing algorithm changes; downloading missing MP4s alone does not require a rebuild if all thumbnails were already indexed.

> **Setup order:** Download the dataset and extract its MSB files → migrate and import the Django database → build the visual index → start the app.

## Running the application

### Windows convenience script

From the repository root:

```powershell
.\start_dev.ps1
```

Alternatively, open **two terminals**:

**Terminal 1 — Django**

```powershell
uv run python backend/manage.py runserver
```

**Terminal 2 — React**

```powershell
cd frontend
npm run dev
```

- Frontend: http://localhost:5173
- Django: http://127.0.0.1:8000
- The frontend accesses Django through `/api/` using the Vite development proxy.

Both servers must be running. Stop a server with `Ctrl+C`. If Django is started with `--noreload` (as in the Windows script), **restart it after editing Python files**.

## Useful commands

```powershell
# Check Django configuration
uv run python backend/manage.py check

# Run BM25 tests
uv run python backend/retrieval/test_text_search.py

# Check video and shot counts
uv run python backend/manage.py shell -c "from video_retrieval.models import Video, Shot; print(Video.objects.count(), Shot.objects.count())"

# Example BM25 search endpoint (open in a browser)
# http://127.0.0.1:8000/api/search/text/?q=snowboarding

# Update Python dependencies after pulling
uv sync

# Update frontend dependencies after pulling
cd frontend
npm install
cd ..
```

## Development notes

- Keep `data/`, `indexes/`, `.venv/`, `frontend/node_modules/` and `.env` out of Git.
- Commit dependency manifests and lockfiles: `pyproject.toml`, `uv.lock`, `frontend/package.json`, `frontend/package-lock.json`, plus Django migrations.
- Video IDs are five-character strings with leading zeros (for example, `00031`). Shot IDs look like `shot00031_1`.
- The master-shot boundary source uses **seconds**; Django stores shot times in **milliseconds**, as required by DRES.
- DRES previews do not send anything to the server. Live submissions require valid credentials and `DRES_ENABLE_LIVE=true`; use only during an authorised evaluation or practice session.
- The original full-resolution V3C collection is not required for this assignment. The `metadata`, `msb`, `thumbnails` and `videos_480p` packages are sufficient for the current workflow.
