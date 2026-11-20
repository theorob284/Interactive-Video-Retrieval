import json, imagehash, cv2, math
import csv
import numpy as np
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from PIL import Image
from zipfile import ZipFile
from io import BytesIO

HASH_BITS = 64

formats = {".jpg", ".jpeg", ".png", ".webp"}

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INDEX = PROJECT_ROOT / "indexes" / "phashIndex.json"

def getVideoID(shot_id):
    return shot_id[4:].split("_")[0]

def normaliseShotID(stem):
    return "_".join(stem.split("_")[:2])

def isFlat(image, minStd=12):
    return float(np.asarray(image.convert("L")).std()) < minStd

def loadShotTimes(msbDirectory):
    times = {}
    for filePath in sorted(Path(msbDirectory).glob("*.tsv")):
        videoID = filePath.stem
        with open(filePath, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter="\t")
            for shotNumber, row in enumerate(reader, start=1):
                times[f"shot{videoID}_{shotNumber}"] = (float(row["starttime"]), float(row["endtime"]),)
    return times

_POPCOUNT = np.array([bin(i).count("1") for i in range(256)], dtype=np.uint8)

def hammingRow(queryHash, dataHashes):
    x = dataHashes ^ np.uint64(queryHash)
    if hasattr(np, "bitwise_count"):
        return np.bitwise_count(x).astype(np.int16)

    return _POPCOUNT[x.view(np.uint8).reshape(-1, 8)].sum(axis=1, dtype=np.int16)

@lru_cache(maxsize=2)
def loadIndex(indexPath):
    index = json.loads(Path(indexPath).read_text(encoding="utf-8"))
    index = [item for item in index if not item.get("flat")]
    return {
        "shotIDs": [item["shotID"] for item in index],
        "hashes": np.array([int(item["hash"], 16) for item in index], dtype=np.uint64),
        "videoIDs": [item.get("videoID") or getVideoID(item["shotID"]) for item in index],
        "start": np.array([item.get("start", np.nan) for item in index], dtype=float),
        "end": np.array([item.get("end", np.nan) for item in index], dtype=float),
    }

def buildIndex(thumbnailDirectory: str, indexPath: str = DEFAULT_INDEX, msbDirectory: str = None, minStd: float = 12):
    thumbnailDirectory = Path(thumbnailDirectory)
    indexPath = Path(indexPath)

    shotTimes = loadShotTimes(msbDirectory) if msbDirectory else None

    seen = set()
    flatCount = 0
    dupeCount = 0
    untimedCount = 0
    index = []

    for zipPath in sorted(thumbnailDirectory.rglob("*.zip")):
        print(f"Indexing {zipPath.name}")

        try:
            with ZipFile(zipPath) as archive:
                for filename in archive.namelist():
                    imagePath = Path(filename)

                    if imagePath.suffix.lower() not in formats:
                        continue

                    shotID = normaliseShotID(imagePath.stem)

                    if shotID in seen:
                        dupeCount += 1
                        continue

                    try:
                        with Image.open(BytesIO(archive.read(filename))) as image:
                            rgb = image.convert("RGB")
                            hash1 = str(imagehash.phash(rgb))
                            flat = isFlat(rgb, minStd)

                        item = {"shotID": shotID, "hash": hash1, "videoID": getVideoID(shotID), "flat": flat,}

                        if shotTimes is not None:
                            if shotID not in shotTimes:
                                untimedCount += 1
                                continue

                            item["start"], item["end"] = shotTimes[shotID]

                        seen.add(shotID)
                        flatCount += flat
                        index.append(item)

                    except Exception as error:
                        print(f"Error indexing {filename}: {error}")

        except Exception as error:
            print(f"Error opening {zipPath}: {error}")

    indexPath.parent.mkdir(parents=True, exist_ok=True)

    indexPath.write_text(json.dumps(index, indent=2), encoding="utf-8")

    loadIndex.cache_clear()

    print(f"Indexed {len(index)} images")
    print(f"Flat images: {flatCount}")
    print(f"Duplicates skipped: {dupeCount}")
    print(f"Shots without timestamps: {untimedCount}")
    print(f"Index saved to {indexPath}")

def extractFrames(videoPath: str, frameNo: int = 12, minStd: float = 12):
    video = cv2.VideoCapture(str(videoPath))
    totalFrames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = video.get(cv2.CAP_PROP_FPS) or 25
    frames = []

    try:
        if not video.isOpened():
            return frames, 0.0

        if totalFrames <= 0:
            return frames, 0.0

        frameIndex = sorted(set(int((i + 0.5) * totalFrames / frameNo) for i in range (frameNo)))

        for frameInd in frameIndex:
            video.set(cv2.CAP_PROP_POS_FRAMES, frameInd)
            success, frame = video.read()
            if not success:
                continue
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            image = Image.fromarray(frame)
            if isFlat(image, minStd):
                continue
            frames.append((frameInd / fps, image))

    finally:
        video.release()

    return frames, totalFrames / fps

def loadQueryFrames(queryPath: str, frameNo: int = 12):
    queryPath = Path(queryPath)
    if queryPath.suffix.lower() in formats:
        with Image.open(queryPath) as image:
            return [(0.0, image.convert("RGB"))], 0.0

    return extractFrames(str(queryPath), frameNo)

def rankShots(dist, shotIDs, topK, matchThreshold):
    best = dist.min(axis=0)
    matched = (dist <= matchThreshold).sum(axis=0)

    order = np.argsort(best, kind="stable")[:topK]

    return [
        {
            "shotID": shotIDs[i],
            "distance": round(float(best[i]), 2),
            "matchedFrames": int(matched[i]),
        } for i in order
    ]

def rankSegments(dist, queryFrames, duration, data, topK, matchThreshold, binSeconds, candidatesPerFrame):
    start, end, videoIDs = data["start"], data["end"], data["videoIDs"]

    timed = ~np.isnan(start)
    if not timed.any():
        raise ValueError("This index has no shot timing. Rebuild it with buildIndex(thumbnailDirectory, msbDirectory=...).")
    dist[:, ~timed] = HASH_BITS + 1

    votes = defaultdict(lambda: [0.0, 0])

    for (tq, _), row in zip(queryFrames, dist):
        candidates = np.flatnonzero(row <= matchThreshold)
        if len(candidates) > candidatesPerFrame:
            keep = np.argpartition(row[candidates], candidatesPerFrame)[:candidatesPerFrame]
            candidates = candidates[keep]

        best = {}
        for j in candidates:
            weight = 1 - float(row[j]) / HASH_BITS
            lo = max(0, math.floor((start[j] - tq) / binSeconds))
            hi = math.floor((end[j] - tq) / binSeconds)
            for b in range(lo, hi + 1):
                key = (videoIDs[j], b)
                if weight > best.get(key, 0.0):
                    best[key] = weight

        for key, weight in best.items():
            votes[key][0] += weight
            votes[key][1] += 1

    perVideo = defaultdict(dict)
    for (videoID, b), (score, frames) in votes.items():
        perVideo[videoID][b] = (score, frames)

    results = []
    for videoID, bins in perVideo.items():
        peak = max(score for score, _ in bins.values())
        peakBins = sorted(b for b, (score, _) in bins.items() if score >= peak - 1e-9)

        run = [peakBins[0]]
        for b in peakBins[1:]:
            if b != run[-1] + 1:
                break
            run.append(b)

        estStart = (run[0] + run[-1] + 1) / 2 * binSeconds

        results.append({
            "videoID": videoID,
            "start": round(estStart, 1),
            "end": round(estStart + duration, 1),
            "score": round(peak, 3),
            "matchedFrames": max(bins[b][1] for b in run),
        })

    results.sort(key=lambda result: (-result["score"], result["videoID"]))
    return results[:topK]

def search(videoPath: str = None, topK: int = 10, indexPath: str = DEFAULT_INDEX, frameNo: int = 12, matchThreshold: int = 10, imagePath: str = None, binSeconds: float = 4.0, candidatesPerFrame: int = 50):
    queryPath = videoPath or imagePath
    if queryPath is None:
        raise ValueError("Provide videoPath or imagePath")

    if topK <= 0:
        return []

    queryFrames, duration = loadQueryFrames(queryPath, frameNo)
    if not queryFrames:
        return []

    data = loadIndex(Path(indexPath))
    if not len(data["hashes"]):
        return []

    queryHashes = [int(str(imagehash.phash(frame.convert("RGB"))), 16) for _, frame in queryFrames]

    dist = np.stack([hammingRow(q, data["hashes"]) for q in queryHashes])

    if Path(queryPath).suffix.lower() in formats:
        return rankShots(dist, data["shotIDs"], topK, matchThreshold)

    return rankSegments(dist, queryFrames, duration, data, topK, matchThreshold, binSeconds, candidatesPerFrame)