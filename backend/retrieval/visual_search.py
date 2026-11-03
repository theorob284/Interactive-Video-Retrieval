import json, imagehash, cv2, math
from pathlib import Path
from PIL import Image

formats = {".jpg", ".jpeg", ".png", ".webp"}

def buildIndex(
    thumbnailDirectory: str,
    indexPath: str = "indexes/phashIndex.json"
):
    thumbnailDirectory = Path(thumbnailDirectory)
    indexPath = Path(indexPath)

    index = []

    for imagePath in sorted(thumbnailDirectory.rglob("*")):
        if imagePath.suffix.lower() not in formats:
            continue
        try:
            with Image.open(imagePath) as image:
                hash1 = str(imagehash.phash(image.convert("RGB")))
            index.append({
                "shotID": imagePath.stem,
                "hash": hash1,       
            })
        except Exception as error:
            print(f"Error with {imagePath}: {error}")

    indexPath.parent.mkdir(parents=True, exist_ok=True)
    indexPath.write_text(
        json.dumps(index, indent=2),
        encoding="utf-8"
    )

    print(f"Indexed {len(index)} images")
    print(f"Index saved to {indexPath}")


def extractFrames(videoPath: str, frameNo: int = 12):
    video = cv2.VideoCapture(str(videoPath))
    totalFrames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    frames = []
    


    try:
        if not video.isOpened():
            return frames

        if totalFrames <= 0:
            return frames

        frameIndex = sorted(set(
            int(i * (totalFrames - 1) / max(frameNo - 1, 1))
            for i in range (frameNo)
        ))

        for frameInd in frameIndex:
            video.set(cv2.CAP_PROP_POS_FRAMES, frameInd)
            success, frame = video.read()
            if not success:
                continue
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frames.append(Image.fromarray(frame))

    finally:
        video.release()

    return frames

def loadQueryFrames(queryPath: str, frameNo: int = 12):
    queryPath = Path(queryPath)

    # Single image query -> one "frame"
    if queryPath.suffix.lower() in formats:
        with Image.open(queryPath) as image:
            return [image.convert("RGB")]

    # Otherwise treat it as a video
    return extractFrames(str(queryPath), frameNo)


def search(
    videoPath: str = None,
    topK: int = 10,
    indexPath: str = "indexes/phashIndex.json",
    frameNo: int = 12,
    matchThreshold: int = 10,
    imagePath: str = None
):
    queryPath = videoPath or imagePath
    if queryPath is None:
        raise ValueError("Provide videoPath or imagePath")

    if topK <= 0:
        return []

    queryFrames = loadQueryFrames(queryPath, frameNo)
    if not queryFrames:
        return []

    index = json.loads(Path(indexPath).read_text(encoding="utf-8"))
    if not index:
        return []

    queryHashes = [
        imagehash.phash(frame.convert("RGB"))
        for frame in queryFrames
    ]

    results = []

    for item in index:
        dataHash = imagehash.hex_to_hash(item["hash"])

        distances = sorted(
            int(queryHash - dataHash)
            for queryHash in queryHashes
        )

        bestCount = max(1, math.ceil(len(distances) * 0.25))
        avgDistance = sum(distances[:bestCount]) / bestCount

        results.append({
            "shotID": item["shotID"],
            "distance": round(avgDistance, 2),
            "matchedFrames": sum(d <= matchThreshold for d in distances)
        })

    results.sort(key=lambda result: (
        result["distance"],
        result["shotID"]
    ))

    return results[:topK]