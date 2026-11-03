import json, imagehash
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

def search(
    imagePath: str,
    topK: int = 50,
    indexPath: str = "indexes/phashIndex.json"
):

    if topK <= 0:
        return []
    imagePath = Path(imagePath)
    indexPath = Path(indexPath)

    with Image.open(imagePath) as image:
        queryHash = imagehash.phash(image.convert("RGB"))
    index = json.loads(indexPath.read_text(encoding="utf-8"))

    results = []

    for item in index:
        dataHash = imagehash.hex_to_hash(item["hash"])
        distance = queryHash - dataHash
        results.append({
            "shotID": item["shotID"],
            "distance": int(distance) 
        })
    results.sort(key=lambda result: (
        result["distance"],
        result["shotID"]
    ))

    return results[:topK]