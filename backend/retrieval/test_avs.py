import csv
import json
from pathlib import Path
from text_search import TextSearch


def load(msb_directory):
    shots = []

    msb_directory = Path(msb_directory)

    for file_path in sorted(msb_directory.glob("*.tsv")):
        videoID = file_path.stem

        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter="\t")

            for shot_number, row in enumerate(reader, start=1):
                shots.append({
                    "shotID": f"shot{videoID}_{shot_number}",
                    "startTime": float(row["starttime"]),
                    "endTime": float(row["endtime"]),
                })

    return shots

def expandVidResults(vidResults, shots):
    shotsByVideo = {}

    for shot in shots:
        videoID = getVideoID(shot["shotID"])
        shotsByVideo.setdefault(videoID, []).append(shot)

    expandedResults = []

    for video_result in vidResults:
        videoID = video_result["videoID"]
        score = video_result["score"]

        for shot in shotsByVideo.get(videoID, []):
            expandedResults.append({
                **shot,
                "score": score
            })

    return expandedResults

def getVideoID(shot_id):
    return shot_id[4:].split("_")[0]


def precision_at_k(results, relShots, k=10):
    top = results[:k]

    if not top:
        return 0.0

    relevantNo = sum(
        1 for result in top
        if result["shotID"] in relShots
    )

    return relevantNo / len(top)


def kRecall(results, relShots, k=10):
    if not relShots:
        return 0.0

    top_ids = {
        result["shotID"]
        for result in results[:k]
    }

    found = len(top_ids.intersection(relShots))

    return found / len(relShots)


def video_diversity_at_k(results, k=10):
    top = results[:k]

    if not top:
        return 0.0

    VideoIDs = {
        getVideoID(result["shotID"])
        for result in top
    }

    return len(VideoIDs) / len(top)

def removeDupes(results):
    seen = set()
    uniqueResults = []

    for result in results:
        shot_id = result["shotID"]

        if shot_id not in seen:
            seen.add(shot_id)
            uniqueResults.append(result)

    return uniqueResults

def diversify(results, max_per_video=2):
    video_counts = {}
    diversified = []

    for result in results:
        videoID = getVideoID(result["shotID"])

        if video_counts.get(videoID, 0) < max_per_video:
            diversified.append(result)
            video_counts[videoID] = video_counts.get(videoID, 0) + 1

    return diversified

def evaluate(results, relShots, k=10):
    results = removeDupes(results)
    results = diversify(results, max_per_video=2)

    precision = precision_at_k(results, relShots, k)
    recall = kRecall(results, relShots, k)
    diversity = video_diversity_at_k(results, k)

    print(f"Precision@{k}: {precision:.2%}")
    print(f"Recall@{k}: {recall:.2%}")
    print(f"Video diversity@{k}: {diversity:.2%}")

    print("\nTop results:")

    for result in results[:k]:
        print(result)


if __name__ == "__main__":
    ROOT = Path(__file__).resolve().parents[2]

    metadata_path = ROOT / "data" / "assignment2_data" / "videos_metadata.json"
    msb_directory = ROOT / "data" / "assignment2_data" / "msb" / "msb"

    with open(metadata_path, "r", encoding="utf-8") as file:
        records = json.load(file)

    text_search = TextSearch(records)

    query = "music recording studio"

    vidResults = text_search.search(query, top_k=20)

    shots = load(msb_directory)

    Results = expandVidResults(vidResults, shots)

    Results = removeDupes(Results)
    Results = diversify(Results, max_per_video=2)

    for result in Results[:10]:
        print(result)