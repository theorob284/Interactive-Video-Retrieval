import csv
import json
from pathlib import Path
from retrieval.text_search import TextSearch

def prioritiseTranscriptShots(results, passages_by_video, boost=0.4):
    ranked_results = []

    for result in results:
        shot = result.copy()

        shot_id = shot["shotID"]
        video_id = getVideoID(shot_id)

        # Shot times are stored in seconds; convert to milliseconds.
        shot_start = round(shot["startTime"] * 1000)
        shot_end = round(shot["endTime"] * 1000)

        best_match = 0.0

        for passage in passages_by_video.get(video_id, []):
            for segment in passage.get("matched_segments", []):
                overlap = max(
                    0,
                    min(shot_end, segment["end_ms"])
                    - max(shot_start, segment["start_ms"]),
                )

                if overlap <= 0:
                    continue

                segment_duration = max(
                    1,
                    segment["end_ms"] - segment["start_ms"],
                )

                overlap_fraction = overlap / segment_duration

                match_score = (
                    overlap_fraction
                    * passage["relevance"]
                    * segment["coverage"]
                )

                best_match = max(best_match, match_score)

        original_score = shot["score"]

        shot["score"] = (
            original_score * (1 + boost * best_match)
        )

        shot["transcript_boosted"] = best_match > 0

        ranked_results.append(shot)

    return sorted(
        ranked_results,
        key=lambda result: result["score"],
        reverse=True,
    )

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
        videoID = video_result["video_id"]
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

def diversify(results, max_per_video=2, prefer_top=False):
    diversified = []
    shotsByVideo = {}

    for result in results:
        videoID = getVideoID(result["shotID"])
        shotsByVideo.setdefault(videoID, []).append(result)

    for videoID, video_shots in shotsByVideo.items():
        count = min(max_per_video, len(video_shots))

        if count <= 0:
            continue

        has_transcript_match = any(
            shot.get("transcript_boosted", False)
            for shot in video_shots
        )

        if prefer_top and has_transcript_match:
            matched_shots = [
                shot for shot in video_shots
                if shot.get("transcript_boosted", False)
            ]

            selected = sorted(
                matched_shots,
                key=lambda result: result["score"],
                reverse=True,
            )[:count]

        else:
            # Original evenly spaced selection.
            chronological = sorted(
                video_shots,
                key=lambda result: result["startTime"],
            )

            selected = []

            for i in range(count):
                index = (
                    (2 * i + 1) * len(chronological)
                    // (2 * count)
                )
                selected.append(chronological[index])

        diversified.extend(selected)

    diversified.sort(
        key=lambda result: result["score"],
        reverse=True,
    )

    return diversified


def evaluate(results, relShots, k=10):
    results = removeDupes(results)
    results = diversify(
        results,
        max_per_video=2,
        prefer_top=True,
    )

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
