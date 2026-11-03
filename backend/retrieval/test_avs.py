import csv
import json
from pathlib import Path
from text_search import TextSearch


def load_shots(msb_directory):
    """
    Load shot IDs and timestamps from the MSB files.
    """
    shots = []

    msb_directory = Path(msb_directory)

    for file_path in sorted(msb_directory.glob("*.tsv")):
        video_id = file_path.stem

        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter="\t")

            for shot_number, row in enumerate(reader, start=1):
                shots.append({
                    "shotID": f"shot{video_id}_{shot_number}",
                    "startTime": float(row["starttime"]),
                    "endTime": float(row["endtime"]),
                })

    return shots

def expand_video_results(video_results, shots):
    """
    Convert video-level results into shot-level results.
    Each shot inherits its parent video's BM25 score.
    """
    shots_by_video = {}

    for shot in shots:
        video_id = get_video_id(shot["shotID"])
        shots_by_video.setdefault(video_id, []).append(shot)

    expanded_results = []

    for video_result in video_results:
        video_id = video_result["video_id"]
        score = video_result["score"]

        for shot in shots_by_video.get(video_id, []):
            expanded_results.append({
                **shot,
                "score": score
            })

    return expanded_results

def get_video_id(shot_id):
    """
    Extract the video ID from a shot ID.

    Example: shot00031_1 -> 00031
    """
    return shot_id[4:].split("_")[0]


def precision_at_k(results, relevant_shots, k=10):
    """Calculate the proportion of the top K results that are relevant."""
    top_results = results[:k]

    if not top_results:
        return 0.0

    relevant_count = sum(
        1 for result in top_results
        if result["shotID"] in relevant_shots
    )

    return relevant_count / len(top_results)


def recall_at_k(results, relevant_shots, k=10):
    """Calculate the proportion of known relevant shots retrieved in top K."""
    if not relevant_shots:
        return 0.0

    top_ids = {
        result["shotID"]
        for result in results[:k]
    }

    found = len(top_ids.intersection(relevant_shots))

    return found / len(relevant_shots)


def video_diversity_at_k(results, k=10):
    """Calculate the proportion of different videos in the top K results."""
    top_results = results[:k]

    if not top_results:
        return 0.0

    video_ids = {
        get_video_id(result["shotID"])
        for result in top_results
    }

    return len(video_ids) / len(top_results)

def remove_duplicates(results):
    """
    Remove repeated shot IDs while preserving ranking order.
    """
    seen = set()
    unique_results = []

    for result in results:
        shot_id = result["shotID"]

        if shot_id not in seen:
            seen.add(shot_id)
            unique_results.append(result)

    return unique_results

def diversify_results(results, max_per_video=2):
    """
    Limit the number of shots returned from each video.
    """
    video_counts = {}
    diversified = []

    for result in results:
        video_id = get_video_id(result["shotID"])

        if video_counts.get(video_id, 0) < max_per_video:
            diversified.append(result)
            video_counts[video_id] = video_counts.get(video_id, 0) + 1

    return diversified

def evaluate_avs(results, relevant_shots, k=10):
    """Print AVS evaluation metrics."""

    results = remove_duplicates(results)
    results = diversify_results(results, max_per_video=2)

    precision = precision_at_k(results, relevant_shots, k)
    recall = recall_at_k(results, relevant_shots, k)
    diversity = video_diversity_at_k(results, k)

    print("\nAVS EVALUATION")
    print("--------------------------")
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

    video_results = text_search.search(query, top_k=20)

    shots = load_shots(msb_directory)

    avs_results = expand_video_results(video_results, shots)

    avs_results = remove_duplicates(avs_results)
    avs_results = diversify_results(avs_results, max_per_video=2)

    for result in avs_results[:10]:
        print(result)