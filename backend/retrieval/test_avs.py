
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


def evaluate_avs(results, relevant_shots, k=10):
    """Print AVS evaluation metrics."""

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

    # Temporary example results.
    # Replace these with actual AVS retrieval results.
    sample_results = [
        {"shotID": "shot00031_1", "score": 9.5},
        {"shotID": "shot00031_2", "score": 9.2},
        {"shotID": "shot00032_1", "score": 8.8},
        {"shotID": "shot00033_1", "score": 8.1},
        {"shotID": "shot00034_1", "score": 7.9},
    ]

    # Temporary ground-truth labels.
    # Replace with shots manually judged relevant.
    relevant_shots = {
        "shot00031_1",
        "shot00032_1",
        "shot00033_1",
    }

    evaluate_avs(sample_results, relevant_shots, k=5)
