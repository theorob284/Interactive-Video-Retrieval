
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django
django.setup()

from video_retrieval.models import Shot
from video_retrieval.views import get_search_engine
from retrieval.avs_search import (expandVidResults, removeDupes, diversify, video_diversity_at_k)


def main():
    engine, _ = get_search_engine()

    print("AVS Search")
    print("Enter a query, or type 'exit' to quit.")

    while True:
        query = input("\nSearch: ").strip()

        if query.lower() == "exit":
            break

        if not query:
            continue

        # Retrieve ranked videos using BM25
        vidResults = engine.search(query, top_k=20)

        if not vidResults:
            print("No matching videos found.")
            continue

        video_ids = [result["video_id"] for result in vidResults]

        # Load shot information from Django
        shots = [
            {
                "shotID": shot.shot_id,
                "startTime": shot.start_ms / 1000,
                "endTime": shot.end_ms / 1000,
            } for shot in Shot.objects.filter(video_id__in=video_ids).order_by("video_id", "shot_number")
        ]

        # Use your partner's AVS functions
        results = expandVidResults(vidResults, shots)

        results = removeDupes(results)
        results = diversify(results, max_per_video=2)

        if not results:
            print("No matching shots found.")
            continue

        print("\nTop AVS Results:")

        for rank, result in enumerate(results[:10], start=1):
            print(
                f"{rank:2}. "
                f"{result['shotID']} | "
                f"Score: {result['score']:.2f} | "
                f"{result['startTime']:.2f}s - "
                f"{result['endTime']:.2f}s"
            )

        diversity = video_diversity_at_k(results, k=10)

        print(f"\nVideo Diversity@10: {diversity:.2%}")

if __name__ == "__main__":
    main()
