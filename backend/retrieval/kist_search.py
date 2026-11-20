def combine_kist_rankings(bm25_results, clip_results, top_k=20, bm25_weight=0.95, clip_weight=1.0, rrf_k=60,):
    combined = {}

    # First route: existing BM25 video ranking.
    for rank, result in enumerate(bm25_results, start=1):
        video_id = str(result["video_id"])

        entry = combined.setdefault(video_id, {"video_id": video_id, "score": 0.0, "bm25_result": None, "clip_shot_id": None,})
        entry["score"] += bm25_weight / (rrf_k + rank)
        entry["bm25_result"] = result

    seen_videos = set()
    video_rank = 0

    for result in clip_results:
        video_id = str(result["video_id"])

        if video_id in seen_videos:
            continue

        seen_videos.add(video_id)
        video_rank += 1

        entry = combined.setdefault(video_id, {"video_id": video_id, "score": 0.0, "bm25_result": None, "clip_shot_id": None})
        entry["score"] += clip_weight / (rrf_k + video_rank)
        entry["clip_shot_id"] = result["shot_id"]

        ranked = sorted(combined.values(), key=lambda item: item["score"], reverse=True,)[:top_k]

        if ranked:
            max_score = ranked[0]["score"]

            for item in ranked:
                item["score"] = (item["score"] / max_score) * 100

        return ranked
