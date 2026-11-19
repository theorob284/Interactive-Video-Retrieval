
class HybridSearch:
    """Combine metadata and transcript BM25 rankings."""

    def __init__(
        self,
        metadata_engine,
        transcript_engine,
        metadata_weight=1.0,
        transcript_weight=1.0,
        rrf_k=60,
    ):
        self.metadata_engine = metadata_engine
        self.transcript_engine = transcript_engine

        self.metadata_weight = metadata_weight
        self.transcript_weight = transcript_weight
        self.rrf_k = rrf_k

    
    def search(self, query, top_k=50):
        if top_k <= 0:
            return []

        metadata_results = self.metadata_engine.search(
            query, top_k=100
        )

        transcript_results = self.transcript_engine.search(
            query, top_k=100
        )

        metadata_scores = {}
        transcript_scores = {}

        for rank, result in enumerate(metadata_results, start=1):
            metadata_scores[result["video_id"]] = (
                self.metadata_weight / (self.rrf_k + rank)
            )

        for rank, result in enumerate(transcript_results, start=1):
            transcript_scores[result["video_id"]] = (
                self.transcript_weight / (self.rrf_k + rank)
            )

        combined = {}

        for video_id in metadata_scores.keys() | transcript_scores.keys():
            metadata_score = metadata_scores.get(video_id, 0.0)
            transcript_score = transcript_scores.get(video_id, 0.0)

            combined[video_id] = (
                max(metadata_score, transcript_score)
                + 0.1 * min(metadata_score, transcript_score)
            )

        ranked = sorted(
            combined.items(),
            key=lambda item: (-item[1], item[0]),
        )

        return [
            {"video_id": video_id, "score": float(score)}
            for video_id, score in ranked[:top_k]
        ]

