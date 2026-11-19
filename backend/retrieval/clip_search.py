
from functools import lru_cache
from pathlib import Path
from collections import defaultdict

import numpy as np
import open_clip
import torch


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INDEX_PATH = PROJECT_ROOT / "indexes" / "clip_full.npz"

MODEL_NAME = "ViT-B-32"
PRETRAINED = "laion2b_s34b_b79k"


@lru_cache(maxsize=1)
def load_index():
    """Load saved shot embeddings once per Django process."""

    with np.load(INDEX_PATH, allow_pickle=False) as data:
        if (
            str(data["model_name"]) != MODEL_NAME
            or str(data["pretrained"]) != PRETRAINED
        ):
            raise ValueError("OpenCLIP model does not match index")

        embeddings = np.ascontiguousarray(
            data["embeddings"], dtype=np.float32
        )
        shot_ids = data["shot_ids"].astype(str)

    if len(embeddings) != len(shot_ids):
        raise ValueError("OpenCLIP index has mismatched arrays")

    return embeddings, shot_ids


@lru_cache(maxsize=1)
def load_model():
    """Load pretrained OpenCLIP once per Django process."""

    # CPU supports the evaluation laptop without CUDA.
    device = "cpu"

    model, _, _ = open_clip.create_model_and_transforms(
        MODEL_NAME,
        pretrained=PRETRAINED,
    )

    model = model.to(device).eval()
    tokenizer = open_clip.get_tokenizer(MODEL_NAME)

    return model, tokenizer


def search_shots(query, top_k=60, max_per_video=3):
    """
    Rank official shots by text-image similarity.

    max_per_video=None disables diversity filtering.
    """
    if not query.strip():
        return []

    embeddings, shot_ids = load_index()
    model, tokenizer = load_model()

    tokens = tokenizer([query]).to("cpu")

    with torch.inference_mode():
        query_vector = model.encode_text(tokens).float()
        query_vector /= query_vector.norm(
            dim=-1, keepdim=True
        )

    query_vector = query_vector.cpu().numpy()[0]

    similarities = embeddings @ query_vector
    ranked = np.argsort(-similarities)

    counts = defaultdict(int)
    results = []

    for index in ranked:
        shot_id = str(shot_ids[index])
        video_id = shot_id[4:].split("_")[0]

        if (
            max_per_video is not None
            and counts[video_id] >= max_per_video
        ):
            continue

        results.append({
            "shot_id": shot_id,
            "video_id": video_id,
            "score": float(similarities[index]),
        })

        counts[video_id] += 1

        if len(results) >= top_k:
            break

    return results
