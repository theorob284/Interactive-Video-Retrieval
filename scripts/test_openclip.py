
import argparse
import json
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
from collections import defaultdict

import numpy as np
import open_clip
import torch
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
THUMBNAILS = ROOT / "data" / "assignment2_data" / "thumbnails"
INDEX_PATH = ROOT / "indexes" / "clip_full.npz"
SHARD_DIR = ROOT / "indexes" / "clip_shards"

MODEL_NAME = "ViT-B-32"
PRETRAINED = "laion2b_s34b_b79k"

FOCUS_VIDEOS = [
    "00031",
    "03485",
    "01937",
    "04323",
    "00636",
]

SAMPLE_VIDEOS = 150

def shot_number(filename):
    return int(Path(filename).stem.rsplit("_", 1)[-1])


def sample_evenly(items, count):
    if count is None or len(items) <= count:
        return items

    indices = np.linspace(
        0, len(items) - 1, count, dtype=int
    )
    return [items[i] for i in indices]


def load_model(device):
    print("Loading OpenCLIP model...")

    model, _, preprocess = (
        open_clip.create_model_and_transforms(
            MODEL_NAME,
            pretrained=PRETRAINED,
        )
    )

    model = model.to(device).eval()
    tokenizer = open_clip.get_tokenizer(MODEL_NAME)

    return model, preprocess, tokenizer



def build_index(batch_size=16):
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is not available")

    device = "cuda"
    print("GPU:", torch.cuda.get_device_name(0))

    SHARD_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)

    archives = sorted(THUMBNAILS.glob("*.zip"))
    print(f"Found {len(archives)} thumbnail archives")

    if not archives:
        raise RuntimeError("No thumbnail ZIP archives found")

    model, preprocess, _ = load_model(device)

    # Index every available video.
    for video_number, archive_path in enumerate(archives, start=1):
        video_id = archive_path.stem
        shard_path = SHARD_DIR / f"{video_id}.npz"

        # Resume: skip videos already indexed.
        if shard_path.exists():
            print(f"[{video_number}/{len(archives)}] Skipping {video_id}")
            continue

        shot_ids = []
        embeddings = []
        batch_images = []
        batch_ids = []

        def process_batch():
            if not batch_images:
                return

            images = torch.stack(batch_images).to(device)

            with torch.inference_mode():
                features = model.encode_image(images)
                features = features.float()
                features /= features.norm(
                    dim=-1, keepdim=True
                )

            embeddings.append(features.cpu().numpy())
            shot_ids.extend(batch_ids)

            batch_images.clear()
            batch_ids.clear()

        with ZipFile(archive_path) as archive:
            filenames = sorted(
                [
                    name for name in archive.namelist()
                    if Path(name).stem.startswith(
                        f"shot{video_id}_"
                    )
                    and Path(name).suffix.lower()
                    in {".png", ".jpg", ".jpeg"}
                ],
                key=shot_number,
            )

            for filename in filenames:
                try:
                    with Image.open(
                        BytesIO(archive.read(filename))
                    ) as image:
                        processed = preprocess(
                            image.convert("RGB")
                        )

                    batch_images.append(processed)
                    batch_ids.append(Path(filename).stem)

                    if len(batch_images) >= batch_size:
                        process_batch()

                except Exception as error:
                    print("Skipped image:", filename, error)

            process_batch()

        if not embeddings:
            print("No valid shots:", video_id)
            continue

        vectors = np.concatenate(embeddings, axis=0)

        # Save atomically so interrupted writes
        # don't leave a misleading completed shard.
        temp_path = shard_path.with_suffix(".tmp")

        with open(temp_path, "wb") as file:
            np.savez(
                file,
                embeddings=vectors,
                shot_ids=np.asarray(shot_ids),
            )

        temp_path.replace(shard_path)

        print(
            f"[{video_number}/{len(archives)}] "
            f"Indexed {video_id}: {len(shot_ids)} shots"
        )

    # Only combine when every archive has a shard.
    missing = [
        archive.stem
        for archive in archives
        if not (SHARD_DIR / f"{archive.stem}.npz").exists()
    ]

    if missing:
        print("Incomplete indexing.")
        print("Videos still missing:", missing[:20])
        print("Rerun --build to continue.")
        return

    print("\nCombining all saved video indexes...")

    all_embeddings = []
    all_shot_ids = []

    for archive in archives:
        shard_path = SHARD_DIR / f"{archive.stem}.npz"

        with np.load(shard_path, allow_pickle=False) as data:
            all_embeddings.append(data["embeddings"])
            all_shot_ids.extend(data["shot_ids"].tolist())

    vectors = np.concatenate(all_embeddings, axis=0)

    temp_path = INDEX_PATH.with_suffix(".tmp")

    with open(temp_path, "wb") as file:
        np.savez(
            file,
            embeddings=vectors,
            shot_ids=np.asarray(all_shot_ids),
            model_name=np.asarray(MODEL_NAME),
            pretrained=np.asarray(PRETRAINED),
        )

    temp_path.replace(INDEX_PATH)

    print(f"\nIndexed {len(all_shot_ids)} total shots")
    print(f"Across {len(archives)} videos")
    print("Saved:", INDEX_PATH)

def limit_shots_per_video(results, max_per_video=3, top_k=20):
    counts = defaultdict(int)
    selected = []

    for index in results:
        shot_id = str(index["shot_id"])
        video_id = shot_id.split("_")[0].replace("shot", "")

        if counts[video_id] >= max_per_video:
            continue

        selected.append(index)
        counts[video_id] += 1

        if len(selected) >= top_k:
            break

    return selected

def search(query, top_k=20):
    device = "cuda" if torch.cuda.is_available() else "cpu"

    model, _, tokenizer = load_model(device)

    with np.load(INDEX_PATH, allow_pickle=False) as data:
        vectors = data["embeddings"]
        shot_ids = data["shot_ids"]

        if (
            str(data["model_name"]) != MODEL_NAME
            or str(data["pretrained"]) != PRETRAINED
        ):
            raise ValueError("Model does not match saved index")

    tokens = tokenizer([query]).to(device)

    with torch.inference_mode():
        query_vector = model.encode_text(tokens).float()
        query_vector /= query_vector.norm(dim=-1, keepdim=True)

    query_vector = query_vector.cpu().numpy()[0]

    similarities = vectors @ query_vector

    # Rank all indexed shots by OpenCLIP similarity.
    ranked = np.argsort(-similarities)

    # Apply diversity after visual relevance ranking.
    candidates = [
        {"shot_id": str(shot_ids[i]), "index": int(i)}
        for i in ranked
    ]

    selected = limit_shots_per_video(
        candidates,
        max_per_video=3,
        top_k=top_k,
    )

    order = [item["index"] for item in selected]

    print(f"\nQuery: {query}\n")

    for rank, index in enumerate(order, start=1):
        print(
            f"{rank:2}. {shot_ids[index]} "
            f"| similarity: {similarities[index]:.4f}"
        )

    return [
        {
            "shot_id": str(shot_ids[index]),
            "score": float(similarities[index]),
        }
        for index in order
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--query", type=str)
    parser.add_argument("--top-k", type=int, default=20)

    args = parser.parse_args()

    if args.build:
        build_index()
    elif args.query:
        search(args.query, args.top_k)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
