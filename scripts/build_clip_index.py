from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

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

def shot_number(filename):
    return int(Path(filename).stem.rsplit("_", 1)[-1])

def load_model(device):
    print("Loading OpenCLIP model...")

    model, _, preprocess = open_clip.create_model_and_transforms(MODEL_NAME, pretrained=PRETRAINED,)

    return model.to(device).eval(), preprocess

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

    model, preprocess = load_model(device)

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
                features /= features.norm(dim=-1, keepdim=True)

            embeddings.append(features.cpu().numpy())
            shot_ids.extend(batch_ids)

            batch_images.clear()
            batch_ids.clear()

        with ZipFile(archive_path) as archive:
            filenames = sorted(
                [
                    name for name in archive.namelist() if Path(name).stem.startswith(f"shot{video_id}_") and Path(name).suffix.lower() in {".png", ".jpg", ".jpeg"}
                ], key=shot_number,
            )

            for filename in filenames:
                try:
                    with Image.open(BytesIO(archive.read(filename))) as image:
                        processed = preprocess(image.convert("RGB"))

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
        temp_path = shard_path.with_suffix(".tmp")

        with open(temp_path, "wb") as file:
            np.savez(file, embeddings=vectors, shot_ids=np.asarray(shot_ids))

        temp_path.replace(shard_path)

        print(
            f"[{video_number}/{len(archives)}] "
            f"Indexed {video_id}: {len(shot_ids)} shots"
        )

    # Only combine when every archive has a shard.
    missing = [archive.stem for archive in archives if not (SHARD_DIR / f"{archive.stem}.npz").exists()]

    if missing:
        print("Incomplete indexing.")
        print("Videos still missing:", missing[:20])
        print("Rerun the script to continue.")
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
        np.savez(file, embeddings=vectors, shot_ids=np.asarray(all_shot_ids), model_name=np.asarray(MODEL_NAME), pretrained=np.asarray(PRETRAINED),)

    temp_path.replace(INDEX_PATH)

    print(f"\nIndexed {len(all_shot_ids)} total shots")
    print(f"Across {len(archives)} videos")
    print("Saved:", INDEX_PATH)

if __name__ == "__main__":
    build_index()
