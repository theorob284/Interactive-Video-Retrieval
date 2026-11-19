
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO

from PIL import Image, ImageDraw

from test_openclip import search


ROOT = Path(__file__).resolve().parents[1]
THUMBNAILS = ROOT / "data" / "assignment2_data" / "thumbnails"
OUTPUT = ROOT / "indexes" / "clip_results.jpg"

QUERY = "Drummer with headphones"
TOP_K = 20


def main():
    results = search(QUERY, top_k=TOP_K)

    tile_w, tile_h = 240, 170
    columns = 4
    rows = (len(results) + columns - 1) // columns

    montage = Image.new(
        "RGB",
        (columns * tile_w, rows * tile_h),
        "white",
    )
    draw = ImageDraw.Draw(montage)

    for position, result in enumerate(results):
        shot_id = result["shot_id"]
        score = result["score"]

        video_id = shot_id.split("_")[0].replace("shot", "")
        zip_path = THUMBNAILS / f"{video_id}.zip"

        if not zip_path.exists():
            print("Missing archive:", zip_path)
            continue

        with ZipFile(zip_path) as archive:
            filename = next(
                (
                    name for name in archive.namelist()
                    if Path(name).stem == shot_id
                ),
                None,
            )

            if filename is None:
                print("Missing shot:", shot_id)
                continue

            with Image.open(BytesIO(archive.read(filename))) as source:
                image = source.convert("RGB")
                image.thumbnail((tile_w - 12, tile_h - 40))

        x = (position % columns) * tile_w
        y = (position // columns) * tile_h

        montage.paste(image, (x + 6, y + 6))

        draw.text(
            (x + 6, y + tile_h - 30),
            f"{position + 1}. {shot_id}",
            fill="black",
        )

        draw.text(
            (x + 6, y + tile_h - 16),
            f"Similarity: {score:.3f}",
            fill="black",
        )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    montage.save(OUTPUT, quality=90)

    print("Saved:", OUTPUT)


if __name__ == "__main__":
    main()
