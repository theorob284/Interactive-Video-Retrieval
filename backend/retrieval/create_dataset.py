# WILL DELETE DURING FINAL SUBMISSION THEO XOXOXOX

from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance

ROOT = Path(__file__).resolve().parents[1]

THUMBNAIL_DIR = ROOT / "data" / "synthetic_thumbnails"
QUERY_DIR = ROOT / "data" / "synthetic_queries"

THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
QUERY_DIR.mkdir(parents=True, exist_ok=True)


def create_scene(name, draw_scene):
    image = Image.new("RGB", (320, 240), "white")
    draw = ImageDraw.Draw(image)

    draw_scene(draw)

    thumbnail_path = THUMBNAIL_DIR / f"{name}.png"
    image.save(thumbnail_path)

    # Create a slightly altered query image
    query = ImageEnhance.Brightness(image).enhance(1.08)
    query_path = QUERY_DIR / f"{name}_query.png"
    query.save(query_path)

    print(f"Created {thumbnail_path.name}")


def city_scene(draw):
    draw.rectangle((30, 90, 90, 230), fill="gray")
    draw.rectangle((110, 40, 180, 230), fill="blue")
    draw.rectangle((200, 70, 280, 230), fill="brown")
    draw.rectangle((125, 65, 140, 85), fill="yellow")


def beach_scene(draw):
    draw.rectangle((0, 120, 320, 240), fill="orange")
    draw.rectangle((0, 150, 320, 240), fill="blue")
    draw.ellipse((220, 25, 275, 80), fill="yellow")


def forest_scene(draw):
    draw.rectangle((0, 180, 320, 240), fill="brown")
    for x in (40, 110, 190, 260):
        draw.polygon(
            [(x, 35), (x - 35, 150), (x + 35, 150)],
            fill="green",
        )


def football_scene(draw):
    draw.rectangle((0, 0, 320, 240), fill="green")
    draw.rectangle((20, 20, 300, 220), outline="white", width=4)
    draw.ellipse((135, 90, 185, 140), fill="white")
    draw.rectangle((145, 100, 175, 130), fill="black")


def create_dataset():
    create_scene("shot00031_1", city_scene)
    create_scene("shot00031_2", beach_scene)
    create_scene("shot00032_1", forest_scene)
    create_scene("shot00032_2", football_scene)

    print("\nSynthetic dataset created successfully.")


if __name__ == "__main__":
    create_dataset()