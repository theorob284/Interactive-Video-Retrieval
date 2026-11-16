
import random
from pathlib import Path

import cv2

ROOT = Path(__file__).resolve().parents[1]

VIDEO_DIR = ROOT / "data" / "assignment2_data" / "videos_480p"
OUTPUT_DIR = ROOT / "frontend" / "public" / "homepage"

IMAGES_PER_CARD = 5
CARD_NAMES = ["text", "visual", "avs"]


def extract_frame(video_path, output_path):
    video = cv2.VideoCapture(str(video_path))

    try:
        if not video.isOpened():
            return False

        total_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))

        if total_frames <= 0:
            return False

        # Choose a frame somewhere in the middle of the video.
        frame_number = random.randint(int(total_frames * 0.2), int(total_frames * 0.8),)

        video.set(cv2.CAP_PROP_POS_FRAMES, frame_number)

        success, frame = video.read()

        if not success:
            return False

        return cv2.imwrite(str(output_path), frame, [cv2.IMWRITE_JPEG_QUALITY, 92],)

    finally:
        video.release()


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    videos = list(VIDEO_DIR.glob("*.mp4"))
    random.shuffle(videos)

    image_number = 0

    for video_path in videos:
        if image_number >= IMAGES_PER_CARD * len(CARD_NAMES):
            break

        card = CARD_NAMES[image_number // IMAGES_PER_CARD]
        position = image_number % IMAGES_PER_CARD + 1

        output_path = OUTPUT_DIR / f"{card}_{position}.jpg"

        if extract_frame(video_path, output_path):
            print(f"Saved {output_path.name} from {video_path.name}")
            image_number += 1

    print(f"\nGenerated {image_number} homepage images.")

if __name__ == "__main__":
    main()
