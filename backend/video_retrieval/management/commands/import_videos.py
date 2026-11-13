
import json
from decimal import Decimal
from pathlib import Path
from zipfile import ZipFile

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from video_retrieval.models import Video, Shot

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DATA_DIR = ROOT / "data" / "assignment2_data"

def to_milliseconds(seconds):
    return round(Decimal(str(seconds)) * 1000)

def read_shots(archive, member_name, video_id):
    shots = []

    with archive.open(member_name) as file:
        lines = (line.decode("utf-8-sig").strip() for line in file)

        lines = (line for line in lines if line)

        headers = next(lines).split()

        required = {"startframe", "starttime", "endframe", "endtime"}

        if not required.issubset(headers):
            raise ValueError(f"Unexpected TSV columns for video {video_id}: {headers}")

        for shot_number, line in enumerate(lines, start=1):
            values = line.split()

            if len(values) != len(headers):
                raise ValueError(f"Invalid shot row {shot_number} for video {video_id}")

            row = dict(zip(headers, values))

            shot_id = f"shot{video_id}_{shot_number}"

            shots.append(
                Shot(
                    shot_id=shot_id,
                    shot_number=shot_number,
                    start_ms=to_milliseconds(row["starttime"]),
                    end_ms=to_milliseconds(row["endtime"]),
                    thumbnail_path=(
                        f"thumbnails/{video_id}.zip::{shot_id}.png"
                    ),
                )
            )

    return shots

class Command(BaseCommand):
    help = "Import V3C video metadata and official shot boundaries"

    def add_arguments(self, parser):
        parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Directory containing videos_metadata.json and msb.zip",)

    def handle(self, *args, **options):
        data_dir = options["data_dir"]

        metadata_path = data_dir / "videos_metadata.json"
        msb_path = data_dir / "msb.zip"

        if not metadata_path.is_file():
            raise CommandError(f"Missing metadata: {metadata_path}")

        if not msb_path.is_file():
            raise CommandError(f"Missing shot boundaries: {msb_path}")

        with metadata_path.open(encoding="utf-8") as file:
            records = json.load(file)

        video_count = 0
        total_shots = 0

        with ZipFile(msb_path) as archive:
            # Locate TSV files even if the ZIP contains subdirectories.
            members = {}

            for name in archive.namelist():
                if name.lower().endswith(".tsv"):
                    filename = Path(name).name

                    if filename in members:
                        raise CommandError(f"Duplicate TSV filename: {filename}")

                    members[filename] = name

            for record in records:
                video_id = str(record["v3cId"])
                filename = f"{video_id}.tsv"

                if filename not in members:
                    raise CommandError(f"Missing shot boundaries for {video_id}")

                try:
                    shots = read_shots(archive, members[filename], video_id)

                    expected_count = int(record["shot_count"])

                    if len(shots) != expected_count:
                        raise ValueError(
                            f"Video {video_id}: expected "
                            f"{expected_count} shots, found {len(shots)}"
                        )

                    tags = record.get("tags") or []

                    if isinstance(tags, str):
                        tags = [tag.strip() for tag in tags.split(";") if tag.strip()]

                    # Save video and its shots together.
                    with transaction.atomic():
                        video, _ = Video.objects.update_or_create(
                            video_id=video_id,
                            defaults={
                                "vimeo_id": str(
                                    record.get("vimeoId") or ""
                                ),
                                "title": record.get("title") or "",
                                "description": (
                                    record.get("description") or ""
                                ),
                                "tags": tags,
                                "category": (
                                    record.get("primary_category") or ""
                                ),
                                "duration_ms": to_milliseconds(
                                    record["duration"]
                                ),
                                "shot_count": expected_count,
                                "video_path": (
                                    record.get("video_480p") or ""
                                ),
                                "thumbnails_path": (
                                    record.get("thumbnails_zip") or ""
                                ),
                                "shot_boundaries_path": (
                                    record.get("shot_boundaries") or ""
                                ),
                            },
                        )

                        # Recreate shots so the command can be rerun.
                        Shot.objects.filter(video=video).delete()

                        for shot in shots:
                            shot.video = video

                        Shot.objects.bulk_create(shots, batch_size=2000,)

                    video_count += 1
                    total_shots += len(shots)

                except (ValueError, KeyError) as error:
                    raise CommandError(f"Failed importing video {video_id}: {error}") from error

                if video_count % 100 == 0:
                    self.stdout.write(f"Imported {video_count} videos...")

        self.stdout.write(
            self.style.SUCCESS(
                f"Import complete: {video_count} videos, "
                f"{total_shots} shots."
            )
        )
