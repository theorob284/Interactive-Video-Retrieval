from django.db import models

# Create your models here.

class Video(models.Model):

    video_id = models.CharField(max_length=True, primary_key=True)
    title = models.CharField(blank=True)
    description = models.CharField(blank=True)
    tags = models.JSONField(default=list)
    duration_ms = models.BigIntegerField()
    video_path = models.TextField()


class Shot(models.Model):

    shot_id = models.CharField(max_length=150, primary_key=True)
    video = models.ForeignKey(Video, on_delete=models.CASCADE, related_name="shots")
    start_ms = models.BigIntegerField()
    end_ms = models.BigIntegerField()
    thumbnail_path = models.TextField()

    class Meta:
        indexes = [
            models.Index(
                fields = {"video", "start_ms"}
            )
        ]

class VideoText(models.Model):

    text = models.CharField(blank=True, null=True)