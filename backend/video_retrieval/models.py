from django.db import models

class Video(models.Model):
    video_id = models.CharField(max_length=5, primary_key=True)
    vimeo_id = models.CharField(max_length=30, blank=True)

    title = models.TextField(blank=True)
    description = models.TextField(blank=True)
    tags = models.JSONField(default=list)

    category = models.CharField(max_length=150, blank=True)

    duration_ms = models.BigIntegerField()
    shot_count = models.PositiveIntegerField()

    video_path = models.TextField()
    thumbnails_path = models.TextField()
    shot_boundaries_path = models.TextField()

    def __str__(self):
        return self.title or self.video_id


class Shot(models.Model):
    shot_id = models.CharField(max_length=150, primary_key=True)

    video = models.ForeignKey(Video,on_delete=models.CASCADE,related_name="shots")

    shot_number = models.PositiveIntegerField()

    start_ms = models.BigIntegerField()
    end_ms = models.BigIntegerField()

    thumbnail_path = models.TextField()

    class Meta:
        ordering = ["video", "shot_number"]

        constraints = [
            models.UniqueConstraint(
                fields=["video", "shot_number"],
                name="unique_video_shot"
            )
        ]

        indexes = [models.Index(fields=["video", "start_ms"])]


class VideoText(models.Model):

    text = models.TextField(blank=True, null=True)
    language = models.CharField(max_length=50,blank=True,null=True)
    video = models.OneToOneField(Video,on_delete=models.CASCADE,related_name="transcript")