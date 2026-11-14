from django.urls import path
from . import views

urlpatterns = [
    path("search/text/", views.text_search, name="text_search"),
    path("search/visual/", views.visual_search_view, name="visual_search"),
    path("csrf/", views.csrf_token, name="csrf_token"),
    path("videos/<str:video_id>/shots/", views.video_shots, name="video_shots"),
    path("shots/<str:shot_id>/thumbnail/", views.shot_thumbnail, name="shot_thumbnail"),
    path("videos/<str:video_id>/file/", views.video_file, name="video_file"),
    path("dres/submit/", views.submit_shot, name="submit_shot"),
]