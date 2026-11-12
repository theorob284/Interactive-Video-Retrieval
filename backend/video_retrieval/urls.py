from django.urls import path
from . import views

urlpatterns = [
    path("search/text/", views.text_search, name="text_search"),
    path("search/visual/", views.visual_search_view, name="visual_search"),
    path("csrf/", views.csrf_token, name="csrf_token"),
]