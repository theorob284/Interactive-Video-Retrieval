from django.urls import path
from . import views

urlpatterns = [
    path("search/text/", views.text_search, name="text_search"),
]