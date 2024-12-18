from django.urls import path
from data_transfer import views
# import data_transfer

urlpatterns = [
    path('', views.index, name='index'),
    path('get_collections/', views.get_collections, name='get_collections'),
    path('get_schema/', views.get_schema, name='get_schema'),
    path('confirm_transfer/', views.confirm_transfer, name='confirm_transfer'),  # Add this line
]
