from django.contrib import admin
from django.urls import path
from. import views

urlpatterns = [
    path('',views.home,name="home"),
    path('get_list',views.get_tablenames,name="table_names"),
    path('tables',views.create_tables,name="create_tables"),
    path('queries',views.query_execute,name="queries"),
    path('table_names',views.get_tablenames,name="table_names"),
    path('file_upload',views.file_read,name="file_upload"),
    path('insert_data',views.insert_data,name="insert_data"),
    path('delete_data',views.delete_table,name="delete_data"),
    path('get_start',views.get_function_status,name="get_start"),
    path('download_csv',views.download_file,name="download_csv"),
    path('insert_only',views.insert_only,name="insert_only"),
    
]