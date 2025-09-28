# ARCHIVO 1: ipc/urls.py
from django.urls import path
from . import views

app_name = 'ipc'

urlpatterns = [
    # Vistas principales
    path('', views.DashboardView.as_view(), name='dashboard'),
    path('chile/economia/ipc/', views.IPCDetailView.as_view(), name='ipc_detail'),
    
    # APIs para gráficos
    path('api/ipc/chart/', views.api_ipc_chart_data, name='api_ipc_chart'),
    path('api/ipc/summary/', views.api_ipc_summary, name='api_ipc_summary'),
]
