from django.shortcuts import render
from django.http import JsonResponse
from django.views.generic import TemplateView
from django.core.cache import cache
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator
from .models import IPCData
import json
import pandas as pd

class DashboardView(TemplateView):
    """
    Vista principal del dashboard público
    """
    template_name = 'dashboard/index.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Obtener estadísticas básicas
        total_records = IPCData.objects.count()
        latest_record = IPCData.objects.first() if total_records > 0 else None
        oldest_record = IPCData.objects.last() if total_records > 0 else None
        
        context.update({
            'page_title': 'Analytics Platform Chile',
            'total_records': total_records,
            'latest_record': latest_record,
            'oldest_record': oldest_record,
        })
        
        return context

class IPCDetailView(TemplateView):
    """
    Vista específica para datos IPC
    """
    template_name = 'dashboard/ipc_detail.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Obtener datos IPC
        ipc_data = IPCData.objects.all().order_by('fecha')
        
        # Estadísticas
        total_records = ipc_data.count()
        latest = ipc_data.last() if total_records > 0 else None
        
        # Datos para mostrar en tabla (últimos 24 meses)
        recent_data_queryset = IPCData.objects.all().order_by('-fecha')[:24]
        # Convertir a lista y luego invertir
        recent_data_list = list(recent_data_queryset)
        recent_data_list.reverse()
        
        context.update({
            'page_title': 'IPC - Índice de Precios al Consumidor',
            'total_records': total_records,
            'latest_record': latest,
            'recent_data': recent_data_list,
        })
        
        return context

@cache_page(300)  # Cache por 5 minutos
def api_ipc_chart_data(request):
    """
    API para datos del gráfico IPC
    """
    # Obtener parámetros
    limit = request.GET.get('limit', '24')

    # Crear clave de caché única
    cache_key = f'ipc_chart_data_{limit}'

    # Intentar obtener datos del caché
    cached_data = cache.get(cache_key)
    if cached_data:
        return JsonResponse(cached_data)
    
    if limit == 'all':
        queryset = IPCData.objects.all().order_by('fecha')
    else:
        try:
            limit_num = int(limit)
            queryset = IPCData.objects.all().order_by('-fecha')[:limit_num]
            # Convertir a lista y ordenar por fecha ascendente
            queryset = sorted(list(queryset), key=lambda x: x.fecha)
        except ValueError:
            limit_num = 24
            queryset = IPCData.objects.all().order_by('-fecha')[:limit_num]
            queryset = sorted(list(queryset), key=lambda x: x.fecha)
    
    # Formatear para Chart.js
    labels = []
    mensual_data = []
    anual_data = []
    
    for item in queryset:
        labels.append(item.periodo)
        mensual_data.append(float(item.variacion_mensual))
        anual_data.append(float(item.variacion_anual))
    
    chart_data = {
        'labels': labels,
        'datasets': [
            {
                'label': 'Variación Mensual (%)',
                'data': mensual_data,
                'borderColor': 'rgb(75, 192, 192)',
                'backgroundColor': 'rgba(75, 192, 192, 0.1)',
                'tension': 0.4,
                'fill': False
            },
            {
                'label': 'Variación Anual (%)',
                'data': anual_data,
                'borderColor': 'rgb(255, 99, 132)',
                'backgroundColor': 'rgba(255, 99, 132, 0.1)',
                'tension': 0.4,
                'fill': False
            }
        ]
    }

    # Guardar en caché por 5 minutos
    cache.set(cache_key, chart_data, 300)

    return JsonResponse(chart_data)

@cache_page(600)  # Cache por 10 minutos (datos más estables)
def api_ipc_summary(request):
    """
    API para resumen de datos IPC
    """
    # Verificar caché primero
    cache_key = 'ipc_summary_data'
    cached_summary = cache.get(cache_key)
    if cached_summary:
        return JsonResponse(cached_summary)

    queryset = IPCData.objects.all()

    if not queryset.exists():
        return JsonResponse({'error': 'No hay datos disponibles'})
    
    # Calcular estadísticas
    df = pd.DataFrame(list(queryset.values('variacion_mensual', 'variacion_anual')))
    
    summary = {
        'total_records': queryset.count(),
        'date_range': {
            'start': queryset.last().periodo,
            'end': queryset.first().periodo
        },
        'latest': {
            'periodo': queryset.first().periodo,
            'mensual': float(queryset.first().variacion_mensual),
            'anual': float(queryset.first().variacion_anual)
        },
        'statistics': {
            'mensual': {
                'promedio': round(df['variacion_mensual'].mean(), 2),
                'maximo': float(df['variacion_mensual'].max()),
                'minimo': float(df['variacion_mensual'].min())
            },
            'anual': {
                'promedio': round(df['variacion_anual'].mean(), 2),
                'maximo': float(df['variacion_anual'].max()),
                'minimo': float(df['variacion_anual'].min())
            }
        }
    }

    # Guardar en caché por 10 minutos
    cache.set(cache_key, summary, 600)

    return JsonResponse(summary)