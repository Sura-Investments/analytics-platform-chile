from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from django.views.generic import TemplateView
from django.core.cache import cache
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator
from .models import IPCData
import json
import pandas as pd
import io
from datetime import datetime

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
                'type': 'bar',
                'data': mensual_data,
                'backgroundColor': 'rgba(75, 192, 192, 0.7)',
                'borderColor': 'rgb(75, 192, 192)',
                'borderWidth': 1,
                'yAxisID': 'y1'  # Eje derecho
            },
            {
                'label': 'Variación Anual (%)',
                'type': 'line',
                'data': anual_data,
                'borderColor': 'rgb(255, 99, 132)',
                'backgroundColor': 'rgba(255, 99, 132, 0.1)',
                'tension': 0.4,
                'fill': False,
                'borderWidth': 3,
                'yAxisID': 'y'  # Eje izquierdo
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

def api_ipc_export_excel(request):
    """
    Exportar todos los datos IPC a Excel
    """
    try:
        # Obtener todos los datos ordenados por fecha
        queryset = IPCData.objects.all().order_by('-fecha')

        if not queryset.exists():
            return HttpResponse("No hay datos para exportar", status=404)

        # Convertir a DataFrame
        data = []
        for record in queryset:
            data.append({
                'Período': record.periodo,
                'Fecha': record.fecha.strftime('%d/%m/%Y'),
                'Variación Mensual (%)': float(record.variacion_mensual),
                'Variación Anual (%)': float(record.variacion_anual),
                'Fecha Actualización': record.updated_at.strftime('%d/%m/%Y %H:%M')
            })

        df = pd.DataFrame(data)

        # Crear buffer para el archivo Excel
        output = io.BytesIO()

        # Crear archivo Excel con múltiples hojas (12 meses por hoja)
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            rows_per_sheet = 12
            total_rows = len(df)
            sheet_number = 1

            for start_idx in range(0, total_rows, rows_per_sheet):
                end_idx = min(start_idx + rows_per_sheet, total_rows)
                sheet_data = df.iloc[start_idx:end_idx]

                # Nombre de la hoja
                sheet_name = f'Página {sheet_number}'

                # Escribir datos en la hoja
                sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)

                # Obtener worksheet para formatear
                worksheet = writer.sheets[sheet_name]

                # Ajustar ancho de columnas
                for column in worksheet.columns:
                    max_length = 0
                    column = [cell for cell in column]
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column[0].column_letter].width = adjusted_width

                sheet_number += 1

            # Hoja resumen
            summary_data = {
                'Estadística': [
                    'Total de Registros',
                    'Período Más Antiguo',
                    'Período Más Reciente',
                    'Promedio Variación Mensual',
                    'Promedio Variación Anual',
                    'Máxima Variación Mensual',
                    'Mínima Variación Mensual',
                    'Máxima Variación Anual',
                    'Mínima Variación Anual'
                ],
                'Valor': [
                    len(df),
                    df.iloc[-1]['Período'],
                    df.iloc[0]['Período'],
                    f"{df['Variación Mensual (%)'].mean():.2f}%",
                    f"{df['Variación Anual (%)'].mean():.2f}%",
                    f"{df['Variación Mensual (%)'].max():.2f}%",
                    f"{df['Variación Mensual (%)'].min():.2f}%",
                    f"{df['Variación Anual (%)'].max():.2f}%",
                    f"{df['Variación Anual (%)'].min():.2f}%"
                ]
            }

            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Resumen', index=False)

        output.seek(0)

        # Configurar respuesta HTTP
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

        # Nombre del archivo con fecha actual
        filename = f'datos_ipc_chile_{datetime.now().strftime("%Y%m%d")}.xlsx'
        response['Content-Disposition'] = f'attachment; filename="{filename}"'

        return response

    except Exception as e:
        return HttpResponse(f"Error generando Excel: {str(e)}", status=500)