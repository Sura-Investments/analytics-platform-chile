from django.db import models
from datetime import datetime
import pandas as pd

class IPCData(models.Model):
    periodo = models.CharField(max_length=20, unique=True, verbose_name="Período")
    fecha = models.DateField(verbose_name="Fecha")
    variacion_mensual = models.DecimalField(
        max_digits=6, 
        decimal_places=2, 
        verbose_name="Variación Mensual (%)"
    )
    variacion_anual = models.DecimalField(
        max_digits=6, 
        decimal_places=2, 
        verbose_name="Variación Anual (%)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Dato IPC"
        verbose_name_plural = "Datos IPC"
        ordering = ['-fecha']
    
    def __str__(self):
        return f"{self.periodo} - Mensual: {self.variacion_mensual}% - Anual: {self.variacion_anual}%"
    
    @classmethod
    def parse_periodo_and_fecha(cls, periodo_value):
        """
        Maneja diferentes formatos de período:
        - 'ene.2011' -> convierte a fecha y periodo
        - '2011-01-01' (fecha) -> convierte a periodo formato 'ene.2011'
        """
        meses_nombres = {
            1: 'ene', 2: 'feb', 3: 'mar', 4: 'abr', 5: 'may', 6: 'jun',
            7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dic'
        }
        
        meses_numeros = {v: k for k, v in meses_nombres.items()}
        
        try:
            # Si es un string como 'ene.2011'
            if isinstance(periodo_value, str) and '.' in periodo_value:
                mes_str, año_str = periodo_value.split('.')
                mes = meses_numeros.get(mes_str.lower())
                año = int(año_str)
                
                if mes:
                    fecha = datetime(año, mes, 1).date()
                    return periodo_value, fecha
            
            # Si es una fecha (pandas Timestamp o datetime)
            elif hasattr(periodo_value, 'year') and hasattr(periodo_value, 'month'):
                # Convertir pandas Timestamp a datetime si es necesario
                if hasattr(periodo_value, 'to_pydatetime'):
                    fecha = periodo_value.to_pydatetime().date()
                else:
                    fecha = periodo_value.date() if hasattr(periodo_value, 'date') else periodo_value
                
                # Crear periodo en formato 'ene.2011'
                mes_nombre = meses_nombres.get(fecha.month)
                if mes_nombre:
                    periodo = f"{mes_nombre}.{fecha.year}"
                    return periodo, fecha
            
            # Si es un string de fecha como '2011-01-01'
            elif isinstance(periodo_value, str):
                try:
                    fecha = pd.to_datetime(periodo_value).date()
                    mes_nombre = meses_nombres.get(fecha.month)
                    if mes_nombre:
                        periodo = f"{mes_nombre}.{fecha.year}"
                        return periodo, fecha
                except:
                    pass
                    
        except Exception as e:
            print(f"Error parseando periodo: {periodo_value} -> {e}")
        
        return None, None