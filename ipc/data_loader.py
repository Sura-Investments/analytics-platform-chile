import pandas as pd
from django.conf import settings
from .models import IPCData
import logging

logger = logging.getLogger(__name__)

class IPCDataLoader:
    """
    Clase para cargar datos desde Parquet a la base de datos
    """
    
    def __init__(self):
        self.parquet_path = settings.PARQUET_FILE_PATH
    
    def load_data(self):
        """
        Carga datos desde Parquet a la base de datos
        """
        try:
            print(f"📁 Leyendo archivo: {self.parquet_path}")
            
            # Leer Parquet
            df = pd.read_parquet(self.parquet_path)
            
            print(f"📊 Filas encontradas: {len(df)}")
            print(f"📋 Columnas: {df.columns.tolist()}")
            
            # Limpiar nombres de columnas
            df.columns = df.columns.str.strip()
            
            # Verificar columnas esperadas
            expected_cols = ['Periodo', '1. Variación Mensual', '2. Variación Anual']
            missing_cols = [col for col in expected_cols if col not in df.columns]
            
            if missing_cols:
                print(f"❌ Error: No se encontraron estas columnas: {missing_cols}")
                print(f"📋 Columnas disponibles: {df.columns.tolist()}")
                return None
            
            # Limpiar datos
            df = df.dropna(subset=['Periodo'])
            print(f"📊 Filas válidas después de limpiar: {len(df)}")
            
            # Mostrar primeras filas para verificar
            print("\n📋 Primeras 3 filas:")
            print(df.head(3).to_string())
            
            # Procesar cada fila
            created_count = 0
            updated_count = 0
            error_count = 0
            
            for index, row in df.iterrows():
                try:
                    periodo_raw = row['Periodo']
                    
                    # Convertir comas a puntos y manejar valores
                    variacion_mensual = str(row['1. Variación Mensual']).replace(',', '.')
                    variacion_anual = str(row['2. Variación Anual']).replace(',', '.')
                    
                    variacion_mensual = float(variacion_mensual)
                    variacion_anual = float(variacion_anual)
                    
                    # Usar el nuevo método para parsear periodo y fecha
                    periodo, fecha = IPCData.parse_periodo_and_fecha(periodo_raw)
                    
                    if fecha is None or periodo is None:
                        print(f"⚠️  No se pudo parsear el periodo: {periodo_raw}")
                        error_count += 1
                        continue
                    
                    # Crear o actualizar registro
                    obj, created = IPCData.objects.update_or_create(
                        periodo=periodo,
                        defaults={
                            'fecha': fecha,
                            'variacion_mensual': variacion_mensual,
                            'variacion_anual': variacion_anual,
                        }
                    )
                    
                    if created:
                        created_count += 1
                        if created_count <= 5:  # Mostrar solo los primeros 5
                            print(f"✅ Creado: {periodo} -> {variacion_mensual}% / {variacion_anual}%")
                    else:
                        updated_count += 1
                        if updated_count <= 2:  # Mostrar solo los primeros 2 updates
                            print(f"🔄 Actualizado: {periodo} -> {variacion_mensual}% / {variacion_anual}%")
                        
                except Exception as e:
                    print(f"❌ Error procesando fila {index}: {e}")
                    error_count += 1
                    continue
            
            print(f"\n🎉 ¡Carga completada!")
            print(f"   ✅ Creados: {created_count}")
            print(f"   🔄 Actualizados: {updated_count}")
            print(f"   ❌ Errores: {error_count}")
            
            return {'created': created_count, 'updated': updated_count, 'errors': error_count}
            
        except FileNotFoundError:
            print(f"❌ Error: No se encontró el archivo en: {self.parquet_path}")
            print("💡 Verifica que la ruta sea correcta y el archivo exista.")
            return None
            
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            return None
    
    def get_data_summary(self):
        """
        Obtiene resumen de los datos en la base de datos
        """
        total_records = IPCData.objects.count()
        if total_records == 0:
            return None
            
        latest = IPCData.objects.first()
        oldest = IPCData.objects.last()
        
        return {
            'total_records': total_records,
            'date_range': f"{oldest.periodo} - {latest.periodo}",
            'latest_data': {
                'periodo': latest.periodo,
                'variacion_mensual': latest.variacion_mensual,
                'variacion_anual': latest.variacion_anual
            }
        }