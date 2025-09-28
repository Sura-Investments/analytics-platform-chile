"""
Script para convertir el Excel IPC a formato Parquet optimizado
Ejecutar UNA SOLA VEZ para crear el archivo optimizado
"""

import pandas as pd
import os

def convert_excel_to_parquet():
    # Rutas de archivos - usar rutas relativas para portabilidad
    base_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(base_dir, "Bases de Datos", "IPC_Anual_Mensual.xlsx")

    # Nueva estructura organizada
    parquet_dir = os.path.join(base_dir, "data", "parquet", "chile")
    os.makedirs(parquet_dir, exist_ok=True)
    parquet_path = os.path.join(parquet_dir, "ipc_data.parquet")
    
    print("🔄 Convirtiendo Excel a Parquet...")
    
    try:
        # Leer Excel actual
        print("📁 Leyendo archivo Excel...")
        df = pd.read_excel(excel_path, sheet_name='Cuadro')
        
        print(f"📊 Registros encontrados: {len(df)}")
        print(f"📋 Columnas: {df.columns.tolist()}")
        
        # Limpiar nombres de columnas
        df.columns = df.columns.str.strip()
        
        # Optimizar tipos de datos para máximo rendimiento
        print("⚡ Optimizando tipos de datos...")
        
        # Convertir periodo a string optimizado
        df['Periodo'] = df['Periodo'].astype('string')
        
        # Optimizar números (float32 en vez de float64 = 50% menos memoria)
        df['1. Variación Mensual'] = pd.to_numeric(df['1. Variación Mensual'], errors='coerce').astype('float32')
        df['2. Variación Anual'] = pd.to_numeric(df['2. Variación Anual'], errors='coerce').astype('float32')
        
        # Limpiar datos nulos
        df = df.dropna(subset=['Periodo'])
        
        print(f"📊 Registros válidos después de limpiar: {len(df)}")
        
        # Guardar en Parquet con compresión máxima
        print("💾 Guardando archivo Parquet optimizado...")
        df.to_parquet(parquet_path, 
                     compression='snappy',  # Compresión rápida y eficiente
                     index=False)
        
        # Verificar tamaños de archivo
        excel_size = os.path.getsize(excel_path) / 1024  # KB
        parquet_size = os.path.getsize(parquet_path) / 1024  # KB
        
        print("✅ Conversión completada!")
        print(f"📦 Tamaño Excel: {excel_size:.1f} KB")
        print(f"📦 Tamaño Parquet: {parquet_size:.1f} KB")
        print(f"🚀 Reducción de tamaño: {((excel_size - parquet_size) / excel_size * 100):.1f}%")
        
        # Test de velocidad básico
        import time
        
        print("\n⏱️  Test de velocidad:")
        
        # Excel
        start = time.time()
        df_excel = pd.read_excel(excel_path, sheet_name='Cuadro')
        excel_time = (time.time() - start) * 1000
        
        # Parquet
        start = time.time()
        df_parquet = pd.read_parquet(parquet_path)
        parquet_time = (time.time() - start) * 1000
        
        print(f"📈 Excel: {excel_time:.1f}ms")
        print(f"⚡ Parquet: {parquet_time:.1f}ms")
        print(f"🏆 Mejora de velocidad: {(excel_time / parquet_time):.1f}x más rápido")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Optimizador de archivos IPC")
    print("=" * 50)
    
    # Verificar que pandas esté instalado
    try:
        import pyarrow
        print("✅ PyArrow encontrado")
    except ImportError:
        print("⚠️  Instalando PyArrow...")
        os.system("pip install pyarrow")
    
    success = convert_excel_to_parquet()
    
    if success:
        print("\n🎉 ¡Optimización completada!")
        print("📝 Próximo paso: Actualizar data_loader.py para usar Parquet")
    else:
        print("\n❌ Error en la conversión")
    
    input("\nPresiona Enter para continuar...")