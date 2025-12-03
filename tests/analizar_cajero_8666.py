"""
Script para analizar el procesamiento del cajero 8666
"""
import pandas as pd
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

def analizar_cajero_8666():
    """Analiza en detalle el cajero 8666."""
    codigo_cajero = 8666
    
    print("=" * 100)
    print(f"ANÁLISIS DEL CAJERO {codigo_cajero}")
    print("=" * 100)
    print()
    
    # Buscar archivo original
    insumos_dir = Path(__file__).parent.parent / "insumos_excel"
    archivos_originales = list(insumos_dir.glob("gestion_*_ksgarro.xlsx"))
    archivos_originales = [f for f in archivos_originales if not f.name.startswith("~$")]
    
    if not archivos_originales:
        print("❌ No se encontró archivo original")
        return
    
    archivo_original = sorted(archivos_originales, key=lambda x: x.stat().st_mtime, reverse=True)[0]
    print(f"1. Archivo original: {archivo_original.name}")
    print()
    
    # Buscar archivo procesado
    archivos_procesados = list(insumos_dir.glob("gestion_*_ksgarro_procesado.xlsx"))
    archivos_procesados = [f for f in archivos_procesados if not f.name.startswith("~$")]
    
    if not archivos_procesados:
        print("❌ No se encontró archivo procesado")
        return
    
    archivo_procesado = sorted(archivos_procesados, key=lambda x: x.stat().st_mtime, reverse=True)[0]
    print(f"2. Archivo procesado: {archivo_procesado.name}")
    print()
    
    # Cargar archivos
    try:
        df_original = pd.read_excel(archivo_original)
        df_procesado = pd.read_excel(archivo_procesado)
    except Exception as e:
        print(f"❌ Error al cargar archivos: {e}")
        return
    
    # Filtrar por cajero
    registros_originales = df_original[df_original['codigo_cajero'] == codigo_cajero]
    registros_procesados = df_procesado[df_procesado['codigo_cajero'] == codigo_cajero]
    
    print(f"3. REGISTROS EN ARCHIVO ORIGINAL: {len(registros_originales)}")
    print("-" * 100)
    print()
    
    if len(registros_originales) == 0:
        print(f"❌ No se encontraron registros para el cajero {codigo_cajero} en el archivo original")
        return
    
    for idx, registro in registros_originales.iterrows():
        tipo = registro.get('tipo_registro', 'N/A')
        print(f"   Registro: {tipo}")
        print(f"   - Fecha arqueo: {registro.get('fecha_arqueo', 'N/A')}")
        print(f"   - Sobrantes: {registro.get('sobrantes', 0)}")
        print(f"   - Faltantes: {registro.get('faltantes', 0)}")
        print(f"   - Justificación: {registro.get('justificacion', 'N/A')}")
        print(f"   - Nuevo estado: {registro.get('nuevo_estado', 'N/A')}")
        print(f"   - Ratificar grabar: {registro.get('ratificar_grabar_diferencia', 'N/A')}")
        print()
    
    print(f"4. REGISTROS EN ARCHIVO PROCESADO: {len(registros_procesados)}")
    print("-" * 100)
    print()
    
    if len(registros_procesados) == 0:
        print(f"❌ No se encontraron registros para el cajero {codigo_cajero} en el archivo procesado")
        return
    
    for idx, registro in registros_procesados.iterrows():
        tipo = registro.get('tipo_registro', 'N/A')
        print(f"📋 REGISTRO {tipo}:")
        print(f"   - Fecha arqueo: {registro.get('fecha_arqueo', 'N/A')}")
        print(f"   - Sobrantes: {registro.get('sobrantes', 0)}")
        print(f"   - Faltantes: {registro.get('faltantes', 0)}")
        print(f"   - Justificación: {registro.get('justificacion', 'N/A')}")
        print(f"   - Nuevo estado: {registro.get('nuevo_estado', 'N/A')}")
        print(f"   - Ratificar grabar: {registro.get('ratificar_grabar_diferencia', 'N/A')}")
        print(f"   - Observaciones: {registro.get('observaciones', 'N/A')}")
        print(f"   - Movimiento encontrado: {registro.get('movimiento_encontrado', 'N/A')}")
        print(f"   - Movimiento fuente: {registro.get('movimiento_fuente', 'N/A')}")
        print()
        
        # Mostrar resumen de pasos si existe
        resumen_pasos = registro.get('resumen_pasos', '')
        if pd.notna(resumen_pasos) and resumen_pasos:
            print(f"   📝 Resumen de pasos ({tipo}):")
            pasos = str(resumen_pasos).split(' | ')
            for i, paso in enumerate(pasos, 1):
                print(f"      {i}. {paso}")
            print()
    
    # Análisis comparativo
    print("=" * 100)
    print("5. ANÁLISIS COMPARATIVO")
    print("=" * 100)
    print()
    
    registro_arqueo = registros_procesados[registros_procesados['tipo_registro'] == 'ARQUEO']
    registro_diario = registros_procesados[registros_procesados['tipo_registro'] == 'DIARIO']
    
    if len(registro_arqueo) > 0:
        arqueo = registro_arqueo.iloc[0]
        sobrante_arqueo = arqueo.get('sobrantes', 0)
        faltante_arqueo = arqueo.get('faltantes', 0)
        diferencia_arqueo = abs(sobrante_arqueo) if sobrante_arqueo < 0 else faltante_arqueo
        
        print(f"   ARQUEO:")
        print(f"     - Diferencia: ${diferencia_arqueo:,.0f}")
        print(f"     - Tipo: {'SOBRANTE' if sobrante_arqueo < 0 else 'FALTANTE' if faltante_arqueo > 0 else 'NINGUNO'}")
        print()
    
    if len(registro_diario) > 0:
        diario = registro_diario.iloc[0]
        sobrante_diario = diario.get('sobrantes', 0)
        faltante_diario = diario.get('faltantes', 0)
        diferencia_diario = abs(sobrante_diario) if sobrante_diario < 0 else faltante_diario
        
        print(f"   DIARIO:")
        print(f"     - Diferencia: ${diferencia_diario:,.0f}")
        print(f"     - Tipo: {'SOBRANTE' if sobrante_diario < 0 else 'FALTANTE' if faltante_diario > 0 else 'NINGUNO'}")
        print()
    
    if len(registro_arqueo) > 0 and len(registro_diario) > 0:
        arqueo = registro_arqueo.iloc[0]
        diario = registro_diario.iloc[0]
        sobrante_arqueo = arqueo.get('sobrantes', 0)
        faltante_arqueo = arqueo.get('faltantes', 0)
        sobrante_diario = diario.get('sobrantes', 0)
        faltante_diario = diario.get('faltantes', 0)
        
        diferencia_arqueo = abs(sobrante_arqueo) if sobrante_arqueo < 0 else faltante_arqueo
        diferencia_diario = abs(sobrante_diario) if sobrante_diario < 0 else faltante_diario
        
        print(f"   COMPARACIÓN:")
        if abs(diferencia_arqueo - diferencia_diario) < 0.01:
            print(f"     ✓ Los valores son iguales (${diferencia_arqueo:,.0f})")
            print(f"     → Se debería aplicar la regla: ARQUEO y DIARIO con misma diferencia")
        else:
            print(f"     ✗ Los valores son diferentes (ARQUEO: ${diferencia_arqueo:,.0f}, DIARIO: ${diferencia_diario:,.0f})")
        print()
    
    # Determinar regla aplicada
    print("=" * 100)
    print("6. REGLA APLICADA")
    print("=" * 100)
    print()
    
    if len(registro_arqueo) > 0:
        arqueo = registro_arqueo.iloc[0]
        justificacion = arqueo.get('justificacion', 'N/A')
        nuevo_estado = arqueo.get('nuevo_estado', 'N/A')
        
        print(f"   ARQUEO:")
        print(f"     - Justificación: {justificacion}")
        print(f"     - Nuevo estado: {nuevo_estado}")
        
        # Determinar regla
        if pd.isna(justificacion) or justificacion == 'N/A':
            print(f"     → REGLA: No identificada claramente")
        elif 'PENDIENTE REVISION MANUAL' in str(justificacion).upper():
            print(f"     → REGLA: Caso por defecto (Pendiente de revisión manual)")
        elif 'ARQUEO sin DIARIO' in str(arqueo.get('resumen_pasos', '')):
            print(f"     → REGLA: ARQUEO sin DIARIO")
        elif 'ARQUEO y DIARIO tienen misma diferencia' in str(arqueo.get('resumen_pasos', '')):
            print(f"     → REGLA: ARQUEO y DIARIO con misma diferencia")
        elif 'DIARIO sin ARQUEO' in str(arqueo.get('resumen_pasos', '')):
            print(f"     → REGLA: DIARIO sin ARQUEO")
        else:
            print(f"     → REGLA: {justificacion}")
        print()
    
    if len(registro_diario) > 0:
        diario = registro_diario.iloc[0]
        justificacion = diario.get('justificacion', 'N/A')
        nuevo_estado = diario.get('nuevo_estado', 'N/A')
        
        print(f"   DIARIO:")
        print(f"     - Justificación: {justificacion}")
        print(f"     - Nuevo estado: {nuevo_estado}")
        
        # Determinar regla
        if pd.isna(justificacion) or justificacion == 'N/A':
            print(f"     → REGLA: No identificada claramente")
        elif 'PENDIENTE REVISION MANUAL' in str(justificacion).upper():
            print(f"     → REGLA: Caso por defecto (Pendiente de revisión manual)")
        elif 'ARQUEO sin DIARIO' in str(diario.get('resumen_pasos', '')):
            print(f"     → REGLA: ARQUEO sin DIARIO")
        elif 'ARQUEO y DIARIO tienen misma diferencia' in str(diario.get('resumen_pasos', '')):
            print(f"     → REGLA: ARQUEO y DIARIO con misma diferencia")
        elif 'DIARIO sin ARQUEO' in str(diario.get('resumen_pasos', '')):
            print(f"     → REGLA: DIARIO sin ARQUEO")
        else:
            print(f"     → REGLA: {justificacion}")
        print()
    
    print("=" * 100)
    print("FIN DEL ANÁLISIS")
    print("=" * 100)

if __name__ == "__main__":
    analizar_cajero_8666()

