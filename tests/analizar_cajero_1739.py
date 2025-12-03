"""
Script para analizar el cajero 1739 y ver qué regla se aplicó.
"""
import pandas as pd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

def analizar_cajero_1739():
    """Analiza el cajero 1739."""
    
    codigo_cajero = 1739
    
    print("=" * 100)
    print(f"ANÁLISIS DEL CAJERO {codigo_cajero}")
    print("=" * 100)
    
    # Buscar archivo original
    archivos_originales = sorted([f for f in Path("insumos_excel").glob("gestion_*_ksgarro.xlsx") 
                                  if not f.name.startswith("~$") and "procesado" not in f.name], 
                                 key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not archivos_originales:
        print("No se encontraron archivos originales")
        return
    
    archivo_original = archivos_originales[0]
    print(f"\n1. Archivo original: {archivo_original.name}")
    
    df_original = pd.read_excel(archivo_original)
    cajero_original = df_original[df_original['codigo_cajero'] == codigo_cajero]
    
    if len(cajero_original) == 0:
        print(f"\n❌ Cajero {codigo_cajero} no encontrado en archivo original")
        return
    
    print(f"\n2. REGISTROS EN ARCHIVO ORIGINAL: {len(cajero_original)}")
    print("-" * 100)
    
    for idx, row in cajero_original.iterrows():
        tipo_registro = row.get('tipo_registro', 'N/A')
        print(f"\n   Registro: {tipo_registro}")
        print(f"   - Fecha arqueo: {row.get('fecha_arqueo', 'N/A')}")
        print(f"   - Sobrantes: {row.get('sobrantes', 'N/A')}")
        print(f"   - Faltantes: {row.get('faltantes', 'N/A')}")
        print(f"   - Justificación: {row.get('justificacion', 'N/A')}")
        print(f"   - Nuevo estado: {row.get('nuevo_estado', 'N/A')}")
        print(f"   - Ratificar grabar: {row.get('ratificar_grabar_diferencia', 'N/A')}")
    
    # Buscar archivo procesado
    archivos_procesados = sorted([f for f in Path("insumos_excel").glob("*procesado.xlsx") 
                                  if not f.name.startswith("~$")], 
                                 key=lambda x: x.stat().st_mtime, reverse=True)
    
    if archivos_procesados:
        archivo_procesado = archivos_procesados[0]
        print(f"\n3. Archivo procesado: {archivo_procesado.name}")
        
        df_procesado = pd.read_excel(archivo_procesado)
        cajero_procesado = df_procesado[df_procesado['codigo_cajero'] == codigo_cajero]
        
        if len(cajero_procesado) == 0:
            print(f"\n⚠️  Cajero {codigo_cajero} NO aparece en archivo procesado")
            print("   Esto puede significar que:")
            print("   - El registro fue filtrado (no tenía descuadre o no cumplía criterios)")
            print("   - Hubo un error durante el procesamiento")
        else:
            print(f"\n4. REGISTROS EN ARCHIVO PROCESADO: {len(cajero_procesado)}")
            print("-" * 100)
            
            for idx, row in cajero_procesado.iterrows():
                tipo_registro = row.get('tipo_registro', 'N/A')
                print(f"\n   Registro: {tipo_registro}")
                print(f"   - Fecha arqueo: {row.get('fecha_arqueo', 'N/A')}")
                print(f"   - Sobrantes: {row.get('sobrantes', 'N/A')}")
                print(f"   - Faltantes: {row.get('faltantes', 'N/A')}")
                print(f"   - Justificación: {row.get('justificacion', 'N/A')}")
                print(f"   - Nuevo estado: {row.get('nuevo_estado', 'N/A')}")
                print(f"   - Ratificar grabar: {row.get('ratificar_grabar_diferencia', 'N/A')}")
                print(f"   - Observaciones: {row.get('observaciones', 'N/A')}")
                
                resumen_pasos = row.get('resumen_pasos', '')
                if pd.notna(resumen_pasos) and resumen_pasos:
                    print(f"\n   📝 Resumen de pasos:")
                    pasos = str(resumen_pasos).split(' | ')
                    for i, paso in enumerate(pasos, 1):
                        print(f"      {i}. {paso}")
                
                # Determinar regla
                justificacion = row.get('justificacion', 'N/A')
                nuevo_estado = row.get('nuevo_estado', 'N/A')
                
                print(f"\n   🔍 REGLA APLICADA:")
                if 'Solo llega ARQUEO' in str(resumen_pasos):
                    print("     → REGLA: ARQUEO sin DIARIO")
                elif 'ARQUEO y DIARIO tienen misma diferencia' in str(resumen_pasos):
                    print("     → REGLA: ARQUEO y DIARIO con misma diferencia")
                elif 'provisión' in str(resumen_pasos).lower():
                    print("     → REGLA: Provisión día anterior")
                elif justificacion == 'PENDIENTE REVISION MANUAL' or nuevo_estado == 'Pendiente de revisión manual':
                    print("     → REGLA: Caso por defecto (Pendiente de revisión manual)")
                elif 'CRUCE DE NOVEDADES' in str(nuevo_estado) or 'Cruzar' in str(justificacion):
                    print("     → REGLA: Cruce de novedades (movimiento con fecha diferente)")
                elif 'PARTIDA YA CONTABILIZADA' in str(nuevo_estado):
                    print("     → REGLA: Movimiento encontrado en NACIONAL (misma fecha)")
                elif 'SOBRANTE CONTABLE' in str(nuevo_estado) or 'FALTANTE CONTABLE' in str(nuevo_estado):
                    print("     → REGLA: Movimiento encontrado en BD (SOBRANTES_BD o FALTANTES_BD)")
                elif 'SOBRANTE EN ARQUEO' in str(nuevo_estado) or 'FALTANTE EN ARQUEO' in str(nuevo_estado):
                    print("     → REGLA: No se encontró movimiento (descuadre físico)")
                else:
                    print("     → REGLA: No identificada claramente")
    else:
        print("\n⚠️  No se encontraron archivos procesados")

if __name__ == "__main__":
    analizar_cajero_1739()

