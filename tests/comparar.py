"""
Script para comparar el procesamiento manual vs automático
y generar un informe de coincidencias y diferencias.
"""
import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

# Configurar codificación UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

def limpiar_valor_numerico(valor):
    """Limpia y convierte valores numéricos"""
    if pd.isna(valor) or valor == '' or valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        # Remover símbolos comunes
        valor = valor.replace('$', '').replace(',', '').replace(' ', '').replace('-', '')
        try:
            return float(valor)
        except:
            return 0.0
    return 0.0

def normalizar_texto(texto):
    """Normaliza texto para comparación"""
    if pd.isna(texto) or texto is None:
        return ''
    texto = str(texto).strip().upper()
    # Normalizar espacios múltiples
    texto = ' '.join(texto.split())
    return texto

def comparar_archivos(ruta_manual: Path, ruta_automatico: Path) -> Dict:
    """
    Compara dos archivos procesados y genera estadísticas.
    
    Args:
        ruta_manual: Ruta al archivo procesado manualmente
        ruta_automatico: Ruta al archivo procesado automáticamente
    
    Returns:
        Diccionario con estadísticas y diferencias
    """
    print("=" * 100)
    print("COMPARACIÓN: PROCESAMIENTO MANUAL vs AUTOMÁTICO")
    print("=" * 100)
    print(f"\nArchivo manual: {ruta_manual.name}")
    print(f"Archivo automático: {ruta_automatico.name}\n")
    
    # Cargar archivos
    try:
        df_manual = pd.read_excel(ruta_manual)
        df_automatico = pd.read_excel(ruta_automatico)
        print(f"✓ Archivos cargados correctamente")
        print(f"  - Manual: {len(df_manual)} registros")
        print(f"  - Automático: {len(df_automatico)} registros\n")
    except Exception as e:
        print(f"✗ Error al cargar archivos: {e}")
        return {}
    
    # Identificar columnas clave para comparar
    columnas_comparar = [
        'codigo_cajero',
        'tipo_registro',
        'fecha_arqueo',
        'justificacion',
        'nuevo_estado',
        'ratificar_grabar_diferencia',
        'observaciones'
    ]
    
    # Verificar que existan las columnas necesarias
    columnas_faltantes_manual = [c for c in columnas_comparar if c not in df_manual.columns]
    columnas_faltantes_automatico = [c for c in columnas_comparar if c not in df_automatico.columns]
    
    if columnas_faltantes_manual:
        print(f"⚠ Advertencia: Columnas faltantes en archivo manual: {columnas_faltantes_manual}")
    if columnas_faltantes_automatico:
        print(f"⚠ Advertencia: Columnas faltantes en archivo automático: {columnas_faltantes_automatico}")
    
    # Normalizar columnas de código de cajero (puede ser 'codigo_cajero', 'NIT', 'cajero', etc.)
    col_cajero_manual = None
    col_cajero_automatico = None
    
    for col in ['codigo_cajero', 'NIT', 'cajero', 'CAJERO', 'CODIGO_CAJERO']:
        if col in df_manual.columns:
            col_cajero_manual = col
            break
    for col in ['codigo_cajero', 'NIT', 'cajero', 'CAJERO', 'CODIGO_CAJERO']:
        if col in df_automatico.columns:
            col_cajero_automatico = col
            break
    
    if not col_cajero_manual or not col_cajero_automatico:
        print("✗ Error: No se pudo identificar la columna de código de cajero")
        return {}
    
    # Crear clave única para comparar (cajero + tipo_registro + fecha)
    def crear_clave(row, df, col_cajero):
        cajero = int(row[col_cajero]) if pd.notna(row[col_cajero]) else None
        tipo = str(row.get('tipo_registro', '')) if 'tipo_registro' in df.columns else ''
        fecha = str(row.get('fecha_arqueo', '')) if 'fecha_arqueo' in df.columns else ''
        return f"{cajero}_{tipo}_{fecha}"
    
    # Crear diccionarios con las claves
    dict_manual = {}
    for idx, row in df_manual.iterrows():
        clave = crear_clave(row, df_manual, col_cajero_manual)
        if clave not in dict_manual:
            dict_manual[clave] = []
        dict_manual[clave].append((idx, row))
    
    dict_automatico = {}
    for idx, row in df_automatico.iterrows():
        clave = crear_clave(row, df_automatico, col_cajero_automatico)
        if clave not in dict_automatico:
            dict_automatico[clave] = []
        dict_automatico[clave].append((idx, row))
    
    # Comparar registros
    coincidencias = []
    diferencias = []
    solo_manual = []
    solo_automatico = []
    solo_ratificar_grabar_coincide = []  # Casos donde solo coincide ratificar_grabar_diferencia
    solo_ratificar_grabar_coincide = []  # Casos donde solo coincide ratificar_grabar_diferencia
    
    # Obtener todas las claves únicas
    todas_las_claves = set(dict_manual.keys()) | set(dict_automatico.keys())
    
    for clave in todas_las_claves:
        registros_manual = dict_manual.get(clave, [])
        registros_automatico = dict_automatico.get(clave, [])
        
        if not registros_manual:
            # Solo está en automático
            for idx, row in registros_automatico:
                solo_automatico.append({
                    'clave': clave,
                    'registro': row,
                    'origen': 'automático'
                })
            continue
        
        if not registros_automatico:
            # Solo está en manual
            for idx, row in registros_manual:
                solo_manual.append({
                    'clave': clave,
                    'registro': row,
                    'origen': 'manual'
                })
            continue
        
        # Comparar cada registro manual con cada registro automático
        for idx_m, row_m in registros_manual:
            mejor_coincidencia = None
            mejor_score = 0
            mejor_diferencias = []
            solo_ratificar_igual = False
            
            for idx_a, row_a in registros_automatico:
                # Comparar campos clave
                campos_iguales = []
                campos_diferentes = []
                ratificar_igual = False
                
                for campo in ['justificacion', 'nuevo_estado', 'ratificar_grabar_diferencia', 'observaciones']:
                    if campo in df_manual.columns and campo in df_automatico.columns:
                        valor_manual = normalizar_texto(row_m.get(campo, ''))
                        valor_automatico = normalizar_texto(row_a.get(campo, ''))
                        
                        if valor_manual == valor_automatico:
                            campos_iguales.append(campo)
                            if campo == 'ratificar_grabar_diferencia':
                                ratificar_igual = True
                        else:
                            campos_diferentes.append({
                                'campo': campo,
                                'manual': valor_manual if valor_manual else '(vacío)',
                                'automatico': valor_automatico if valor_automatico else '(vacío)'
                            })
                
                score = len(campos_iguales) / 4.0  # 4 campos a comparar
                
                if score > mejor_score:
                    mejor_score = score
                    mejor_coincidencia = (idx_a, row_a)
                    mejor_diferencias = campos_diferentes
                    solo_ratificar_igual = (ratificar_igual and len(campos_iguales) == 1)
            
            # Si todos los campos coinciden, es una coincidencia perfecta
            if mejor_score == 1.0:
                coincidencias.append({
                    'clave': clave,
                    'registro_manual': row_m,
                    'registro_automatico': mejor_coincidencia[1],
                    'cajero': int(row_m[col_cajero_manual]) if pd.notna(row_m[col_cajero_manual]) else None,
                    'tipo_registro': str(row_m.get('tipo_registro', '')) if 'tipo_registro' in df_manual.columns else ''
                })
            else:
                # Verificar si solo coincide ratificar_grabar_diferencia
                if solo_ratificar_igual and mejor_coincidencia:
                    solo_ratificar_grabar_coincide.append({
                        'clave': clave,
                        'cajero': int(row_m[col_cajero_manual]) if pd.notna(row_m[col_cajero_manual]) else None,
                        'tipo_registro': str(row_m.get('tipo_registro', '')) if 'tipo_registro' in df_manual.columns else '',
                        'registro_manual': row_m,
                        'registro_automatico': mejor_coincidencia[1],
                        'diferencias': mejor_diferencias
                    })
                diferencias.append({
                    'clave': clave,
                    'registro_manual': row_m,
                    'registro_automatico': mejor_coincidencia[1] if mejor_coincidencia else None,
                    'cajero': int(row_m[col_cajero_manual]) if pd.notna(row_m[col_cajero_manual]) else None,
                    'tipo_registro': str(row_m.get('tipo_registro', '')) if 'tipo_registro' in df_manual.columns else '',
                    'diferencias': mejor_diferencias,
                    'score': mejor_score
                })
    
    # Calcular estadísticas
    total_registros_manual = len(df_manual)
    total_coincidencias = len(coincidencias)
    total_diferencias = len(diferencias)
    total_solo_manual = len(solo_manual)
    total_solo_automatico = len(solo_automatico)
    total_solo_ratificar_coincide = len(solo_ratificar_grabar_coincide)
    
    porcentaje_acierto = (total_coincidencias / total_registros_manual * 100) if total_registros_manual > 0 else 0
    porcentaje_solo_ratificar = (total_solo_ratificar_coincide / total_registros_manual * 100) if total_registros_manual > 0 else 0
    
    # Generar informe
    print("=" * 100)
    print("RESUMEN DE COMPARACIÓN")
    print("=" * 100)
    print(f"\nTotal de registros en archivo manual: {total_registros_manual}")
    print(f"Total de registros en archivo automático: {len(df_automatico)}")
    print(f"\n{'='*100}")
    print(f"ESTADÍSTICAS:")
    print(f"{'='*100}")
    print(f"✓ Coincidencias perfectas: {total_coincidencias} ({porcentaje_acierto:.2f}%)")
    print(f"✗ Diferencias encontradas: {total_diferencias} ({100-porcentaje_acierto:.2f}%)")
    print(f"⚠ Solo en manual: {total_solo_manual}")
    print(f"⚠ Solo en automático: {total_solo_automatico}")
    print(f"✓ Solo coincide en 'ratificar_grabar_diferencia': {total_solo_ratificar_coincide} ({porcentaje_solo_ratificar:.2f}%)")
    
    # Mostrar diferencias detalladas
    if diferencias:
        print(f"\n{'='*100}")
        print(f"DIFERENCIAS DETALLADAS ({len(diferencias)} casos):")
        print(f"{'='*100}\n")
        
        for i, diff in enumerate(diferencias[:50], 1):  # Mostrar primeros 50
            print(f"\n{i}. Cajero: {diff['cajero']} | Tipo: {diff['tipo_registro']}")
            print(f"   Score de coincidencia: {diff['score']*100:.1f}%")
            print(f"   Diferencias:")
            for campo_diff in diff['diferencias']:
                print(f"     - {campo_diff['campo']}:")
                print(f"       Manual:    {campo_diff['manual']}")
                print(f"       Automático: {campo_diff['automatico']}")
        
        if len(diferencias) > 50:
            print(f"\n... y {len(diferencias) - 50} casos más")
    
    # Agrupar diferencias por tipo
    if diferencias:
        print(f"\n{'='*100}")
        print(f"ANÁLISIS DE DIFERENCIAS POR CAMPO:")
        print(f"{'='*100}\n")
        
        diferencias_por_campo = {
            'justificacion': 0,
            'nuevo_estado': 0,
            'ratificar_grabar_diferencia': 0,
            'observaciones': 0
        }
        
        for diff in diferencias:
            for campo_diff in diff['diferencias']:
                campo = campo_diff['campo']
                if campo in diferencias_por_campo:
                    diferencias_por_campo[campo] += 1
        
        for campo, cantidad in diferencias_por_campo.items():
            porcentaje = (cantidad / total_diferencias * 100) if total_diferencias > 0 else 0
            print(f"  - {campo}: {cantidad} casos ({porcentaje:.1f}% de las diferencias)")
    
    # Guardar informe detallado en archivo
    # Crear carpeta informe_comparacion si no existe
    carpeta_informes = ruta_manual.parent / 'informe_comparacion'
    carpeta_informes.mkdir(exist_ok=True)
    
    ruta_informe = carpeta_informes / f"informe_comparacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(ruta_informe, 'w', encoding='utf-8') as f:
        f.write("=" * 100 + "\n")
        f.write("INFORME DE COMPARACIÓN: PROCESAMIENTO MANUAL vs AUTOMÁTICO\n")
        f.write("=" * 100 + "\n\n")
        f.write(f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Archivo manual: {ruta_manual.name}\n")
        f.write(f"Archivo automático: {ruta_automatico.name}\n\n")
        f.write(f"Total de registros en archivo manual: {total_registros_manual}\n")
        f.write(f"Total de registros en archivo automático: {len(df_automatico)}\n\n")
        f.write(f"ESTADÍSTICAS:\n")
        f.write(f"✓ Coincidencias perfectas: {total_coincidencias} ({porcentaje_acierto:.2f}%)\n")
        f.write(f"✗ Diferencias encontradas: {total_diferencias} ({100-porcentaje_acierto:.2f}%)\n")
        f.write(f"⚠ Solo en manual: {total_solo_manual}\n")
        f.write(f"⚠ Solo en automático: {total_solo_automatico}\n")
        f.write(f"✓ Solo coincide en 'ratificar_grabar_diferencia': {total_solo_ratificar_coincide} ({porcentaje_solo_ratificar:.2f}%)\n\n")
        
        if diferencias:
            f.write(f"\n{'='*100}\n")
            f.write(f"DIFERENCIAS DETALLADAS ({len(diferencias)} casos):\n")
            f.write(f"{'='*100}\n\n")
            
            for i, diff in enumerate(diferencias, 1):
                f.write(f"\n{i}. Cajero: {diff['cajero']} | Tipo: {diff['tipo_registro']}\n")
                f.write(f"   Score de coincidencia: {diff['score']*100:.1f}%\n")
                f.write(f"   Diferencias:\n")
                for campo_diff in diff['diferencias']:
                    f.write(f"     - {campo_diff['campo']}:\n")
                    f.write(f"       Manual:    {campo_diff['manual']}\n")
                    f.write(f"       Automático: {campo_diff['automatico']}\n")
    
    print(f"\n{'='*100}")
    print(f"✓ Informe detallado guardado en: {ruta_informe}")
    print(f"{'='*100}\n")
    
    return {
        'total_manual': total_registros_manual,
        'total_automatico': len(df_automatico),
        'coincidencias': total_coincidencias,
        'diferencias': total_diferencias,
        'solo_manual': total_solo_manual,
        'solo_automatico': total_solo_automatico,
        'solo_ratificar_coincide': total_solo_ratificar_coincide,
        'porcentaje_acierto': porcentaje_acierto,
        'porcentaje_solo_ratificar': porcentaje_solo_ratificar,
        'lista_diferencias': diferencias,
        'lista_solo_ratificar': solo_ratificar_grabar_coincide,
        'ruta_informe': ruta_informe
    }

def main(fecha_especifica=None):
    """Función principal"""
    # Obtener fecha de hoy o usar fecha específica
    if fecha_especifica:
        # Formato esperado: DD_MM_YYYY (ejemplo: 15_12_2025)
        fecha_str = fecha_especifica
    else:
        fecha_hoy = datetime.now()
        # Formato: DD_MM_YYYY (ejemplo: 15_12_2025)
        fecha_str = fecha_hoy.strftime('%d_%m_%Y')
    
    # Buscar archivos
    proyecto_root = Path(__file__).parent.parent
    insumos_dir = proyecto_root / 'insumos_excel'
    
    # Buscar archivo manual (Karol) - buscar con fecha de hoy
    archivo_manual = None
    patron_manual = f'gestion_{fecha_str}_ksgarro_Karol.xlsx'
    for archivo in insumos_dir.glob(patron_manual):
        archivo_manual = archivo
        break
    
    if not archivo_manual:
        print(f"✗ No se encontró el archivo manual: {patron_manual}")
        print(f"   Buscando en: {insumos_dir}")
        print(f"   Fecha buscada: {fecha_str}")
        # Intentar buscar cualquier archivo manual reciente
        archivos_manuales = list(insumos_dir.glob('gestion_*_ksgarro_Karol.xlsx'))
        if archivos_manuales:
            archivos_manuales.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            print(f"   Archivos manuales encontrados (más reciente primero):")
            for arch in archivos_manuales[:5]:
                print(f"     - {arch.name}")
        return
    
    # Buscar archivo automático - buscar con fecha de hoy
    archivo_automatico = None
    patron_automatico = f'gestion_{fecha_str}_ksgarro_procesado.xlsx'
    for archivo in insumos_dir.glob(patron_automatico):
        archivo_automatico = archivo
        break
    
    if not archivo_automatico:
        print(f"✗ No se encontró el archivo automático: {patron_automatico}")
        print(f"   Buscando en: {insumos_dir}")
        print(f"   Fecha buscada: {fecha_str}")
        # Intentar buscar cualquier archivo automático reciente
        archivos_automaticos = list(insumos_dir.glob('gestion_*_ksgarro_procesado.xlsx'))
        if archivos_automaticos:
            archivos_automaticos.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            print(f"   Archivos automáticos encontrados (más reciente primero):")
            for arch in archivos_automaticos[:5]:
                print(f"     - {arch.name}")
        return
    
    # Comparar archivos
    resultado = comparar_archivos(archivo_manual, archivo_automatico)
    
    if resultado:
        print("\n" + "=" * 100)
        print("COMPARACIÓN COMPLETADA")
        print("=" * 100)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Comparar procesamiento manual vs automático')
    parser.add_argument(
        '--fecha',
        type=str,
        help='Fecha específica en formato DD_MM_YYYY (ej: 15_12_2025) para analizar'
    )
    args = parser.parse_args()
    main(fecha_especifica=args.fecha)

