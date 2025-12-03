"""
Script para analizar en detalle el cajero 1448 y entender qué regla se aplicó.
"""
import pandas as pd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger_config import configurar_logger
import logging

configurar_logger(nivel=logging.INFO)
logger = logging.getLogger(__name__)

# También usar print para asegurar que se vea la salida
def print_and_log(msg):
    print(msg)
    logger.info(msg)

def analizar_cajero_1448():
    """Analiza en detalle el cajero 1448."""
    
    # Cargar archivo procesado más reciente (excluir archivos temporales que empiezan con ~$)
    archivos = sorted([f for f in Path("insumos_excel").glob("*procesado.xlsx") if not f.name.startswith("~$")], 
                     key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not archivos:
        print_and_log("No se encontraron archivos procesados")
        return
    
    archivo_procesado = archivos[0]
    print_and_log(f"Analizando archivo: {archivo_procesado}")
    
    print_and_log("=" * 100)
    print_and_log("ANÁLISIS DETALLADO DEL CAJERO 1448")
    print_and_log("=" * 100)
    
    # Cargar archivo procesado
    df_procesado = pd.read_excel(archivo_procesado)
    cajero_1448 = df_procesado[df_procesado['codigo_cajero'] == 1448]
    
    if len(cajero_1448) == 0:
        print_and_log("Cajero 1448 no encontrado en el archivo procesado")
        return
    
    print_and_log(f"\n1. REGISTROS ENCONTRADOS: {len(cajero_1448)}")
    print_and_log("-" * 100)
    
    for idx, row in cajero_1448.iterrows():
        tipo_registro = row.get('tipo_registro', 'N/A')
        print_and_log(f"\n   Registro: {tipo_registro}")
        print_and_log(f"   - Fecha arqueo: {row.get('fecha_arqueo', 'N/A')}")
        print_and_log(f"   - Sobrantes: {row.get('sobrantes', 'N/A')}")
        print_and_log(f"   - Faltantes: {row.get('faltantes', 'N/A')}")
        print_and_log(f"   - Justificación: {row.get('justificacion', 'N/A')}")
        print_and_log(f"   - Nuevo estado: {row.get('nuevo_estado', 'N/A')}")
        print_and_log(f"   - Ratificar grabar: {row.get('ratificar_grabar_diferencia', 'N/A')}")
        print_and_log(f"   - Observaciones: {row.get('observaciones', 'N/A')}")
        print_and_log(f"   - Movimiento encontrado: {row.get('movimiento_encontrado', 'N/A')}")
        print_and_log(f"   - Movimiento fuente: {row.get('movimiento_fuente', 'N/A')}")
        print_and_log(f"   - Resumen pasos: {row.get('resumen_pasos', 'N/A')}")
    
    # Separar registros ARQUEO y DIARIO
    registro_arqueo = cajero_1448[cajero_1448['tipo_registro'] == 'ARQUEO']
    registro_diario = cajero_1448[cajero_1448['tipo_registro'] == 'DIARIO']
    
    print_and_log("\n" + "=" * 100)
    print_and_log("2. REGLA APLICADA")
    print_and_log("=" * 100)
    
    if len(registro_arqueo) > 0:
        arqueo = registro_arqueo.iloc[0]
        resumen_pasos_arqueo = arqueo.get('resumen_pasos', '')
        justificacion_arqueo = arqueo.get('justificacion', 'N/A')
        nuevo_estado_arqueo = arqueo.get('nuevo_estado', 'N/A')
        
        print_and_log(f"\n   ARQUEO:")
        print_and_log(f"     - Justificación: {justificacion_arqueo}")
        print_and_log(f"     - Nuevo estado: {nuevo_estado_arqueo}")
        
        if resumen_pasos_arqueo:
            print_and_log(f"\n   Resumen de pasos (ARQUEO):")
            pasos = str(resumen_pasos_arqueo).split(' | ')
            for i, paso in enumerate(pasos, 1):
                print_and_log(f"     {i}. {paso}")
        
        # Determinar qué regla se aplicó basándose en la justificación y el resumen
        print_and_log(f"\n   REGLA IDENTIFICADA:")
        if 'Solo llega ARQUEO' in str(resumen_pasos_arqueo):
            print_and_log("     → REGLA: ARQUEO sin DIARIO")
        elif 'ARQUEO y DIARIO tienen misma diferencia' in str(resumen_pasos_arqueo):
            print_and_log("     → REGLA: ARQUEO y DIARIO con misma diferencia")
        elif 'provisión' in str(resumen_pasos_arqueo).lower():
            print_and_log("     → REGLA: Provisión día anterior")
        elif justificacion_arqueo == 'PENDIENTE REVISION MANUAL' or nuevo_estado_arqueo == 'Pendiente de revisión manual':
            print_and_log("     → REGLA: Caso por defecto (Pendiente de revisión manual)")
        elif 'NACIONAL' in str(resumen_pasos_arqueo) and 'PARTIDA YA CONTABILIZADA' in str(nuevo_estado_arqueo):
            print_and_log("     → REGLA: Movimiento encontrado en NACIONAL (misma fecha)")
        elif 'CRUCE DE NOVEDADES' in str(nuevo_estado_arqueo) or 'CRUZAR' in str(justificacion_arqueo):
            print_and_log("     → REGLA: Cruce de novedades (movimiento con fecha diferente)")
        elif 'SOBRANTE CONTABLE' in str(nuevo_estado_arqueo) or 'FALTANTE CONTABLE' in str(nuevo_estado_arqueo):
            print_and_log("     → REGLA: Movimiento encontrado en BD (SOBRANTES_BD o FALTANTES_BD)")
        elif 'SOBRANTE EN ARQUEO' in str(nuevo_estado_arqueo) or 'FALTANTE EN ARQUEO' in str(nuevo_estado_arqueo):
            print_and_log("     → REGLA: No se encontró movimiento (descuadre físico)")
        else:
            print_and_log("     → REGLA: No identificada claramente")
    
    if len(registro_diario) > 0:
        diario = registro_diario.iloc[0]
        resumen_pasos_diario = diario.get('resumen_pasos', '')
        justificacion_diario = diario.get('justificacion', 'N/A')
        nuevo_estado_diario = diario.get('nuevo_estado', 'N/A')
        
        print_and_log(f"\n   DIARIO:")
        print_and_log(f"     - Justificación: {justificacion_diario}")
        print_and_log(f"     - Nuevo estado: {nuevo_estado_diario}")
        
        if resumen_pasos_diario:
            print_and_log(f"\n   Resumen de pasos (DIARIO):")
            pasos = str(resumen_pasos_diario).split(' | ')
            for i, paso in enumerate(pasos, 1):
                print_and_log(f"     {i}. {paso}")

if __name__ == "__main__":
    analizar_cajero_1448()

