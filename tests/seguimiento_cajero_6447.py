"""
Script para hacer seguimiento detallado del cajero 6447.
"""
import pandas as pd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.cargador_config import CargadorConfig
from src.procesamiento.procesador_arqueos import ProcesadorArqueos
from src.consultas.consultor_movimientos import ConsultorMovimientos
from src.utils.logger_config import configurar_logger
import logging

configurar_logger(nivel=logging.INFO)
logger = logging.getLogger(__name__)

def print_and_log(msg):
    """Imprime y registra el mensaje."""
    print(msg)
    logger.info(msg)

def seguimiento_cajero_6447():
    """Hace seguimiento detallado del cajero 6447."""
    
    codigo_cajero = 6447
    
    print_and_log("=" * 100)
    print_and_log(f"SEGUIMIENTO DETALLADO DEL CAJERO {codigo_cajero}")
    print_and_log("=" * 100)
    
    # 1. Cargar archivo procesado más reciente
    archivos = sorted([f for f in Path("insumos_excel").glob("*procesado.xlsx") if not f.name.startswith("~$")], 
                     key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not archivos:
        print_and_log("No se encontraron archivos procesados")
        return
    
    archivo_procesado = archivos[0]
    print_and_log(f"\n1. Archivo procesado: {archivo_procesado.name}")
    
    # Cargar archivo procesado
    df_procesado = pd.read_excel(archivo_procesado)
    cajero_6447 = df_procesado[df_procesado['codigo_cajero'] == codigo_cajero]
    
    if len(cajero_6447) == 0:
        print_and_log(f"\n❌ Cajero {codigo_cajero} no encontrado en el archivo procesado")
        return
    
    print_and_log(f"\n2. REGISTROS ENCONTRADOS: {len(cajero_6447)}")
    print_and_log("-" * 100)
    
    # Separar registros ARQUEO y DIARIO
    registro_arqueo = cajero_6447[cajero_6447['tipo_registro'] == 'ARQUEO']
    registro_diario = cajero_6447[cajero_6447['tipo_registro'] == 'DIARIO']
    
    # Mostrar información del ARQUEO
    if len(registro_arqueo) > 0:
        arqueo = registro_arqueo.iloc[0]
        print_and_log("\n📋 REGISTRO ARQUEO:")
        print_and_log(f"   - Fecha arqueo: {arqueo.get('fecha_arqueo', 'N/A')}")
        print_and_log(f"   - Sobrantes: {arqueo.get('sobrantes', 'N/A'):,.0f}" if pd.notna(arqueo.get('sobrantes')) else f"   - Sobrantes: {arqueo.get('sobrantes', 'N/A')}")
        print_and_log(f"   - Faltantes: {arqueo.get('faltantes', 'N/A'):,.0f}" if pd.notna(arqueo.get('faltantes')) else f"   - Faltantes: {arqueo.get('faltantes', 'N/A')}")
        print_and_log(f"   - Justificación: {arqueo.get('justificacion', 'N/A')}")
        print_and_log(f"   - Nuevo estado: {arqueo.get('nuevo_estado', 'N/A')}")
        print_and_log(f"   - Ratificar grabar: {arqueo.get('ratificar_grabar_diferencia', 'N/A')}")
        print_and_log(f"   - Observaciones: {arqueo.get('observaciones', 'N/A')}")
        print_and_log(f"   - Movimiento encontrado: {arqueo.get('movimiento_encontrado', 'N/A')}")
        print_and_log(f"   - Movimiento fuente: {arqueo.get('movimiento_fuente', 'N/A')}")
        
        resumen_pasos_arqueo = arqueo.get('resumen_pasos', '')
        if pd.notna(resumen_pasos_arqueo) and resumen_pasos_arqueo:
            print_and_log(f"\n   📝 Resumen de pasos (ARQUEO):")
            pasos = str(resumen_pasos_arqueo).split(' | ')
            for i, paso in enumerate(pasos, 1):
                print_and_log(f"      {i}. {paso}")
        else:
            print_and_log(f"\n   ⚠️  No hay resumen de pasos disponible para ARQUEO")
    
    # Mostrar información del DIARIO
    if len(registro_diario) > 0:
        diario = registro_diario.iloc[0]
        print_and_log("\n📋 REGISTRO DIARIO:")
        print_and_log(f"   - Fecha arqueo: {diario.get('fecha_arqueo', 'N/A')}")
        print_and_log(f"   - Sobrantes: {diario.get('sobrantes', 'N/A'):,.0f}" if pd.notna(diario.get('sobrantes')) else f"   - Sobrantes: {diario.get('sobrantes', 'N/A')}")
        print_and_log(f"   - Faltantes: {diario.get('faltantes', 'N/A'):,.0f}" if pd.notna(diario.get('faltantes')) else f"   - Faltantes: {diario.get('faltantes', 'N/A')}")
        print_and_log(f"   - Justificación: {diario.get('justificacion', 'N/A')}")
        print_and_log(f"   - Nuevo estado: {diario.get('nuevo_estado', 'N/A')}")
        print_and_log(f"   - Ratificar grabar: {diario.get('ratificar_grabar_diferencia', 'N/A')}")
        print_and_log(f"   - Observaciones: {diario.get('observaciones', 'N/A')}")
        print_and_log(f"   - Movimiento encontrado: {diario.get('movimiento_encontrado', 'N/A')}")
        print_and_log(f"   - Movimiento fuente: {diario.get('movimiento_fuente', 'N/A')}")
        
        resumen_pasos_diario = diario.get('resumen_pasos', '')
        if pd.notna(resumen_pasos_diario) and resumen_pasos_diario:
            print_and_log(f"\n   📝 Resumen de pasos (DIARIO):")
            pasos = str(resumen_pasos_diario).split(' | ')
            for i, paso in enumerate(pasos, 1):
                print_and_log(f"      {i}. {paso}")
        else:
            print_and_log(f"\n   ⚠️  No hay resumen de pasos disponible para DIARIO")
    
    # Análisis comparativo
    print_and_log("\n" + "=" * 100)
    print_and_log("3. ANÁLISIS COMPARATIVO")
    print_and_log("=" * 100)
    
    if len(registro_arqueo) > 0 and len(registro_diario) > 0:
        arqueo = registro_arqueo.iloc[0]
        diario = registro_diario.iloc[0]
        
        sobrante_arqueo = arqueo.get('sobrantes', 0) if pd.notna(arqueo.get('sobrantes')) else 0
        faltante_arqueo = arqueo.get('faltantes', 0) if pd.notna(arqueo.get('faltantes')) else 0
        sobrante_diario = diario.get('sobrantes', 0) if pd.notna(diario.get('sobrantes')) else 0
        faltante_diario = diario.get('faltantes', 0) if pd.notna(diario.get('faltantes')) else 0
        
        diferencia_arqueo = abs(sobrante_arqueo) if sobrante_arqueo < 0 else faltante_arqueo
        diferencia_diario = abs(sobrante_diario) if sobrante_diario < 0 else faltante_diario
        
        print_and_log(f"\n   ARQUEO:")
        print_and_log(f"     - Diferencia: ${diferencia_arqueo:,.0f}")
        print_and_log(f"     - Tipo: {'SOBRANTE' if sobrante_arqueo < 0 else 'FALTANTE' if faltante_arqueo > 0 else 'NINGUNO'}")
        
        print_and_log(f"\n   DIARIO:")
        print_and_log(f"     - Diferencia: ${diferencia_diario:,.0f}")
        print_and_log(f"     - Tipo: {'SOBRANTE' if sobrante_diario < 0 else 'FALTANTE' if faltante_diario > 0 else 'NINGUNO'}")
        
        print_and_log(f"\n   COMPARACIÓN:")
        if abs(diferencia_arqueo - diferencia_diario) < 0.01:
            print_and_log(f"     ✓ Los valores son iguales (${diferencia_arqueo:,.0f})")
            print_and_log(f"     → Se debería aplicar la regla: ARQUEO y DIARIO con misma diferencia")
        else:
            print_and_log(f"     ⚠️  Los valores NO son iguales")
            print_and_log(f"     → Diferencia: ${abs(diferencia_arqueo - diferencia_diario):,.0f}")
            print_and_log(f"     → Cada registro se procesó independientemente")
    
    # Identificar regla aplicada
    print_and_log("\n" + "=" * 100)
    print_and_log("4. REGLA APLICADA")
    print_and_log("=" * 100)
    
    if len(registro_arqueo) > 0:
        arqueo = registro_arqueo.iloc[0]
        justificacion_arqueo = arqueo.get('justificacion', 'N/A')
        nuevo_estado_arqueo = arqueo.get('nuevo_estado', 'N/A')
        resumen_pasos_arqueo = arqueo.get('resumen_pasos', '')
        
        print_and_log(f"\n   ARQUEO:")
        print_and_log(f"     - Justificación: {justificacion_arqueo}")
        print_and_log(f"     - Nuevo estado: {nuevo_estado_arqueo}")
        
        # Determinar regla
        if 'Solo llega ARQUEO' in str(resumen_pasos_arqueo):
            print_and_log(f"     → REGLA: ARQUEO sin DIARIO")
        elif 'ARQUEO y DIARIO tienen misma diferencia' in str(resumen_pasos_arqueo):
            print_and_log(f"     → REGLA: ARQUEO y DIARIO con misma diferencia")
        elif 'provisión' in str(resumen_pasos_arqueo).lower():
            print_and_log(f"     → REGLA: Provisión día anterior")
        elif justificacion_arqueo == 'PENDIENTE REVISION MANUAL' or nuevo_estado_arqueo == 'Pendiente de revisión manual':
            print_and_log(f"     → REGLA: Caso por defecto (Pendiente de revisión manual)")
        elif 'CRUCE DE NOVEDADES' in str(nuevo_estado_arqueo) or 'Cruzar' in str(justificacion_arqueo):
            print_and_log(f"     → REGLA: Cruce de novedades (movimiento con fecha diferente)")
        elif 'PARTIDA YA CONTABILIZADA' in str(nuevo_estado_arqueo):
            print_and_log(f"     → REGLA: Movimiento encontrado en NACIONAL (misma fecha)")
        elif 'SOBRANTE CONTABLE' in str(nuevo_estado_arqueo) or 'FALTANTE CONTABLE' in str(nuevo_estado_arqueo):
            print_and_log(f"     → REGLA: Movimiento encontrado en BD (SOBRANTES_BD o FALTANTES_BD)")
        elif 'SOBRANTE EN ARQUEO' in str(nuevo_estado_arqueo) or 'FALTANTE EN ARQUEO' in str(nuevo_estado_arqueo):
            print_and_log(f"     → REGLA: No se encontró movimiento (descuadre físico)")
        else:
            print_and_log(f"     → REGLA: No identificada claramente")
    
    if len(registro_diario) > 0:
        diario = registro_diario.iloc[0]
        justificacion_diario = diario.get('justificacion', 'N/A')
        nuevo_estado_diario = diario.get('nuevo_estado', 'N/A')
        resumen_pasos_diario = diario.get('resumen_pasos', '')
        
        print_and_log(f"\n   DIARIO:")
        print_and_log(f"     - Justificación: {justificacion_diario}")
        print_and_log(f"     - Nuevo estado: {nuevo_estado_diario}")
        
        # Determinar regla
        if 'Solo llega DIARIO' in str(resumen_pasos_diario):
            print_and_log(f"     → REGLA: DIARIO sin ARQUEO")
        elif 'ARQUEO y DIARIO tienen misma diferencia' in str(resumen_pasos_diario):
            print_and_log(f"     → REGLA: ARQUEO y DIARIO con misma diferencia")
        elif 'CRUCE DE NOVEDADES' in str(nuevo_estado_diario) or 'Cruzar' in str(justificacion_diario):
            print_and_log(f"     → REGLA: Cruce de novedades (movimiento con fecha diferente)")
        elif 'PARTIDA YA CONTABILIZADA' in str(nuevo_estado_diario):
            print_and_log(f"     → REGLA: Movimiento encontrado en NACIONAL (misma fecha)")
        elif 'SOBRANTE CONTABLE' in str(nuevo_estado_diario) or 'FALTANTE CONTABLE' in str(nuevo_estado_diario):
            print_and_log(f"     → REGLA: Movimiento encontrado en BD (SOBRANTES_BD o FALTANTES_BD)")
        elif 'SOBRANTE EN ARQUEO' in str(nuevo_estado_diario) or 'FALTANTE EN ARQUEO' in str(nuevo_estado_diario):
            print_and_log(f"     → REGLA: No se encontró movimiento (descuadre físico)")
        else:
            print_and_log(f"     → REGLA: No identificada claramente")
    
    print_and_log("\n" + "=" * 100)
    print_and_log("FIN DEL SEGUIMIENTO")
    print_and_log("=" * 100)

if __name__ == "__main__":
    seguimiento_cajero_6447()

