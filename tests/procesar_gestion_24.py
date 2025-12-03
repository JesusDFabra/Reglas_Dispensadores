"""
Script para procesar específicamente el archivo de gestión del 24 de noviembre de 2025.
"""

import sys
from pathlib import Path
from datetime import datetime

# Agregar el directorio raíz del proyecto al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

from src.config.cargador_config import CargadorConfig
from src.procesamiento.procesador_arqueos import ProcesadorArqueos
from src.utils.logger_config import configurar_logger
import logging

# Configurar logging
logger = configurar_logger(nivel=logging.INFO)


def main():
    """Función principal para procesar gestión del 24."""
    try:
        logger.info("=" * 100)
        logger.info("PROCESANDO GESTIÓN DEL 24 DE NOVIEMBRE DE 2025")
        logger.info("=" * 100)
        
        # Cargar configuración
        logger.info("\n1. Cargando configuración...")
        config = CargadorConfig(usar_fecha_actual=False)  # No usar fecha actual, procesar específicamente el 24
        
        # Forzar fecha de proceso al 25 de noviembre (día siguiente al arqueo del 24)
        config_data = config.cargar()
        config_data['proceso']['fecha_proceso'] = '2025-11-25'
        config_data['proceso']['fecha_arqueo'] = '2025-11-24'
        config_data['proceso']['aplicar_filtro_dias_habiles'] = False  # Deshabilitar filtro para procesar todos
        
        logger.info(f"   Fecha de proceso: {config_data['proceso']['fecha_proceso']}")
        logger.info(f"   Fecha de arqueo: {config_data['proceso']['fecha_arqueo']}")
        
        # Buscar archivo específico del 24
        logger.info("\n2. Buscando archivo del 24...")
        ruta_archivo = Path('insumos_excel/gestion_24_11_2025_ksgarro.xlsx')
        
        if not ruta_archivo.exists():
            logger.error(f"Archivo no encontrado: {ruta_archivo}")
            return
        
        logger.info(f"   Archivo encontrado: {ruta_archivo}")
        
        # Procesar archivo
        logger.info("\n3. Procesando archivo...")
        procesador = ProcesadorArqueos(config, consultar_movimientos=True)
        
        # Cargar archivo directamente
        df_original = procesador.cargar_archivo_excel(ruta_archivo)
        logger.info(f"   Total de registros en archivo: {len(df_original)}")
        
        # Guardar referencia para actualización
        procesador._ruta_archivo_original = ruta_archivo
        procesador._df_archivo_original = df_original
        
        # Filtrar por ARQUEO
        df_arqueos = procesador.filtrar_por_tipo_registro(df_original, 'ARQUEO')
        logger.info(f"   Registros ARQUEO: {len(df_arqueos)}")
        
        # Verificar registros con descuadre
        registros_con_descuadre = df_arqueos[
            ((df_arqueos['sobrantes'].notna()) & (df_arqueos['sobrantes'] != 0)) |
            ((df_arqueos['faltantes'].notna()) & (df_arqueos['faltantes'] != 0))
        ]
        logger.info(f"   Registros con descuadre: {len(registros_con_descuadre)}")
        
        # Procesar consulta de movimientos
        if len(registros_con_descuadre) > 0:
            logger.info("\n4. Consultando movimientos...")
            df_procesado = procesador._consultar_movimientos(registros_con_descuadre)
            
            # Mostrar resumen de consultas
            movimientos_encontrados = df_procesado[df_procesado['movimiento_encontrado'] == True]
            logger.info(f"   Movimientos encontrados: {len(movimientos_encontrados)}")
            logger.info(f"   Movimientos NO encontrados: {len(df_procesado) - len(movimientos_encontrados)}")
            
            # Mostrar fuentes de movimientos
            if len(movimientos_encontrados) > 0:
                fuentes = movimientos_encontrados['movimiento_fuente'].value_counts()
                logger.info(f"   Fuentes de movimientos:")
                for fuente, cantidad in fuentes.items():
                    logger.info(f"     - {fuente}: {cantidad}")
            
            # Actualizar archivo original
            logger.info("\n5. Actualizando archivo original...")
            procesador._actualizar_archivo_original(df_procesado)
            
            # Verificar archivo procesado
            ruta_procesado = procesador.obtener_ruta_archivo_procesado()
            if ruta_procesado:
                logger.info(f"   Archivo procesado guardado en: {ruta_procesado}")
            
            # Mostrar resumen de clasificaciones
            logger.info("\n6. Resumen de clasificaciones:")
            if 'justificacion' in df_procesado.columns:
                clasificaciones = df_procesado['justificacion'].value_counts()
                logger.info("   Justificaciones:")
                for justificacion, cantidad in clasificaciones.items():
                    logger.info(f"     - {justificacion}: {cantidad}")
            
            if 'nuevo_estado' in df_procesado.columns:
                estados = df_procesado['nuevo_estado'].value_counts()
                logger.info("   Estados:")
                for estado, cantidad in estados.items():
                    logger.info(f"     - {estado}: {cantidad}")
        else:
            logger.info("   No hay registros con descuadre para procesar")
        
        logger.info("\n" + "=" * 100)
        logger.info("PROCESO COMPLETADO")
        logger.info("=" * 100)
        
    except Exception as e:
        logger.error(f"Error en el proceso: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

