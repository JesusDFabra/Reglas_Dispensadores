"""
Script de ejemplo para demostrar el uso del procesador de arqueos.
Este script puede ser usado para probar la configuración y el procesamiento.
"""

import sys
from pathlib import Path

# Agregar el directorio raíz del proyecto al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

from src.config.cargador_config import CargadorConfig
from src.procesamiento.procesador_arqueos import ProcesadorArqueos
from src.utils.logger_config import configurar_logger
import logging

# Configurar logging
logger = configurar_logger(nivel=logging.INFO)


def ejemplo_cargar_configuracion():
    """Ejemplo de cómo cargar la configuración."""
    logger.info("Ejemplo: Cargar configuración")
    logger.info("-" * 60)
    
    try:
        config = CargadorConfig()
        config_data = config.cargar()
        
        logger.info("Configuración cargada exitosamente")
        logger.info(f"Fecha de proceso: {config_data['proceso']['fecha_proceso']}")
        logger.info(f"Fecha de arqueo: {config_data['proceso']['fecha_arqueo']}")
        
        insumos_activos = config.obtener_insumos_activos()
        logger.info(f"Insumos activos: {list(insumos_activos.keys())}")
        
        return config
    
    except Exception as e:
        logger.error(f"Error al cargar configuración: {e}")
        raise


def ejemplo_procesar_insumo(config: CargadorConfig):
    """Ejemplo de cómo procesar un insumo específico."""
    logger.info("\nEjemplo: Procesar insumo")
    logger.info("-" * 60)
    
    try:
        procesador = ProcesadorArqueos(config)
        
        # Procesar el insumo activo (gestion_ksgarro)
        nombre_insumo = "gestion_ksgarro"
        logger.info(f"Procesando insumo: {nombre_insumo}")
        
        df_procesado = procesador.procesar_insumo(nombre_insumo)
        
        logger.info(f"Registros procesados: {len(df_procesado)}")
        logger.info(f"Columnas: {list(df_procesado.columns)}")
        
        if len(df_procesado) > 0:
            logger.info("\nPrimeras 3 filas del resultado:")
            logger.info("\n" + str(df_procesado.head(3).to_string()))
        
        return df_procesado
    
    except Exception as e:
        logger.error(f"Error al procesar insumo: {e}")
        raise


def main():
    """Función principal del ejemplo."""
    logger.info("=" * 60)
    logger.info("EJEMPLO DE USO - PROCESADOR DE ARQUEOS")
    logger.info("=" * 60)
    
    try:
        # Ejemplo 1: Cargar configuración
        config = ejemplo_cargar_configuracion()
        
        # Ejemplo 2: Procesar insumo
        df = ejemplo_procesar_insumo(config)
        
        logger.info("\n" + "=" * 60)
        logger.info("EJEMPLO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error en el ejemplo: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

