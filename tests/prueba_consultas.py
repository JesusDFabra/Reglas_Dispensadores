"""
Script de prueba para verificar las consultas de movimientos.
"""

import sys
from pathlib import Path

# Agregar el directorio raíz del proyecto al path
proyecto_root = Path(__file__).parent.parent
sys.path.insert(0, str(proyecto_root))

from src.config.cargador_config import CargadorConfig
from src.procesamiento.procesador_arqueos import ProcesadorArqueos
from src.consultas.consultor_movimientos import ConsultorMovimientos
from src.utils.logger_config import configurar_logger
import logging

# Configurar logging
logger = configurar_logger(nivel=logging.INFO)


def main():
    """Función principal de prueba."""
    try:
        logger.info("=" * 80)
        logger.info("PRUEBA: Consulta de movimientos")
        logger.info("=" * 80)
        
        # Cargar configuración
        logger.info("Cargando configuración...")
        config = CargadorConfig(usar_fecha_actual=True)
        config_data = config.cargar()
        
        fecha_arqueo = config_data['proceso']['fecha_arqueo']
        logger.info(f"Fecha de arqueo: {fecha_arqueo}")
        
        # Procesar el insumo con consultas de movimientos
        procesador = ProcesadorArqueos(config, consultar_movimientos=True)
        nombre_insumo = "gestion_ksgarro"
        
        logger.info(f"\nProcesando insumo: {nombre_insumo}")
        logger.info("-" * 80)
        
        buscar_mas_reciente = config_data['proceso'].get('buscar_archivo_mas_reciente', True)
        df_procesado = procesador.procesar_insumo(nombre_insumo, buscar_mas_reciente)
        
        logger.info(f"\nTotal de registros ARQUEO procesados: {len(df_procesado)}")
        
        # Verificar si se agregaron columnas de consulta
        columnas_consulta = [
            'movimiento_encontrado', 'movimiento_fuente', 
            'movimiento_valor', 'movimiento_detalle'
        ]
        
        columnas_agregadas = [col for col in columnas_consulta if col in df_procesado.columns]
        
        if columnas_agregadas:
            logger.info(f"Columnas de consulta agregadas: {columnas_agregadas}")
            
            # Mostrar resumen de consultas
            total_consultas = len(df_procesado)
            movimientos_encontrados = df_procesado['movimiento_encontrado'].sum() if 'movimiento_encontrado' in df_procesado.columns else 0
            
            logger.info(f"\nResumen de consultas:")
            logger.info(f"  Total de registros: {total_consultas}")
            logger.info(f"  Movimientos encontrados: {movimientos_encontrados}")
            logger.info(f"  Movimientos no encontrados: {total_consultas - movimientos_encontrados}")
            
            # Mostrar distribución por fuente
            if 'movimiento_fuente' in df_procesado.columns:
                fuentes = df_procesado['movimiento_fuente'].value_counts()
                logger.info(f"\nDistribución por fuente:")
                for fuente, cantidad in fuentes.items():
                    logger.info(f"  {fuente}: {cantidad}")
            
            # Mostrar primeros 10 registros con consultas
            logger.info("\n" + "=" * 100)
            logger.info("PRIMEROS 10 REGISTROS CON CONSULTAS")
            logger.info("=" * 100)
            
            columnas_a_mostrar = ['codigo_cajero', 'sobrantes', 'faltantes']
            if 'movimiento_encontrado' in df_procesado.columns:
                columnas_a_mostrar.append('movimiento_encontrado')
            if 'movimiento_fuente' in df_procesado.columns:
                columnas_a_mostrar.append('movimiento_fuente')
            if 'movimiento_valor' in df_procesado.columns:
                columnas_a_mostrar.append('movimiento_valor')
            
            df_muestra = df_procesado.head(10)[columnas_a_mostrar]
            print("\n" + df_muestra.to_string(index=False))
            
        else:
            logger.warning("No se agregaron columnas de consulta. Verificar configuración.")
        
        logger.info("\n" + "=" * 100)
        logger.info("PRUEBA COMPLETADA")
        logger.info("=" * 100)
        
    except Exception as e:
        logger.error(f"Error en la prueba: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

