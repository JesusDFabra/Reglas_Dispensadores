"""
Script de prueba para ejecutar el proceso completo de arqueo
con todas las reglas de negocio implementadas.
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
import pandas as pd

# Configurar logging
logger = configurar_logger(nivel=logging.INFO)


def main():
    """Función principal de prueba completa."""
    try:
        logger.info("=" * 100)
        logger.info("PRUEBA COMPLETA: PROCESO DE ARQUEO CON REGLAS DE NEGOCIO")
        logger.info("=" * 100)
        
        # Cargar configuración
        logger.info("\n1. Cargando configuración...")
        config = CargadorConfig(usar_fecha_actual=True)
        config_data = config.cargar()
        
        fecha_proceso = config_data['proceso']['fecha_proceso']
        fecha_arqueo = config_data['proceso']['fecha_arqueo']
        
        logger.info(f"   Fecha de proceso: {fecha_proceso}")
        logger.info(f"   Fecha de arqueo: {fecha_arqueo}")
        
        # Inicializar procesador
        logger.info("\n2. Inicializando procesador de arqueos...")
        procesador = ProcesadorArqueos(config, consultar_movimientos=True)
        
        nombre_insumo = "gestion_ksgarro"
        buscar_mas_reciente = config_data['proceso'].get('buscar_archivo_mas_reciente', True)
        
        logger.info(f"   Insumo: {nombre_insumo}")
        logger.info(f"   Consultar movimientos: Habilitado")
        logger.info(f"   Buscar archivo más reciente: {buscar_mas_reciente}")
        
        # Procesar el insumo completo
        logger.info("\n3. Procesando insumo completo...")
        logger.info("-" * 100)
        
        df_procesado = procesador.procesar_insumo(nombre_insumo, buscar_mas_reciente)
        
        logger.info("-" * 100)
        logger.info(f"\n   Total de registros ARQUEO procesados: {len(df_procesado)}")
        
        if len(df_procesado) == 0:
            logger.warning("No se encontraron registros ARQUEO para procesar")
            return
        
        # Análisis de resultados
        logger.info("\n4. Análisis de resultados...")
        logger.info("-" * 100)
        
        # Estadísticas generales
        total_registros = len(df_procesado)
        
        # Registros con descuadre
        registros_con_descuadre = df_procesado[
            ((df_procesado['faltantes'].notna()) & (df_procesado['faltantes'] != 0)) |
            ((df_procesado['sobrantes'].notna()) & (df_procesado['sobrantes'] != 0))
        ]
        
        total_con_descuadre = len(registros_con_descuadre)
        
        logger.info(f"   Total de registros ARQUEO: {total_registros}")
        logger.info(f"   Registros con descuadre (sobrante/faltante): {total_con_descuadre}")
        logger.info(f"   Registros sin descuadre: {total_registros - total_con_descuadre}")
        
        # Análisis de consultas de movimientos
        if 'movimiento_encontrado' in df_procesado.columns:
            movimientos_encontrados = df_procesado['movimiento_encontrado'].sum()
            movimientos_no_encontrados = total_con_descuadre - movimientos_encontrados
            
            logger.info(f"\n   Consultas de movimientos:")
            logger.info(f"   - Movimientos encontrados: {movimientos_encontrados}")
            logger.info(f"   - Movimientos NO encontrados: {movimientos_no_encontrados}")
            
            # Distribución por fuente
            if 'movimiento_fuente' in df_procesado.columns:
                fuentes = df_procesado[df_procesado['movimiento_encontrado'] == True]['movimiento_fuente'].value_counts()
                if len(fuentes) > 0:
                    logger.info(f"\n   Distribución por fuente:")
                    for fuente, cantidad in fuentes.items():
                        logger.info(f"   - {fuente}: {cantidad}")
        
        # Análisis de sobrantes y faltantes
        logger.info(f"\n   Análisis de descuadres:")
        
        registros_faltantes = df_procesado[
            (df_procesado['faltantes'].notna()) & (df_procesado['faltantes'] != 0)
        ]
        registros_sobrantes = df_procesado[
            (df_procesado['sobrantes'].notna()) & (df_procesado['sobrantes'] != 0)
        ]
        
        logger.info(f"   - Registros con faltante: {len(registros_faltantes)}")
        if len(registros_faltantes) > 0:
            logger.info(f"     Total faltantes: {registros_faltantes['faltantes'].sum():,.0f}")
            logger.info(f"     Promedio faltantes: {registros_faltantes['faltantes'].mean():,.0f}")
        
        logger.info(f"   - Registros con sobrante: {len(registros_sobrantes)}")
        if len(registros_sobrantes) > 0:
            logger.info(f"     Total sobrantes: {registros_sobrantes['sobrantes'].sum():,.0f}")
            logger.info(f"     Promedio sobrantes: {registros_sobrantes['sobrantes'].mean():,.0f}")
        
        # Registros actualizados en archivo original
        if 'movimiento_encontrado' in df_procesado.columns:
            registros_actualizados = df_procesado[
                (df_procesado['movimiento_encontrado'] == False) &
                (
                    ((df_procesado['faltantes'].notna()) & (df_procesado['faltantes'] != 0)) |
                    ((df_procesado['sobrantes'].notna()) & (df_procesado['sobrantes'] != 0))
                )
            ]
            
            # Separar por tipo de descuadre
            registros_sobrantes_actualizados = registros_actualizados[
                (registros_actualizados['sobrantes'].notna()) & 
                (registros_actualizados['sobrantes'] != 0)
            ]
            registros_faltantes_actualizados = registros_actualizados[
                (registros_actualizados['faltantes'].notna()) & 
                (registros_actualizados['faltantes'] != 0)
            ]
            
            logger.info(f"\n   Registros actualizados en archivo original:")
            logger.info(f"   - Total actualizados: {len(registros_actualizados)}")
            logger.info(f"   - SOBRANTES ({len(registros_sobrantes_actualizados)}):")
            logger.info(f"     * justificacion = 'SOBRANTE CONTABLE'")
            logger.info(f"     * nuevo_estado = 'SOBRANTE CONTABLE'")
            logger.info(f"   - FALTANTES ({len(registros_faltantes_actualizados)}):")
            logger.info(f"     * justificacion = 'Fisico'")
            logger.info(f"     * nuevo_estado = 'FALTANTE EN ARQUEO'")
        
        # Mostrar muestra de registros
        logger.info("\n5. Muestra de registros procesados...")
        logger.info("-" * 100)
        
        columnas_a_mostrar = ['codigo_cajero', 'codigo_suc', 'sobrantes', 'faltantes']
        if 'movimiento_encontrado' in df_procesado.columns:
            columnas_a_mostrar.append('movimiento_encontrado')
        if 'movimiento_fuente' in df_procesado.columns:
            columnas_a_mostrar.append('movimiento_fuente')
        if 'justificacion' in df_procesado.columns:
            columnas_a_mostrar.append('justificacion')
        if 'nuevo_estado' in df_procesado.columns:
            columnas_a_mostrar.append('nuevo_estado')
        
        # Mostrar primeros 15 registros
        df_muestra = df_procesado.head(15)[columnas_a_mostrar]
        print("\n" + "=" * 120)
        print("MUESTRA DE REGISTROS PROCESADOS (Primeros 15)")
        print("=" * 120)
        print(df_muestra.to_string(index=False))
        print("=" * 120)
        
        # Mostrar registros actualizados
        if 'movimiento_encontrado' in df_procesado.columns and len(registros_actualizados) > 0:
            logger.info("\n6. Registros actualizados en archivo original...")
            logger.info("-" * 100)
            
            columnas_actualizados = ['codigo_cajero', 'codigo_suc', 'faltantes', 'sobrantes']
            if 'justificacion' in registros_actualizados.columns:
                columnas_actualizados.append('justificacion')
            if 'nuevo_estado' in registros_actualizados.columns:
                columnas_actualizados.append('nuevo_estado')
            
            df_actualizados_muestra = registros_actualizados.head(10)[columnas_actualizados]
            print("\n" + "=" * 120)
            print("REGISTROS ACTUALIZADOS EN ARCHIVO ORIGINAL (Primeros 10)")
            print("=" * 120)
            print(df_actualizados_muestra.to_string(index=False))
            print("=" * 120)
        
        # Guardar resultados detallados
        logger.info("\n7. Guardando resultados detallados...")
        nombre_salida = f"resultado_proceso_completo_{nombre_insumo}_{fecha_proceso.replace('-', '_')}"
        ruta_salida = procesador.guardar_resultados(df_procesado, nombre_salida)
        logger.info(f"   Resultados guardados en: {ruta_salida}")
        
        # Resumen final
        logger.info("\n" + "=" * 100)
        logger.info("RESUMEN FINAL DEL PROCESO")
        logger.info("=" * 100)
        logger.info(f"[OK] Total de registros ARQUEO procesados: {total_registros}")
        logger.info(f"[OK] Registros con descuadre: {total_con_descuadre}")
        
        if 'movimiento_encontrado' in df_procesado.columns:
            logger.info(f"[OK] Movimientos encontrados: {movimientos_encontrados}")
            logger.info(f"[OK] Movimientos NO encontrados (actualizados): {movimientos_no_encontrados}")
        
        logger.info(f"[OK] Archivo original actualizado automaticamente")
        logger.info(f"[OK] Resultados guardados en: {ruta_salida}")
        logger.info("=" * 100)
        logger.info("PRUEBA COMPLETA FINALIZADA EXITOSAMENTE")
        logger.info("=" * 100)
        
    except Exception as e:
        logger.error(f"Error en la prueba completa: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

